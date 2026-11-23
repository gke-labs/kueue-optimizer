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

"""`Deployment` scraping and replay self-test scenarios (`st81`–`st90`).

Covers Categories 17 & 18 (`st81`–`st90`):
- `st81` vs. `st82`: `Deployment` `ScaleUp` (`2 -> 6` replicas at `t=+10s`) with full `ClusterQueue`
  headroom (`st81`: `6/6` running, `0` pending) vs. `8`-TPU `ClusterQueue` cap (`st82`: `4/6` running,
  `2` pending Kueue-gated replica `Workload`s).
- `st83` vs. `st84`: `Deployment` `ScaleDown` (`8 -> 4` at `t=+10s`, unblocking `2` queued `4`-TPU
  batch `Job`s, then `4 -> 0` scale-to-zero at `t=+26s`, `st83`) vs. unscaled `8`-replica control
  permanently starving both batch `Job`s (`st84`).
- `st85` vs. `st86`: `Deployment` rolling `Upgrade` (`templateHash` `th-0000000000000001 -> th-0000000000000002`
  surviving container sanitization at `t=+12s`) in a tight `8`-TPU `ClusterQueue` using
  `rollingUpdate: {maxUnavailable: 1, maxSurge: 0}` (`st85`: rolls all `4` `v2` pods cleanly) vs.
  `{maxUnavailable: 0, maxSurge: 2}` (`st86`: `2` surge `v2` pods deadlock on Kueue admission gate).
- `st87` vs. `st88`: `Deployment` `Paused` (`t=+8s`) deferring a `1 -> 4` TPU/pod `Upgrade` (`t=+11s`)
  until `Resumed` (`t=+24s`, `st87`) vs. unpaused immediate rollout at `t=+11s` (`st88`).
- `st89` vs. `st90`: `t0` `Deployment` full (`2/2`) + partial (`4/6`) readiness status & `LowerPriority`
  preemption + `ReplicaSet` self-healing (`st89`) vs. `t0` `Deployment` deletion (`t=+8s`), same-name
  incarnation-`1` re-creation (`replicas: 6` at `t=+12s`), and `canary` creation/deletion (`st90`).
"""

from __future__ import annotations

from typing import Any, Callable


DEFAULT_NS = "st-workloads"
DEFAULT_FLAVOR = "st-tpu-flavor"
DEFAULT_ACCEL_LABEL = "tpu-v7x-st"
PAUSE_IMAGE = "registry.k8s.io/pause:3.10"


def _make_node(
    idx: int,
    det_uid_fn: Callable[[str], str],
    *,
    tpu_per_node: int = 4,
    cpu_per_node: int = 16,
    mem_gi_per_node: int = 64,
) -> dict[str, Any]:
    name = f"st-node-{idx:02d}"
    return {
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {
            "name": name,
            "uid": det_uid_fn(f"node:{name}"),
            "labels": {
                "beta.kubernetes.io/arch": "amd64",
                "beta.kubernetes.io/os": "linux",
                "kubernetes.io/arch": "amd64",
                "kubernetes.io/hostname": name,
                "kubernetes.io/os": "linux",
                "kubernetes.io/role": "agent",
                "node-role.kubernetes.io/agent": "",
                "type": "kwok",
                "kueue-optimizer.gke.io/node-role": "agent",
                "cloud.google.com/gke-tpu-accelerator": DEFAULT_ACCEL_LABEL,
            },
            "annotations": {
                "kwok.x-k8s.io/node": "fake",
                "node.alpha.kubernetes.io/ttl": "0",
            },
        },
        "spec": {},
        "status": {
            "phase": "Running",
            "capacity": {
                "cpu": str(cpu_per_node),
                "memory": f"{mem_gi_per_node}Gi",
                "pods": "110",
                "google.com/tpu": str(tpu_per_node),
            },
            "allocatable": {
                "cpu": str(cpu_per_node),
                "memory": f"{mem_gi_per_node}Gi",
                "pods": "110",
                "google.com/tpu": str(tpu_per_node),
            },
            "conditions": [
                {
                    "type": "Ready",
                    "status": "True",
                    "reason": "KubeletReady",
                    "message": "kubelet is posting ready status",
                }
            ],
        },
    }


def _make_deployment_obj(
    name: str,
    det_uid_fn: Callable[[str], str],
    fmt_ts_fn: Callable[[float], str],
    *,
    local_queue: str,
    replicas: int,
    tpu_per_pod: int = 2,
    cpu_per_pod: int = 2,
    mem_gi_per_pod: int = 8,
    priority_class: str = "p-std",
    paused: bool = False,
    template_hash: str = "th-0000000000000001",
    max_unavailable: int | str = 1,
    max_surge: int | str = 1,
    creation_offset_sec: float = 0.0,
    incarnation: int = 0,
) -> dict[str, Any]:
    labels: dict[str, str] = {
        "app": name,
        "kueue.x-k8s.io/queue-name": local_queue,
    }
    tpl_labels: dict[str, str] = {
        "app": name,
        "kueue.x-k8s.io/queue-name": local_queue,
        "kueue.x-k8s.io/managed": "true",
    }
    if priority_class:
        labels["kueue.x-k8s.io/priority-class"] = priority_class
        tpl_labels["kueue.x-k8s.io/priority-class"] = priority_class

    reqs = {
        "cpu": str(cpu_per_pod),
        "memory": f"{mem_gi_per_pod}Gi",
        "google.com/tpu": str(tpu_per_pod),
    }
    spec: dict[str, Any] = {
        "replicas": int(replicas),
        "paused": bool(paused),
        "selector": {"matchLabels": {"app": name}},
        "strategy": {
            "type": "RollingUpdate",
            "rollingUpdate": {
                "maxUnavailable": max_unavailable,
                "maxSurge": max_surge,
            },
        },
        "template": {
            "metadata": {
                "labels": tpl_labels,
                "annotations": {
                    "kueue.x-k8s.io/pod-suspending-parent": "deployment",
                    "kueue-optimizer.gke.io/template-hash": template_hash,
                },
            },
            "spec": {
                "restartPolicy": "Always",
                "tolerations": [
                    {
                        "key": "kwok.x-k8s.io/node",
                        "operator": "Equal",
                        "value": "fake",
                        "effect": "NoSchedule",
                    }
                ],
                "containers": [
                    {
                        "name": "serving",
                        "image": PAUSE_IMAGE,
                        "resources": {
                            "requests": dict(reqs),
                            "limits": dict(reqs),
                        },
                    }
                ],
            },
        },
    }
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": name,
            "namespace": DEFAULT_NS,
            "uid": det_uid_fn(f"deploy:{DEFAULT_NS}/{name}:inc{incarnation}"),
            "creationTimestamp": fmt_ts_fn(creation_offset_sec),
            "labels": labels,
        },
        "spec": spec,
    }


def _make_deployment_wf_event(
    *,
    fmt_ts_fn: Callable[[float], str],
    event_offset_sec: float,
    creation_offset_sec: float,
    event_type: str,
    workload_state: str,
    name: str,
    local_queue: str,
    replicas: int,
    ready_replicas: int,
    paused: bool,
    template_hash: str,
    deploy_obj: dict[str, Any],
    update_types: list[str] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "objectKind": "Deployment",
        "name": name,
        "namespace": DEFAULT_NS,
        "creationTime": fmt_ts_fn(creation_offset_sec),
        "localQueueName": local_queue,
        "replicas": int(replicas),
        "readyReplicas": int(ready_replicas),
        "paused": bool(paused),
        "templateHash": template_hash,
        "object": deploy_obj,
    }
    if update_types:
        payload["updateTypes"] = list(update_types)
    return {
        "timestamp": fmt_ts_fn(event_offset_sec),
        "eventType": event_type,
        "workloadState": workload_state,
        "payload": payload,
    }


def _make_batch_job_and_exec_events(
    *,
    name: str,
    det_uid_fn: Callable[[str], str],
    fmt_ts_fn: Callable[[float], str],
    local_queue: str,
    cluster_queue: str,
    priority_class: str,
    priority_value: int,
    arrival_offset_sec: float,
    admitted_offset_sec: float | None,
    duration_sec: float | None,
    parallelism: int = 1,
    tpu_per_pod: int = 4,
    cpu_per_pod: int = 4,
    mem_gi_per_pod: int = 16,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    reqs = {
        "cpu": str(cpu_per_pod),
        "memory": f"{mem_gi_per_pod}Gi",
        "google.com/tpu": str(tpu_per_pod),
    }
    job_obj: dict[str, Any] = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": name,
            "namespace": DEFAULT_NS,
            "uid": det_uid_fn(f"job:{DEFAULT_NS}/{name}"),
            "creationTimestamp": fmt_ts_fn(arrival_offset_sec),
            "labels": {
                "kueue.x-k8s.io/queue-name": local_queue,
                "kueue.x-k8s.io/priority-class": priority_class,
            },
            "annotations": {
                "kueue-optimizer.gke.io/pod-execution-time-seconds": str(int(duration_sec or 8)),
            },
        },
        "spec": {
            "suspend": True,
            "parallelism": parallelism,
            "completions": parallelism,
            "template": {
                "metadata": {
                    "annotations": {
                        "kueue-optimizer.gke.io/pod-execution-time-seconds": str(int(duration_sec or 8)),
                    }
                },
                "spec": {
                    "restartPolicy": "Never",
                    "tolerations": [
                        {
                            "key": "kwok.x-k8s.io/node",
                            "operator": "Equal",
                            "value": "fake",
                            "effect": "NoSchedule",
                        }
                    ],
                    "containers": [
                        {
                            "name": "worker",
                            "image": PAUSE_IMAGE,
                            "resources": {
                                "requests": dict(reqs),
                                "limits": dict(reqs),
                            },
                        }
                    ],
                },
            },
        },
    }
    wf_ev = {
        "timestamp": fmt_ts_fn(arrival_offset_sec),
        "eventType": "New",
        "workloadState": "NotAdmitted",
        "payload": {
            "objectKind": "Job",
            "name": name,
            "namespace": DEFAULT_NS,
            "creationTime": fmt_ts_fn(arrival_offset_sec),
            "localQueueName": local_queue,
            "object": job_obj,
        },
    }
    exec_evs: list[dict[str, Any]] = []
    if admitted_offset_sec is not None and duration_sec is not None:
        wl_name = f"job-{name}-a1b2c"
        end_off = admitted_offset_sec + duration_sec
        psa = [
            {
                "name": "main",
                "count": parallelism,
                "flavors": {
                    "cpu": DEFAULT_FLAVOR,
                    "memory": DEFAULT_FLAVOR,
                    "google.com/tpu": DEFAULT_FLAVOR,
                },
                "resourceUsage": {
                    "cpu": str(cpu_per_pod * parallelism),
                    "memory": f"{mem_gi_per_pod * parallelism}Gi",
                    "google.com/tpu": str(tpu_per_pod * parallelism),
                },
            }
        ]
        exec_evs.append(
            {
                "timestamp": fmt_ts_fn(admitted_offset_sec),
                "eventType": "Admitted",
                "workloadState": "Admitted",
                "payload": {
                    "workloadName": wl_name,
                    "namespace": DEFAULT_NS,
                    "ownerKind": "Job",
                    "ownerName": name,
                    "ownerAPIVersion": "batch/v1",
                    "localQueueName": local_queue,
                    "clusterQueueName": cluster_queue,
                    "priorityClassName": priority_class,
                    "priority": priority_value,
                    "podSetAssignments": psa,
                    "creationTime": fmt_ts_fn(arrival_offset_sec),
                    "admittedTime": fmt_ts_fn(admitted_offset_sec),
                    "preempted": False,
                    "preemptionCount": 0,
                    "interruptionCount": 0,
                    "reason": "Admitted",
                },
            }
        )
        exec_evs.append(
            {
                "timestamp": fmt_ts_fn(end_off),
                "eventType": "FullExecution",
                "workloadState": "Admitted",
                "payload": {
                    "executionType": "Full",
                    "workloadName": wl_name,
                    "namespace": DEFAULT_NS,
                    "ownerKind": "Job",
                    "ownerName": name,
                    "ownerAPIVersion": "batch/v1",
                    "localQueueName": local_queue,
                    "clusterQueueName": cluster_queue,
                    "priorityClassName": priority_class,
                    "priority": priority_value,
                    "podSetAssignments": psa,
                    "creationTime": fmt_ts_fn(arrival_offset_sec),
                    "admittedTime": fmt_ts_fn(admitted_offset_sec),
                    "endTime": fmt_ts_fn(end_off),
                    "executionDurationSeconds": float(duration_sec),
                    "executionDuration": f"{int(duration_sec)}s",
                    "preempted": False,
                    "preemptionCount": 0,
                    "interruptionCount": 0,
                    "reason": "Succeeded",
                },
            }
        )
    return wf_ev, exec_evs


def _make_t0_deployment_pod_and_exec(
    *,
    deploy_name: str,
    replica_idx: int,
    placement_node: str,
    fmt_ts_fn: Callable[[float], str],
    local_queue: str,
    cluster_queue: str,
    priority_class: str,
    priority_value: int,
    tpu_per_pod: int = 2,
    cpu_per_pod: int = 2,
    mem_gi_per_pod: int = 8,
    scrape_end_sec: float = 36.0,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    pod_name = f"{deploy_name}-rs0-{replica_idx:02d}"
    wl_name = f"pod-{pod_name}-000{replica_idx:02d}"
    pod_ev = {
        "timestamp": fmt_ts_fn(0.0),
        "eventType": "New",
        "payload": {
            "apiVersion": "v1",
            "kind": "Pod",
            "name": pod_name,
            "namespace": DEFAULT_NS,
            "placement": placement_node,
            "resourceRequest": {
                "cpu": str(cpu_per_pod),
                "memory": f"{mem_gi_per_pod}Gi",
                "google.com/tpu": str(tpu_per_pod),
            },
            "schedulingTime": fmt_ts_fn(0.0),
            "firstNoticeTime": fmt_ts_fn(0.0),
            "workloadName": wl_name,
            "ownerKind": "Deployment",
            "ownerName": deploy_name,
            "podSetName": "main",
        },
    }
    psa = [
        {
            "name": "main",
            "count": 1,
            "flavors": {
                "cpu": DEFAULT_FLAVOR,
                "memory": DEFAULT_FLAVOR,
                "google.com/tpu": DEFAULT_FLAVOR,
            },
            "resourceUsage": {
                "cpu": str(cpu_per_pod),
                "memory": f"{mem_gi_per_pod}Gi",
                "google.com/tpu": str(tpu_per_pod),
            },
        }
    ]
    exec_evs = [
        {
            "timestamp": fmt_ts_fn(0.0),
            "eventType": "Admitted",
            "workloadState": "Admitted",
            "payload": {
                "workloadName": wl_name,
                "namespace": DEFAULT_NS,
                "ownerKind": "Deployment",
                "ownerName": deploy_name,
                "ownerAPIVersion": "apps/v1",
                "localQueueName": local_queue,
                "clusterQueueName": cluster_queue,
                "priorityClassName": priority_class,
                "priority": priority_value,
                "podSetAssignments": psa,
                "creationTime": fmt_ts_fn(0.0),
                "admittedTime": fmt_ts_fn(0.0),
                "preempted": False,
                "preemptionCount": 0,
                "interruptionCount": 0,
                "reason": "Admitted",
            },
        },
        {
            "timestamp": fmt_ts_fn(scrape_end_sec),
            "eventType": "PartialExecution",
            "workloadState": "Admitted",
            "payload": {
                "executionType": "Partial",
                "workloadName": wl_name,
                "namespace": DEFAULT_NS,
                "ownerKind": "Deployment",
                "ownerName": deploy_name,
                "ownerAPIVersion": "apps/v1",
                "localQueueName": local_queue,
                "clusterQueueName": cluster_queue,
                "priorityClassName": priority_class,
                "priority": priority_value,
                "podSetAssignments": psa,
                "creationTime": fmt_ts_fn(0.0),
                "admittedTime": fmt_ts_fn(0.0),
                "endTime": fmt_ts_fn(scrape_end_sec),
                "executionDurationSeconds": float(scrape_end_sec),
                "executionDuration": f"{int(scrape_end_sec)}s",
                "preempted": False,
                "preemptionCount": 0,
                "interruptionReason": "ScrapeEnded",
                "interruptionCount": 1,
                "reason": "ScrapeEnded",
            },
        },
    ]
    return pod_ev, exec_evs


def build_deployment_10_scenarios(
    *,
    scenario_spec_cls: Any,
    det_uid_fn: Callable[[str], str],
    fmt_ts_fn: Callable[[float], str],
    make_infra_event_fn: Callable[..., dict[str, Any]],
    make_namespace_fn: Callable[..., dict[str, Any]],
    make_flavor_fn: Callable[..., dict[str, Any]],
    make_priority_class_fn: Callable[..., dict[str, Any]],
    make_cluster_queue_fn: Callable[..., dict[str, Any]],
    make_local_queue_fn: Callable[..., dict[str, Any]],
) -> list[Any]:
    """Generate `st81` through `st90` focused on `Deployment` scraping and replaying."""
    scenarios: list[Any] = []

    def _base_4node_infra(
        cq_name: str,
        lq_name: str,
        *,
        tpu_quota: int = 16,
        within_cq_preemption: str = "Never",
        extra_pcs: list[tuple[str, int]] | None = None,
    ) -> list[dict[str, Any]]:
        inf: list[dict[str, Any]] = []
        for i in range(4):
            inf.append(make_infra_event_fn(_make_node(i, det_uid_fn)))
        inf.append(make_infra_event_fn(make_namespace_fn(DEFAULT_NS)))
        inf.append(make_infra_event_fn(make_flavor_fn(DEFAULT_FLAVOR)))
        inf.append(make_infra_event_fn(make_priority_class_fn("p-std", 100)))
        for pc_n, pc_v in extra_pcs or []:
            inf.append(make_infra_event_fn(make_priority_class_fn(pc_n, pc_v)))
        inf.append(
            make_infra_event_fn(
                make_cluster_queue_fn(
                    cq_name,
                    tpu_quota=tpu_quota,
                    cpu_quota=64,
                    mem_gi_quota=256,
                    within_cq=within_cq_preemption,
                )
            )
        )
        inf.append(make_infra_event_fn(make_local_queue_fn(lq_name, cq_name)))
        return inf

    # =========================================================================
    # st81 & st82: Deployment Scale-Up (Headroom vs. Quota Cap)
    # =========================================================================
    for is_capped in (False, True):
        sc_id = "st82" if is_capped else "st81"
        slug = "deployment-scale-up-quota-cap" if is_capped else "deployment-scale-up-headroom"
        cq_name = "cq-dep-scaleup-cap" if is_capped else "cq-dep-scaleup"
        lq_name = "lq-dep-scaleup"
        tpu_q = 8 if is_capped else 16
        inf = _base_4node_infra(cq_name, lq_name, tpu_quota=tpu_q)

        d_init = _make_deployment_obj(
            "infer-api",
            det_uid_fn,
            fmt_ts_fn,
            local_queue=lq_name,
            replicas=2,
            tpu_per_pod=2,
            template_hash="th-0000000000000001",
        )
        d_up = _make_deployment_obj(
            "infer-api",
            det_uid_fn,
            fmt_ts_fn,
            local_queue=lq_name,
            replicas=6,
            tpu_per_pod=2,
            template_hash="th-0000000000000001",
        )
        wf = [
            _make_deployment_wf_event(
                fmt_ts_fn=fmt_ts_fn,
                event_offset_sec=0.0,
                creation_offset_sec=0.0,
                event_type="New",
                workload_state="NotAdmitted",
                name="infer-api",
                local_queue=lq_name,
                replicas=2,
                ready_replicas=0,
                paused=False,
                template_hash="th-0000000000000001",
                deploy_obj=d_init,
            ),
            _make_deployment_wf_event(
                fmt_ts_fn=fmt_ts_fn,
                event_offset_sec=10.0,
                creation_offset_sec=0.0,
                event_type="Update",
                workload_state="Admitted",
                name="infer-api",
                local_queue=lq_name,
                replicas=6,
                ready_replicas=2,
                paused=False,
                template_hash="th-0000000000000001",
                deploy_obj=d_up,
                update_types=["ScaleUp"],
            ),
        ]

        if not is_capped:
            scenarios.append(
                scenario_spec_cls(
                    scenario_id=sc_id,
                    slug=slug,
                    category="17. Deployment Scaling & Rolling Upgrades",
                    title="`Deployment` Scale-Up (`2 -> 6` Replicas) With Full `ClusterQueue` Headroom",
                    feature_focus="Verifies post-`t0` `Deployment` `ScaleUp` (`updateTypes: ['ScaleUp']`, `replicas: 2 -> 6` at `t=+10s`) creates `4` additional per-pod Kueue `Workload`s that admit immediately onto `16`-TPU `ClusterQueue` (`0` pending, long-running pods never prematurely terminated by KWOK `Stage/pod-complete`)",
                    recommended_cmd=f"python3 deploy/scripts/testrunner.py testscenarios/self-tests/{sc_id}-{slug} --duration 36s",
                    setup_summary="4 nodes (`16` TPUs), `cq-dep-scaleup` (`16` TPUs), `--duration 36s`. `Deployment` `infer-api` (`2` TPUs/pod, `restartPolicy: Always`) arrives at `t=0s` with `replicas: 2` (`4` TPUs) and scales up to `replicas: 6` (`12` TPUs) at `t=+10s`.",
                    expected_behavior="At `t=0s`, `2` replica `Workload`s admit (`4/16` TPUs = `25%`). At `t=+10s`, `ScheduledWorkflowUpdate` applies `replicas: 6`; `replicaset-controller` creates `4` new pods, `mpod.kb.io` gates them, and Kueue admits all `4` (`12/16` TPUs = `75%`). All `6` serving pods remain `Running` until `36s` (`succeeded=0`, `pending=0`).",
                    expected_metrics=[
                        "`Execution Summary`: `Workflows Submitted: 1/1`, `admitted=6, pending=0`, `pods running=6, succeeded=0`.",
                        "`Section 5.1`: `cq-dep-scaleup` `google.com/tpu` steps from `25%` (`4/16`) to **`75%` (`12/16` TPUs, `AVG ≈ 55%–65%`)**.",
                        "`Section 5.3`: **`0`** evictions.",
                        "`Section 5.4`: `Qualifying Workloads: 6 (completed=0, active_at_end=6)`, `EFFECTIVE SAT ≈ 54%–64%` (`16` physical TPUs).",
                    ],
                    must_not_happen=[
                        "KWOK `Stage/pod-complete` terminating `Deployment` pods after `10s` (`pods succeeded` MUST stay `0`).",
                        "Ignoring the `t=+10s` `Update` event and staying stuck at `2` replicas (`admitted=2`).",
                    ],
                    sec51_target="Steps 25% -> 75% (12/16 TPUs, AVG ~60%)",
                    sec52_target="p-std < 2.0s (6 replica Workloads)",
                    sec53_target="0 evictions",
                    sec54_target="54%–64% (active_at_end=6, pending=0)",
                    infra_events=inf,
                    workflow_events=wf,
                    exec_events=[],
                )
            )
        else:
            scenarios.append(
                scenario_spec_cls(
                    scenario_id=sc_id,
                    slug=slug,
                    category="17. Deployment Scaling & Rolling Upgrades",
                    title="`Deployment` Scale-Up (`2 -> 6` Replicas) Capped at `8`-TPU `ClusterQueue` Ceiling (Twin of st81)",
                    feature_focus="Direct A/B twin of `st81` halving `ClusterQueue` quota from `16` to `8` TPUs: `ScaleUp 2 -> 6` at `t=+10s` admits `2` new replicas (`4/6` running = `8/8` TPUs) while holding exact `2` excess replica `Workload`s gated in `LocalQueue` (`pending=2`, `Unadmitted: 2`)",
                    recommended_cmd=f"python3 deploy/scripts/testrunner.py testscenarios/self-tests/{sc_id}-{slug} --duration 36s",
                    setup_summary="Identical 4 nodes (`16` physical TPUs) and `infer-api` `Deployment` (`2 -> 6` replicas at `t=+10s`, `2` TPUs/pod) as `st81`, changing ONLY `cq-dep-scaleup-cap` `nominalQuota` from `16` to `8` TPUs (`--duration 36s`).",
                    expected_behavior="At `t=0s`, `2` replicas admit (`4/8` CQ TPUs = `50%`). At `t=+10s`, `infer-api` scales to `6` replicas: Kueue admits `2` more replicas (`8/8` CQ TPUs = `100%`) and keeps the remaining `2` pods gated (`schedulingGates: [kueue.x-k8s.io/admission]`, `pending=2`).",
                    expected_metrics=[
                        "`Execution Summary`: `admitted=4, pending=2`, `pods running=4, pending=2, succeeded=0`.",
                        "`Section 5.1`: `cq-dep-scaleup-cap` `google.com/tpu` hits **`100%` (`8/8` CQ TPUs)** after `t=+10s` (`AVG ≈ 80%–90%`).",
                        "`Section 5.3`: **`0`** evictions.",
                        "`Section 5.4`: `Qualifying Workloads: 4 (completed=0, active_at_end=4)`, `Unadmitted: 2 [2 pods]`, physical `EFFECTIVE SAT ≈ 38%–45%` (`8/16` cluster TPUs after `t=10s`).",
                    ],
                    must_not_happen=[
                        "Admitting more than `4` replicas (`> 8` TPUs) or scheduling gated pods onto idle physical nodes.",
                        "Counting the `2` gated pending replica `Workload`s in `Section 5.4` saturation.",
                    ],
                    sec51_target="Steps 50% -> 100% (8/8 CQ TPUs, AVG ~85%)",
                    sec52_target="p-std < 2.0s (4 admitted; 2 pending)",
                    sec53_target="0 evictions",
                    sec54_target="38%–45% (active_at_end=4, Unadmitted=2)",
                    infra_events=inf,
                    workflow_events=wf,
                    exec_events=[],
                )
            )

    # =========================================================================
    # st83 & st84: Deployment Scale-Down (8 -> 4 -> 0 Unblocks Batch vs. Control)
    # =========================================================================
    for with_scaledown in (True, False):
        sc_id = "st83" if with_scaledown else "st84"
        slug = (
            "deployment-scale-down-unblocks-batch-and-zero"
            if with_scaledown
            else "deployment-unscaled-blocks-batch-control"
        )
        cq_name = "cq-dep-scaledown" if with_scaledown else "cq-dep-unscaled"
        lq_name = "lq-dep-scaledown"
        inf = _base_4node_infra(cq_name, lq_name, tpu_quota=16)

        d8 = _make_deployment_obj(
            "srv-pool",
            det_uid_fn,
            fmt_ts_fn,
            local_queue=lq_name,
            replicas=8,
            tpu_per_pod=2,
            template_hash="th-0000000000000001",
        )
        wf_list: list[dict[str, Any]] = [
            _make_deployment_wf_event(
                fmt_ts_fn=fmt_ts_fn,
                event_offset_sec=0.0,
                creation_offset_sec=0.0,
                event_type="New",
                workload_state="NotAdmitted",
                name="srv-pool",
                local_queue=lq_name,
                replicas=8,
                ready_replicas=0,
                paused=False,
                template_hash="th-0000000000000001",
                deploy_obj=d8,
            )
        ]
        ex_list: list[dict[str, Any]] = []
        for b_idx in range(2):
            j_wf, j_ex = _make_batch_job_and_exec_events(
                name=f"batch-{b_idx}",
                det_uid_fn=det_uid_fn,
                fmt_ts_fn=fmt_ts_fn,
                local_queue=lq_name,
                cluster_queue=cq_name,
                priority_class="p-std",
                priority_value=100,
                arrival_offset_sec=2.0,
                admitted_offset_sec=10.5 if with_scaledown else None,
                duration_sec=8.0,
                parallelism=2,
                tpu_per_pod=2,
                cpu_per_pod=2,
                mem_gi_per_pod=8,
            )
            wf_list.append(j_wf)
            ex_list.extend(j_ex)

        if with_scaledown:
            d4 = _make_deployment_obj(
                "srv-pool",
                det_uid_fn,
                fmt_ts_fn,
                local_queue=lq_name,
                replicas=4,
                tpu_per_pod=2,
                template_hash="th-0000000000000001",
            )
            d0 = _make_deployment_obj(
                "srv-pool",
                det_uid_fn,
                fmt_ts_fn,
                local_queue=lq_name,
                replicas=0,
                tpu_per_pod=2,
                template_hash="th-0000000000000001",
            )
            wf_list.append(
                _make_deployment_wf_event(
                    fmt_ts_fn=fmt_ts_fn,
                    event_offset_sec=10.0,
                    creation_offset_sec=0.0,
                    event_type="Update",
                    workload_state="Admitted",
                    name="srv-pool",
                    local_queue=lq_name,
                    replicas=4,
                    ready_replicas=8,
                    paused=False,
                    template_hash="th-0000000000000001",
                    deploy_obj=d4,
                    update_types=["ScaleDown"],
                )
            )
            wf_list.append(
                _make_deployment_wf_event(
                    fmt_ts_fn=fmt_ts_fn,
                    event_offset_sec=26.0,
                    creation_offset_sec=0.0,
                    event_type="Update",
                    workload_state="NotAdmitted",
                    name="srv-pool",
                    local_queue=lq_name,
                    replicas=0,
                    ready_replicas=0,
                    paused=False,
                    template_hash="th-0000000000000001",
                    deploy_obj=d0,
                    update_types=["ScaleDown", "StateTransition"],
                )
            )
            scenarios.append(
                scenario_spec_cls(
                    scenario_id=sc_id,
                    slug=slug,
                    category="17. Deployment Scaling & Rolling Upgrades",
                    title="`Deployment` Scale-Down (`8 -> 4 -> 0`) Unblocking Batch `Job`s & Scale-to-Zero",
                    feature_focus="Verifies `ScaleDown` (`8 -> 4` at `t=+10s`) releases `8` TPUs of `ClusterQueue` quota so `2` queued `4`-TPU batch `Job`s admit and complete, followed by full scale-to-zero (`4 -> 0` at `t=+26s`) crediting all `8` retired serving replicas + `2` batch `Job`s in `Section 5.4`",
                    recommended_cmd=f"python3 deploy/scripts/testrunner.py testscenarios/self-tests/{sc_id}-{slug} --duration 34s",
                    setup_summary="4 nodes (`16` TPUs), `cq-dep-scaledown` (`16` TPUs), `--duration 34s`. `srv-pool` (`replicas: 8`, `2` TPUs/pod = `16` TPUs) saturates quota at `t=0s`; `batch-0` & `batch-1` (`4` TPUs each, `8s`) queue at `t=+2s`; `srv-pool` scales `8 -> 4` at `t=+10s` and `4 -> 0` at `t=+26s`.",
                    expected_behavior="`batch-0` and `batch-1` stay queued (`t=2..10s`) while `srv-pool` holds `16/16` TPUs. At `t=+10s`, `ScaleDown 8 -> 4` terminates `4` serving pods (`8` TPUs freed), unblocking both batch `Job`s (`wait ≈ 8s–10.5s`, finishing at `~t=19s`). At `t=+26s`, `ScaleDown 4 -> 0` retires the last `4` serving pods (`replicas: 0` preserved, never coerced to `1`), and replay completes cleanly with `10` completed Qualifying Workloads (`8` retired serving + `2` batch).",
                    expected_metrics=[
                        "`Execution Summary`: `Workflows Submitted: 3/3`, `pods running=0, succeeded=4` (`batch-0`, `batch-1`).",
                        "`Section 5.2`: `p-std` records `10` admitted workloads (`8` initial `t=0s` serving replicas + `2` batch `Job`s waiting `~8s–11s`).",
                        "`Section 5.3`: **`0`** evictions (clean `ScaleDown` retirement is NOT counted as Kueue eviction/badput).",
                        "`Section 5.4`: `Qualifying Workloads: 10 (completed=10 [12 pods], active_at_end=0 [0 pods])`, `Excluded: 0`, `Unadmitted: 0`.",
                    ],
                    must_not_happen=[
                        "Coercing `replicas: 0` to `1` (`pods running` at end MUST be `0`).",
                        "Dropping retired `srv-pool` replicas from `Section 5.4` or misclassifying clean scale-down as `Excluded (Interrupted)`.",
                    ],
                    sec51_target="100% (t=0..19s) -> 50% (t=19..26s) -> 0%",
                    sec52_target="batch wait ~8s–11s (10 total admitted)",
                    sec53_target="0 evictions",
                    sec54_target="75%–92% (completed=10, active_at_end=0)",
                    infra_events=inf,
                    workflow_events=wf_list,
                    exec_events=ex_list,
                )
            )
        else:
            scenarios.append(
                scenario_spec_cls(
                    scenario_id=sc_id,
                    slug=slug,
                    category="17. Deployment Scaling & Rolling Upgrades",
                    title="Unscaled `8`-Replica `Deployment` Blocking Queued Batch `Job`s (Control Twin of st83)",
                    feature_focus="Direct A/B control twin of `st83` omitting `ScaleDown` updates: `srv-pool` holds `16/16` TPUs continuously, keeping both `batch-0` and `batch-1` `Unadmitted: 2` for the entire `34s` window",
                    recommended_cmd=f"python3 deploy/scripts/testrunner.py testscenarios/self-tests/{sc_id}-{slug} --duration 34s",
                    setup_summary="Identical 4 nodes (`16` TPUs), `cq-dep-unscaled` (`16` TPUs), `srv-pool` (`replicas: 8`, `16` TPUs at `t=0s`), and `2` `4`-TPU batch `Job`s (`t=+2s`) as `st83`, omitting the `t=+10s` and `t=+26s` `ScaleDown` updates (`--duration 34s`).",
                    expected_behavior="`srv-pool` occupies `16/16` TPUs (`100%`) across the full `34s` window (`withinClusterQueue: Never`), so `batch-0` and `batch-1` never admit (`pending=2`, `pods succeeded=0`).",
                    expected_metrics=[
                        "`Execution Summary`: `Workflows Submitted: 3/3`, `admitted=8, pending=2`, `pods running=8, succeeded=0`.",
                        "`Section 5.1`: `cq-dep-unscaled` `google.com/tpu` stays at **`~95%–100%` (`16/16` TPUs)**.",
                        "`Section 5.3`: **`0`** evictions.",
                        "`Section 5.4`: `Qualifying Workloads: 8 (completed=0, active_at_end=8)`, `Unadmitted: 2 [4 pods]`, `EFFECTIVE SAT ≈ 92%–99%`.",
                    ],
                    must_not_happen=[
                        "Either `batch-0` or `batch-1` admitting while `srv-pool` holds `16/16` TPUs.",
                    ],
                    sec51_target="95%–100% (16/16 TPUs steady)",
                    sec52_target="p-std < 2.0s (8 serving; 2 batch unadmitted)",
                    sec53_target="0 evictions",
                    sec54_target="92%–99% (active_at_end=8, Unadmitted=2)",
                    infra_events=inf,
                    workflow_events=wf_list,
                    exec_events=ex_list,
                )
            )

    # =========================================================================
    # st85 & st86: Deployment Rolling Upgrade (maxUnavailable=1 vs. maxSurge=2)
    # =========================================================================
    for surge_deadlock in (False, True):
        sc_id = "st86" if surge_deadlock else "st85"
        slug = (
            "deployment-rolling-upgrade-surge-deadlock"
            if surge_deadlock
            else "deployment-rolling-upgrade-max-unavailable"
        )
        cq_name = "cq-dep-upgrade-surge" if surge_deadlock else "cq-dep-upgrade-clean"
        lq_name = "lq-dep-upgrade"
        max_unavail = 0 if surge_deadlock else 1
        max_srg = 2 if surge_deadlock else 0
        inf = _base_4node_infra(cq_name, lq_name, tpu_quota=8)

        d_v1 = _make_deployment_obj(
            "model-server",
            det_uid_fn,
            fmt_ts_fn,
            local_queue=lq_name,
            replicas=4,
            tpu_per_pod=2,
            template_hash="th-0000000000000001",
            max_unavailable=max_unavail,
            max_surge=max_srg,
        )
        d_v2 = _make_deployment_obj(
            "model-server",
            det_uid_fn,
            fmt_ts_fn,
            local_queue=lq_name,
            replicas=4,
            tpu_per_pod=2,
            template_hash="th-0000000000000002",
            max_unavailable=max_unavail,
            max_surge=max_srg,
        )
        wf_upg = [
            _make_deployment_wf_event(
                fmt_ts_fn=fmt_ts_fn,
                event_offset_sec=0.0,
                creation_offset_sec=0.0,
                event_type="New",
                workload_state="NotAdmitted",
                name="model-server",
                local_queue=lq_name,
                replicas=4,
                ready_replicas=0,
                paused=False,
                template_hash="th-0000000000000001",
                deploy_obj=d_v1,
            ),
            _make_deployment_wf_event(
                fmt_ts_fn=fmt_ts_fn,
                event_offset_sec=12.0,
                creation_offset_sec=0.0,
                event_type="Update",
                workload_state="Admitted",
                name="model-server",
                local_queue=lq_name,
                replicas=4,
                ready_replicas=4,
                paused=False,
                template_hash="th-0000000000000002",
                deploy_obj=d_v2,
                update_types=["Upgrade"],
            ),
        ]

        if not surge_deadlock:
            scenarios.append(
                scenario_spec_cls(
                    scenario_id=sc_id,
                    slug=slug,
                    category="17. Deployment Scaling & Rolling Upgrades",
                    title="`Deployment` Rolling Upgrade (`templateHash` `v1 -> v2`) With `maxUnavailable: 1, maxSurge: 0`",
                    feature_focus="Verifies privacy-preserving `kueue-optimizer.gke.io/template-hash` rolling `Upgrade` at `t=+12s` completes all `4` `v2` replicas inside a tight `8`-TPU `ClusterQueue` when `maxUnavailable: 1, maxSurge: 0` releases `1` `v1` pod before creating each `v2` pod",
                    recommended_cmd=f"python3 deploy/scripts/testrunner.py testscenarios/self-tests/{sc_id}-{slug} --duration 40s",
                    setup_summary="4 nodes (`16` physical TPUs), `cq-dep-upgrade-clean` (`8`-TPU quota = exact `4` pods × `2` TPUs), `--duration 40s`. `model-server` (`replicas: 4`, `maxUnavailable: 1, maxSurge: 0`) starts on `th-0000000000000001` at `t=0s` and upgrades to `th-0000000000000002` at `t=+12s`.",
                    expected_behavior="At `t=0s`, `4` `v1` pods fill `8/8` CQ TPUs. At `t=+12s`, `template-hash` changes to `th-0000000000000002`: `deployment-controller` scales down `v1` by `1` -> freeing `2` TPUs -> `1` `v2` pod admits and becomes `Ready` -> repeating 4 times until all `4` `v2` pods are `Running` (`8` total Kueue admissions = `4` cleanly retired `v1` + `4` active `v2`, `pending=0`).",
                    expected_metrics=[
                        "`Section 1 (kueue_admitted_workloads_total)` & `Section 5.2`: **`8`** total admitted replica `Workload`s (`4` `v1` + `4` `v2`), `pending=0` at test end.",
                        "`Section 5.1`: `cq-dep-upgrade-clean` stays near **`88%–100%` (`~7.5–8 / 8` CQ TPUs)**.",
                        "`Section 5.3`: **`0`** evictions.",
                        "`Section 5.4`: `Qualifying Workloads: 8 (completed=4, active_at_end=4)`, `Excluded: 0`, `Unadmitted: 0`, physical `EFFECTIVE SAT ≈ 44%–50%` (`8/16` cluster TPUs).",
                    ],
                    must_not_happen=[
                        "Sanitization stripping the `v1 -> v2` template diff (`admitted_workloads_total` MUST be `8`, never `4`).",
                        "Rollout stalling with `pending > 0` at `t=40s`.",
                    ],
                    sec51_target="88%–100% (~8/8 CQ TPUs across rollout)",
                    sec52_target="p-std < 2.5s (8 total admitted: 4 v1 + 4 v2)",
                    sec53_target="0 evictions",
                    sec54_target="44%–50% (completed=4 v1 + active_at_end=4 v2)",
                    infra_events=inf,
                    workflow_events=wf_upg,
                    exec_events=[],
                )
            )
        else:
            scenarios.append(
                scenario_spec_cls(
                    scenario_id=sc_id,
                    slug=slug,
                    category="18. Deployment Pauses, `t0` Status & Lifecycle",
                    title="`Deployment` Rolling Upgrade Surge Quota Deadlock (`maxUnavailable: 0, maxSurge: 2` — Twin of st85)",
                    feature_focus="Direct A/B twin of `st85` changing ONLY `rollingUpdate` to `{maxUnavailable: 0, maxSurge: 2}` in the same `8`-TPU `ClusterQueue`: `2` surge `v2` pods block on Kueue admission gate while `4` `v1` pods hold `8/8` TPUs, reproducing the classic Kueue surge rollout deadlock",
                    recommended_cmd=f"python3 deploy/scripts/testrunner.py testscenarios/self-tests/{sc_id}-{slug} --duration 40s",
                    setup_summary="Identical 4 nodes (`16` physical TPUs), `8`-TPU `ClusterQueue`, and `model-server` `v1 -> v2` `Upgrade` at `t=+12s` as `st85`, changing ONLY `strategy.rollingUpdate` from `{maxUnavailable: 1, maxSurge: 0}` to `{maxUnavailable: 0, maxSurge: 2}` (`--duration 40s`).",
                    expected_behavior="At `t=0s`, `4` `v1` pods fill `8/8` CQ TPUs. At `t=+12s`, `deployment-controller` creates `2` surge `v2` pods WITHOUT terminating any `v1` pod (`maxUnavailable: 0`). Because `8/8` CQ TPUs are already held by `v1`, both `v2` surge `Workload`s stay `QuotaReserved=False` (`pending=2`), freezing the rollout at `4` `v1` running + `2` `v2` gated (`4` total admissions vs. `8` in `st85`).",
                    expected_metrics=[
                        "`Execution Summary`: `admitted=4, pending=2` (`4` total admissions vs. `8` in `st85`).",
                        "`Section 5.1`: `cq-dep-upgrade-surge` holds **`95%–100%` (`8/8` CQ TPUs)**.",
                        "`Section 5.3`: **`0`** evictions.",
                        "`Section 5.4`: `Qualifying Workloads: 4 (completed=0, active_at_end=4)`, `Unadmitted: 2 [2 pods]`, physical `EFFECTIVE SAT ≈ 45%–50%`.",
                    ],
                    must_not_happen=[
                        "Completing the `v2` rollout when `maxUnavailable: 0` and `0` spare CQ quota exists (`pending` MUST be `2` at `t=40s`).",
                    ],
                    sec51_target="95%–100% (8/8 CQ TPUs held by v1)",
                    sec52_target="p-std < 2.0s (4 v1 admitted; 2 v2 pending)",
                    sec53_target="0 evictions",
                    sec54_target="45%–50% (active_at_end=4 v1, Unadmitted=2 v2)",
                    infra_events=inf,
                    workflow_events=wf_upg,
                    exec_events=[],
                )
            )

    # =========================================================================
    # st87 & st88: Deployment Pause & Resume vs. Unpaused Immediate Upgrade
    # =========================================================================
    for with_pause in (True, False):
        sc_id = "st87" if with_pause else "st88"
        slug = (
            "deployment-pause-defers-rollout-and-resume"
            if with_pause
            else "deployment-unpaused-immediate-rollout-control"
        )
        cq_name = "cq-dep-paused" if with_pause else "cq-dep-unpaused"
        lq_name = "lq-dep-pause"
        inf = _base_4node_infra(cq_name, lq_name, tpu_quota=16)

        d_init_1tpu = _make_deployment_obj(
            "embed-svc",
            det_uid_fn,
            fmt_ts_fn,
            local_queue=lq_name,
            replicas=3,
            tpu_per_pod=1,
            paused=False,
            template_hash="th-0000000000000010",
        )
        wf_pr: list[dict[str, Any]] = [
            _make_deployment_wf_event(
                fmt_ts_fn=fmt_ts_fn,
                event_offset_sec=0.0,
                creation_offset_sec=0.0,
                event_type="New",
                workload_state="NotAdmitted",
                name="embed-svc",
                local_queue=lq_name,
                replicas=3,
                ready_replicas=0,
                paused=False,
                template_hash="th-0000000000000010",
                deploy_obj=d_init_1tpu,
            )
        ]

        if with_pause:
            d_paused = _make_deployment_obj(
                "embed-svc",
                det_uid_fn,
                fmt_ts_fn,
                local_queue=lq_name,
                replicas=3,
                tpu_per_pod=1,
                paused=True,
                template_hash="th-0000000000000010",
            )
            d_upg_while_paused = _make_deployment_obj(
                "embed-svc",
                det_uid_fn,
                fmt_ts_fn,
                local_queue=lq_name,
                replicas=3,
                tpu_per_pod=4,
                paused=True,
                template_hash="th-0000000000000020",
            )
            d_resumed = _make_deployment_obj(
                "embed-svc",
                det_uid_fn,
                fmt_ts_fn,
                local_queue=lq_name,
                replicas=3,
                tpu_per_pod=4,
                paused=False,
                template_hash="th-0000000000000020",
            )
            wf_pr.extend(
                [
                    _make_deployment_wf_event(
                        fmt_ts_fn=fmt_ts_fn,
                        event_offset_sec=8.0,
                        creation_offset_sec=0.0,
                        event_type="Update",
                        workload_state="Admitted",
                        name="embed-svc",
                        local_queue=lq_name,
                        replicas=3,
                        ready_replicas=3,
                        paused=True,
                        template_hash="th-0000000000000010",
                        deploy_obj=d_paused,
                        update_types=["Paused"],
                    ),
                    _make_deployment_wf_event(
                        fmt_ts_fn=fmt_ts_fn,
                        event_offset_sec=11.0,
                        creation_offset_sec=0.0,
                        event_type="Update",
                        workload_state="Admitted",
                        name="embed-svc",
                        local_queue=lq_name,
                        replicas=3,
                        ready_replicas=3,
                        paused=True,
                        template_hash="th-0000000000000020",
                        deploy_obj=d_upg_while_paused,
                        update_types=["Upgrade"],
                    ),
                    _make_deployment_wf_event(
                        fmt_ts_fn=fmt_ts_fn,
                        event_offset_sec=24.0,
                        creation_offset_sec=0.0,
                        event_type="Update",
                        workload_state="Admitted",
                        name="embed-svc",
                        local_queue=lq_name,
                        replicas=3,
                        ready_replicas=3,
                        paused=False,
                        template_hash="th-0000000000000020",
                        deploy_obj=d_resumed,
                        update_types=["Resumed"],
                    ),
                ]
            )
            scenarios.append(
                scenario_spec_cls(
                    scenario_id=sc_id,
                    slug=slug,
                    category="18. Deployment Pauses, `t0` Status & Lifecycle",
                    title="`Deployment` Pause (`spec.paused: true`) Deferring `1 -> 4` TPU Upgrade Until Resume",
                    feature_focus="Verifies `Paused` (`t=+8s`) freezes `Deployment` rollout when a `1 -> 4` TPU/pod `Upgrade` arrives at `t=+11s` (keeping usage at `3/16` TPUs across `t=0..24s`), and `Resumed` (`spec.paused: false` via Server-Side Apply at `t=+24s`) triggers the `12`-TPU rollout",
                    recommended_cmd=f"python3 deploy/scripts/testrunner.py testscenarios/self-tests/{sc_id}-{slug} --duration 40s",
                    setup_summary="4 nodes (`16` TPUs), `cq-dep-paused` (`16` TPUs), `--duration 40s`. `embed-svc` (`replicas: 3`, `1` TPU/pod = `3` TPUs) starts at `t=0s`, pauses at `t=+8s`, upgrades template to `4` TPUs/pod (`12` TPUs) at `t=+11s` while paused, and resumes at `t=+24s`.",
                    expected_behavior="During `t=0..24s`, `embed-svc` consumes only `3/16` TPUs (`18.75%`) because `spec.paused: true` blocks `deployment-controller` from rolling out the `t=+11s` `4`-TPU template. At `t=+24s`, `spec.paused: false` unblocks the rollout, jumping quota usage to `12/16` TPUs (`75%`) for `t=24..40s` (`AVG ≈ 35%–46%`).",
                    expected_metrics=[
                        "`Section 1 (kueue_admitted_workloads_total)`: **`6`** admitted replica `Workload`s (`3` `1`-TPU `v1` + `3` `4`-TPU `v2`).",
                        "`Section 5.1`: `cq-dep-paused` `google.com/tpu` stays at `18.75%` (`3/16`) until `t=24s`, then jumps to `75%` (`12/16`, **`AVG ≈ 35%–46%`**).",
                        "`Section 5.3`: **`0`** evictions.",
                        "`Section 5.4`: `Qualifying Workloads: 6 (completed=3, active_at_end=3)`, `EFFECTIVE SAT ≈ 36%–45%` (strictly lower than `st88`'s `~54%–63%`).",
                    ],
                    must_not_happen=[
                        "Rolling out `4`-TPU pods at `t=+11s` while `spec.paused: true`.",
                        "Server-Side Apply failing to clear `spec.paused` back to `false` at `t=+24s` (leaving usage stuck at `3` TPUs).",
                    ],
                    sec51_target="18.75% (t=0..24s) -> 75% (t=24..40s, AVG ~41%)",
                    sec52_target="p-std < 2.5s (6 total admitted: 3 v1 + 3 v2)",
                    sec53_target="0 evictions",
                    sec54_target="36%–45% (completed=3 v1 + active_at_end=3 v2)",
                    infra_events=inf,
                    workflow_events=wf_pr,
                    exec_events=[],
                )
            )
        else:
            d_upg_immediate = _make_deployment_obj(
                "embed-svc",
                det_uid_fn,
                fmt_ts_fn,
                local_queue=lq_name,
                replicas=3,
                tpu_per_pod=4,
                paused=False,
                template_hash="th-0000000000000020",
            )
            wf_pr.append(
                _make_deployment_wf_event(
                    fmt_ts_fn=fmt_ts_fn,
                    event_offset_sec=11.0,
                    creation_offset_sec=0.0,
                    event_type="Update",
                    workload_state="Admitted",
                    name="embed-svc",
                    local_queue=lq_name,
                    replicas=3,
                    ready_replicas=3,
                    paused=False,
                    template_hash="th-0000000000000020",
                    deploy_obj=d_upg_immediate,
                    update_types=["Upgrade"],
                )
            )
            scenarios.append(
                scenario_spec_cls(
                    scenario_id=sc_id,
                    slug=slug,
                    category="18. Deployment Pauses, `t0` Status & Lifecycle",
                    title="Unpaused `Deployment` Immediate `1 -> 4` TPU Upgrade at `t=+11s` (Control Twin of st87)",
                    feature_focus="Direct A/B control twin of `st87` omitting `Paused`/`Resumed`: the `t=+11s` `1 -> 4` TPU/pod `Upgrade` rolls out `13s` earlier (`t=11s` vs. `t=24s`), lifting time-weighted TPU utilization and `Section 5.4` saturation by `~16–20pp`",
                    recommended_cmd=f"python3 deploy/scripts/testrunner.py testscenarios/self-tests/{sc_id}-{slug} --duration 40s",
                    setup_summary="Identical 4 nodes (`16` TPUs), `cq-dep-unpaused` (`16` TPUs), `embed-svc` (`replicas: 3`, `1` TPU/pod at `t=0s`), and `4`-TPU/pod `Upgrade` at `t=+11s` as `st87`, omitting the `t=+8s` `Paused` and `t=+24s` `Resumed` events (`--duration 40s`).",
                    expected_behavior="At `t=+11s`, `embed-svc` immediately rolls from `3 x 1` TPU (`18.75%`) to `3 x 4` TPUs (`12/16 = 75%`) and runs at `12` TPUs for `~29s` (`t=11..40s`) instead of `~16s` (`t=24..40s` in `st87`).",
                    expected_metrics=[
                        "`Section 1 (kueue_admitted_workloads_total)`: **`6`** admitted replica `Workload`s (`3` `v1` + `3` `v2`).",
                        "`Section 5.1`: `cq-dep-unpaused` `google.com/tpu` jumps to `75%` (`12/16`) at `t=+11s` (**`AVG ≈ 54%–65%`** > `st87` `~35%–46%`).",
                        "`Section 5.3`: **`0`** evictions.",
                        "`Section 5.4`: `Qualifying Workloads: 6 (completed=3, active_at_end=3)`, `EFFECTIVE SAT ≈ 54%–63%` (`+16–20pp` above `st87`).",
                    ],
                    must_not_happen=[
                        "`Section 5.4 EFFECTIVE SAT` in `st88` being lower than or equal to paused `st87`.",
                    ],
                    sec51_target="18.75% (t=0..11s) -> 75% (t=11..40s, AVG ~59%)",
                    sec52_target="p-std < 2.5s (6 total admitted: 3 v1 + 3 v2)",
                    sec53_target="0 evictions",
                    sec54_target="54%–63% (completed=3 v1 + active_at_end=3 v2)",
                    infra_events=inf,
                    workflow_events=wf_pr,
                    exec_events=[],
                )
            )

    # =========================================================================
    # st89: t0 Deployment Full & Partial Readiness Status + Preemption & Recovery
    # =========================================================================
    inf89 = _base_4node_infra(
        "cq-dep-t0-preempt",
        "lq-dep-t0",
        tpu_quota=16,
        within_cq_preemption="LowerPriority",
        extra_pcs=[("p-low", 50), ("p-high", 200)],
    )
    wf89: list[dict[str, Any]] = []
    ex89: list[dict[str, Any]] = []

    # 1. steady-srv: p-std (100), 2/2 ready at t0 (4 TPUs on st-node-00)
    d_steady = _make_deployment_obj(
        "steady-srv",
        det_uid_fn,
        fmt_ts_fn,
        local_queue="lq-dep-t0",
        replicas=2,
        tpu_per_pod=2,
        priority_class="p-std",
        template_hash="th-0000000000000089",
    )
    wf89.append(
        _make_deployment_wf_event(
            fmt_ts_fn=fmt_ts_fn,
            event_offset_sec=0.0,
            creation_offset_sec=0.0,
            event_type="New",
            workload_state="Admitted",
            name="steady-srv",
            local_queue="lq-dep-t0",
            replicas=2,
            ready_replicas=2,
            paused=False,
            template_hash="th-0000000000000089",
            deploy_obj=d_steady,
        )
    )
    for r_i in range(2):
        p_ev, e_evs = _make_t0_deployment_pod_and_exec(
            deploy_name="steady-srv",
            replica_idx=r_i,
            placement_node="st-node-00",
            fmt_ts_fn=fmt_ts_fn,
            local_queue="lq-dep-t0",
            cluster_queue="cq-dep-t0-preempt",
            priority_class="p-std",
            priority_value=100,
        )
        inf89.append(p_ev)
        ex89.extend(e_evs)

    # 2. bg-embed: p-low (50), 2/2 ready at t0 (4 TPUs on st-node-01)
    d_bg = _make_deployment_obj(
        "bg-embed",
        det_uid_fn,
        fmt_ts_fn,
        local_queue="lq-dep-t0",
        replicas=2,
        tpu_per_pod=2,
        priority_class="p-low",
        template_hash="th-0000000000000089",
    )
    wf89.append(
        _make_deployment_wf_event(
            fmt_ts_fn=fmt_ts_fn,
            event_offset_sec=0.0,
            creation_offset_sec=0.0,
            event_type="New",
            workload_state="Admitted",
            name="bg-embed",
            local_queue="lq-dep-t0",
            replicas=2,
            ready_replicas=2,
            paused=False,
            template_hash="th-0000000000000089",
            deploy_obj=d_bg,
        )
    )
    for r_i in range(2):
        p_ev, e_evs = _make_t0_deployment_pod_and_exec(
            deploy_name="bg-embed",
            replica_idx=r_i,
            placement_node="st-node-01",
            fmt_ts_fn=fmt_ts_fn,
            local_queue="lq-dep-t0",
            cluster_queue="cq-dep-t0-preempt",
            priority_class="p-low",
            priority_value=50,
        )
        inf89.append(p_ev)
        ex89.extend(e_evs)

    # 3. partial-api: p-low (50), replicas=6, readyReplicas=4 at t0 (8 TPUs on st-node-02..03; 2 pending!)
    d_partial = _make_deployment_obj(
        "partial-api",
        det_uid_fn,
        fmt_ts_fn,
        local_queue="lq-dep-t0",
        replicas=6,
        tpu_per_pod=2,
        priority_class="p-low",
        template_hash="th-0000000000000089",
    )
    wf89.append(
        _make_deployment_wf_event(
            fmt_ts_fn=fmt_ts_fn,
            event_offset_sec=0.0,
            creation_offset_sec=0.0,
            event_type="New",
            workload_state="Admitted",
            name="partial-api",
            local_queue="lq-dep-t0",
            replicas=6,
            ready_replicas=4,
            paused=False,
            template_hash="th-0000000000000089",
            deploy_obj=d_partial,
        )
    )
    for r_i in range(4):
        p_ev, e_evs = _make_t0_deployment_pod_and_exec(
            deploy_name="partial-api",
            replica_idx=r_i,
            placement_node=f"st-node-{2 + (r_i // 2):02d}",
            fmt_ts_fn=fmt_ts_fn,
            local_queue="lq-dep-t0",
            cluster_queue="cq-dep-t0-preempt",
            priority_class="p-low",
            priority_value=50,
        )
        inf89.append(p_ev)
        ex89.extend(e_evs)

    # 4. urgent-eval: p-high (200), 4 pods x 2 TPU = 8 TPUs, 10s batch Job at t=+8s
    j_urg_wf, j_urg_ex = _make_batch_job_and_exec_events(
        name="urgent-eval",
        det_uid_fn=det_uid_fn,
        fmt_ts_fn=fmt_ts_fn,
        local_queue="lq-dep-t0",
        cluster_queue="cq-dep-t0-preempt",
        priority_class="p-high",
        priority_value=200,
        arrival_offset_sec=8.0,
        admitted_offset_sec=8.5,
        duration_sec=10.0,
        parallelism=4,
        tpu_per_pod=2,
        cpu_per_pod=2,
        mem_gi_per_pod=8,
    )
    wf89.append(j_urg_wf)
    ex89.extend(j_urg_ex)

    scenarios.append(
        scenario_spec_cls(
            scenario_id="st89",
            slug="deployment-t0-partial-status-and-preemption",
            category="18. Deployment Pauses, `t0` Status & Lifecycle",
            title="`t0` `Deployment` Full (`2/2`) & Partial (`4/6`) Readiness + `LowerPriority` Preemption & Recovery",
            feature_focus="Verifies `InitialDeploymentBootstrap` pre-admits exact `readyReplicas` (`steady-srv` `2/2`, `bg-embed` `2/2`, `partial-api` `4/6` with `2` gated in `LocalQueue`) at `t0` with `0` extra `admitted_workloads_total`, then preempts `4` `p-low` serving replicas when `p-high` `urgent-eval` (`8` TPUs, `10s`) arrives at `t=+8s`, and automatically heals via `ReplicaSet` after `t=+18s`",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st89-deployment-t0-partial-status-and-preemption --no-skip-initial-offset --duration 38s",
            setup_summary="4 nodes (`16` TPUs), `cq-dep-t0-preempt` (`16` TPUs, `withinClusterQueue: LowerPriority`), `--no-skip-initial-offset --duration 38s`. At `t0`: `steady-srv` (`p-std=100`, `2/2` = `4` TPUs), `bg-embed` (`p-low=50`, `2/2` = `4` TPUs), and partially admitted `partial-api` (`p-low=50`, `4/6` = `8` TPUs running + `2` pending = `16/16` TPUs full). At `t=+8s`: `urgent-eval` (`p-high=200`, `8` TPUs, `10s` `Job`) arrives.",
            expected_behavior="At `t0`, `8` replica `Workload`s are pre-admitted (`8/10` desired replicas) and `partial-api`'s remaining `2` replicas stay queued in `LocalQueue`. At `t=+8s`, `urgent-eval` (`p-high=200`) preempts `4` `p-low` serving replicas (`8` TPUs) while protecting `steady-srv` (`p-std=100`); `ReplicaSet` immediately creates `4` replacement gated pods. Once `urgent-eval` finishes at `~t=18.5s`, `4` queued `p-low` serving replicas re-admit (`admitted=8, pending=2` restored).",
            expected_metrics=[
                "`Execution Summary`: `Initial Deployments: 3 Deployment(s) (8/10 replica(s) ready at t0)`, `Initial Infra Pods: 0`.",
                "`Section 5.2`: `p-high` (`urgent-eval`) waits **`< 2.5s`**; `4` `p-low` serving replicas admit after `t=18.5s`.",
                "`Section 5.3`: **`Total Evictions == 4`** (`InClusterQueue` preemptions of `p-low` serving replicas; `0` `p-std` evictions).",
                "`Section 5.4`: `completed=1` (`urgent-eval`), `active_at_end=8`, `Excluded: 4` (the `4` preempted `t0` serving replicas), `Unadmitted: 2` (`2` excess `p-low` replicas exceeding `16`-TPU quota).",
            ],
            must_not_happen=[
                "Pre-admitting all `6/6` `partial-api` replicas at `t0` (`readyReplicas: 4` MUST keep `2` gated).",
                "Duplicating `t0` `Deployment` pods into `Initial Infra Pods` (`Initial Infra Pods` MUST be `0`).",
                "Evicting `p-std` `steady-srv` instead of `p-low` replicas.",
            ],
            sec51_target="92%–100% (16/16 TPUs across t0, preemption & heal)",
            sec52_target="p-high < 2.5s; 4 healed p-low admit post-18s",
            sec53_target="4 InClusterQueue preemptions (p-low)",
            sec54_target="72%–86% (completed=1, active_at_end=8, Excluded=4)",
            infra_events=inf89,
            workflow_events=wf89,
            exec_events=ex89,
        )
    )

    # =========================================================================
    # st90: t0 Deployment Deletion, Same-Name Re-Creation & Canary Creation/Deletion
    # =========================================================================
    inf90 = _base_4node_infra("cq-dep-lifecycle", "lq-dep-lifecycle", tpu_quota=16)
    wf90: list[dict[str, Any]] = []
    ex90: list[dict[str, Any]] = []

    d_eph_inc0 = _make_deployment_obj(
        "ephemeral-srv",
        det_uid_fn,
        fmt_ts_fn,
        local_queue="lq-dep-lifecycle",
        replicas=4,
        tpu_per_pod=2,
        template_hash="th-0000000000000090",
        creation_offset_sec=0.0,
        incarnation=0,
    )
    wf90.append(
        _make_deployment_wf_event(
            fmt_ts_fn=fmt_ts_fn,
            event_offset_sec=0.0,
            creation_offset_sec=0.0,
            event_type="New",
            workload_state="Admitted",
            name="ephemeral-srv",
            local_queue="lq-dep-lifecycle",
            replicas=4,
            ready_replicas=4,
            paused=False,
            template_hash="th-0000000000000090",
            deploy_obj=d_eph_inc0,
        )
    )
    for r_i in range(4):
        p_ev, e_evs = _make_t0_deployment_pod_and_exec(
            deploy_name="ephemeral-srv",
            replica_idx=r_i,
            placement_node=f"st-node-{r_i:02d}",
            fmt_ts_fn=fmt_ts_fn,
            local_queue="lq-dep-lifecycle",
            cluster_queue="cq-dep-lifecycle",
            priority_class="p-std",
            priority_value=100,
            scrape_end_sec=8.0,
        )
        inf90.append(p_ev)
        ex90.extend(e_evs)

    # Delete incarnation 0 of ephemeral-srv at t=+8s
    wf90.append(
        _make_deployment_wf_event(
            fmt_ts_fn=fmt_ts_fn,
            event_offset_sec=8.0,
            creation_offset_sec=0.0,
            event_type="Deleted",
            workload_state="Admitted",
            name="ephemeral-srv",
            local_queue="lq-dep-lifecycle",
            replicas=4,
            ready_replicas=4,
            paused=False,
            template_hash="th-0000000000000090",
            deploy_obj=d_eph_inc0,
        )
    )

    # Re-create incarnation 1 of ephemeral-srv (replicas=6 -> 12 TPUs) + create canary-srv (replicas=2 -> 4 TPUs) at t=+12s
    d_eph_inc1 = _make_deployment_obj(
        "ephemeral-srv",
        det_uid_fn,
        fmt_ts_fn,
        local_queue="lq-dep-lifecycle",
        replicas=6,
        tpu_per_pod=2,
        template_hash="th-0000000000000091",
        creation_offset_sec=12.0,
        incarnation=1,
    )
    d_canary = _make_deployment_obj(
        "canary-srv",
        det_uid_fn,
        fmt_ts_fn,
        local_queue="lq-dep-lifecycle",
        replicas=2,
        tpu_per_pod=2,
        template_hash="th-0000000000000092",
        creation_offset_sec=12.0,
        incarnation=0,
    )
    wf90.append(
        _make_deployment_wf_event(
            fmt_ts_fn=fmt_ts_fn,
            event_offset_sec=12.0,
            creation_offset_sec=12.0,
            event_type="New",
            workload_state="NotAdmitted",
            name="ephemeral-srv",
            local_queue="lq-dep-lifecycle",
            replicas=6,
            ready_replicas=0,
            paused=False,
            template_hash="th-0000000000000091",
            deploy_obj=d_eph_inc1,
        )
    )
    wf90.append(
        _make_deployment_wf_event(
            fmt_ts_fn=fmt_ts_fn,
            event_offset_sec=12.0,
            creation_offset_sec=12.0,
            event_type="New",
            workload_state="NotAdmitted",
            name="canary-srv",
            local_queue="lq-dep-lifecycle",
            replicas=2,
            ready_replicas=0,
            paused=False,
            template_hash="th-0000000000000092",
            deploy_obj=d_canary,
        )
    )

    # Delete canary-srv at t=+26s
    wf90.append(
        _make_deployment_wf_event(
            fmt_ts_fn=fmt_ts_fn,
            event_offset_sec=26.0,
            creation_offset_sec=12.0,
            event_type="Deleted",
            workload_state="Admitted",
            name="canary-srv",
            local_queue="lq-dep-lifecycle",
            replicas=2,
            ready_replicas=2,
            paused=False,
            template_hash="th-0000000000000092",
            deploy_obj=d_canary,
        )
    )

    scenarios.append(
        scenario_spec_cls(
            scenario_id="st90",
            slug="deployment-t0-delete-and-recreate-lifecycle",
            category="18. Deployment Pauses, `t0` Status & Lifecycle",
            title="`t0` `Deployment` Deletion, Same-Name Incarnation-`1` Re-Creation & Canary Creation/Deletion",
            feature_focus="Verifies full `Deployment` deletion & creation lifecycle: `t0` `ephemeral-srv` (`4/4` = `8` TPUs) is deleted at `t=+8s` (freeing `100%` of its ReplicaSet/Pods/Workloads), re-created with the same name at `t=+12s` (`replicas: 6` = `12` TPUs) alongside new `canary-srv` (`replicas: 2` = `4` TPUs -> `16/16` TPUs), and `canary-srv` is deleted at `t=+26s` without touching `ephemeral-srv`",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st90-deployment-t0-delete-and-recreate-lifecycle --no-skip-initial-offset --duration 38s",
            setup_summary="4 nodes (`16` TPUs), `cq-dep-lifecycle` (`16` TPUs), `--no-skip-initial-offset --duration 38s`. `t0`: `ephemeral-srv` inc `0` (`4/4` = `8` TPUs); `t=+8s`: `Deleted` `ephemeral-srv` inc `0`; `t=+12s`: `New` `ephemeral-srv` inc `1` (`replicas: 6` = `12` TPUs) + `New` `canary-srv` (`replicas: 2` = `4` TPUs); `t=+26s`: `Deleted` `canary-srv`.",
            expected_behavior="At `t=+8s`, `_execute_workflow_deletion` deletes `ephemeral-srv` inc `0`, its `ReplicaSet`, and its `4` pods (`wait_gone=True`), dropping usage `8 -> 0` TPUs and crediting `4` cleanly retired serving replicas. At `t=+12s`, `ephemeral-srv` inc `1` (`6` pods) and `canary-srv` (`2` pods) admit cleanly (`16/16` TPUs = `100%`). At `t=+26s`, deleting `canary-srv` removes only `canary-srv`'s `ReplicaSet`/pods (`16 -> 12` TPUs = `75%`), leaving `ephemeral-srv` inc `1`'s `6` pods running (`completed=6`, `active_at_end=6`, `Excluded=0`).",
            expected_metrics=[
                "`Execution Summary`: `Initial Deployments: 1 Deployment(s) (4/4 replica(s) ready at t0)`, `Workflows Submitted: 2/2`, `admitted=6, pending=0`, `pods running=6` at `t=38s`.",
                "`Section 1 (kueue_admitted_workloads_total)`: **`8`** post-`t0` admissions (`6` `ephemeral-srv` inc `1` + `2` `canary-srv`; `t0` inc `0` adds `0` counter increments).",
                "`Section 5.3`: **`0`** evictions (graceful `Deployment` deletions are NOT Kueue evictions).",
                "`Section 5.4`: `Qualifying Workloads: 12 (completed=6, active_at_end=6)` (`4` inc-`0` + `2` `canary-srv` retired + `6` inc-`1` active), `Excluded: 0`.",
            ],
            must_not_happen=[
                "Stale `ReplicaSet` or pods from `ephemeral-srv` inc `0` surviving `t=+8s` deletion and blocking inc `1`'s `12` + `canary-srv`'s `4` = `16` TPUs.",
                "Deleting `canary-srv` at `t=+26s` accidentally killing any pod of `ephemeral-srv` inc `1`.",
                "Misclassifying deleted `t0` or `canary-srv` serving replicas as `Excluded (Interrupted)` (`Excluded` MUST be `0`).",
            ],
            sec51_target="50% (t0..8s) -> 0% -> 100% (12..26s) -> 75%",
            sec52_target="p-std < 2.5s (8 post-t0 replica Workloads)",
            sec53_target="0 evictions",
            sec54_target="62%–76% (completed=6 retired + active_at_end=6)",
            infra_events=inf90,
            workflow_events=wf90,
            exec_events=ex90,
        )
    )

    return scenarios


DEPLOYMENT_SCENARIO_PAIRINGS: dict[str, list[dict[str, str]]] = {
    "st81": [
        {
            "peer_id": "st82",
            "relationship": "Exact `Deployment` Scale-Up A/B Twin (`16`-TPU Full Headroom `st81` vs. `8`-TPU Quota Cap `st82`)",
            "knob_changed": "`ClusterQueue` `google.com/tpu` `nominalQuota` changes from `16` (`st81`) to `8` (`st82`) on identical `infer-api` `ScaleUp 2 -> 6` (`2` TPUs/pod) at `t=+10s`.",
            "cross_check": "`st81` admits all `6` replicas (`admitted=6, pending=0`, `Sec 5.4 active_at_end=6`), whereas `st82` caps admission at `4` replicas (`admitted=4, pending=2`, `Sec 5.1 = 100%` of `8`-TPU CQ, `Sec 5.4 Unadmitted=2`).",
        },
    ],
    "st82": [
        {
            "peer_id": "st81",
            "relationship": "Exact `Deployment` Scale-Up A/B Twin (`8`-TPU Quota Cap `st82` vs. `16`-TPU Full Headroom `st81`)",
            "knob_changed": "`ClusterQueue` `google.com/tpu` `nominalQuota` (`8` in `st82` vs. `16` in `st81`).",
            "cross_check": "Proves Kueue gates individual `Deployment` scale-up pods at the `ClusterQueue` quota boundary (`4` running + `2` gated in `st82` vs. `6` running in `st81`, `0` evictions in both).",
        },
    ],
    "st83": [
        {
            "peer_id": "st84",
            "relationship": "Exact `Deployment` Scale-Down A/B Twin (`ScaleDown 8 -> 4 -> 0` `st83` vs. Unscaled `8`-Replica Blocker `st84`)",
            "knob_changed": "Replaying post-`t0` `ScaleDown` updates (`8 -> 4` at `t=+10s`, `4 -> 0` at `t=+26s` in `st83`) vs. omitting `ScaleDown` updates (`st84`) on identical `srv-pool` + `2` `4`-TPU batch `Job`s.",
            "cross_check": "`st83` unblocks both `batch-0` and `batch-1` at `t=+10s` (`pods succeeded=2`, `Unadmitted=0`) and scales cleanly to `0` at `t=+26s` (`completed=10, active_at_end=0`), whereas `st84` starves both batch `Job`s (`pods succeeded=0`, `Unadmitted=2`, `active_at_end=8`).",
        },
    ],
    "st84": [
        {
            "peer_id": "st83",
            "relationship": "Exact `Deployment` Scale-Down A/B Twin (Unscaled Control `st84` vs. `ScaleDown 8 -> 4 -> 0` `st83`)",
            "knob_changed": "Omitting `ScaleDown` updates (`st84`) vs. applying `8 -> 4 -> 0` (`st83`).",
            "cross_check": "Confirms `srv-pool`'s `8` long-running pods (`restartPolicy: Always`) never prematurely complete and permanently hold `16/16` TPUs until an explicit `ScaleDown` or `Deleted` event frees quota.",
        },
    ],
    "st85": [
        {
            "peer_id": "st86",
            "relationship": "Exact `Deployment` Rolling-Upgrade Strategy A/B Twin (`maxUnavailable: 1, maxSurge: 0` `st85` vs. `maxUnavailable: 0, maxSurge: 2` `st86`)",
            "knob_changed": "ONLY `spec.strategy.rollingUpdate` changes (`{maxUnavailable: 1, maxSurge: 0}` in `st85` vs. `{maxUnavailable: 0, maxSurge: 2}` in `st86`) on identical `4`-replica `v1 -> v2` `templateHash` upgrade inside a tight `8`-TPU `ClusterQueue`.",
            "cross_check": "`st85` completes all `4` `v2` replacements (`8` total admissions = `4` retired `v1` + `4` active `v2`, `pending=0`), whereas `st86` deadlocks with `2` surge `v2` pods gated (`4` total admissions, `pending=2`, `Unadmitted=2`).",
        },
    ],
    "st86": [
        {
            "peer_id": "st85",
            "relationship": "Exact `Deployment` Rolling-Upgrade Strategy A/B Twin (Surge Deadlock `st86` vs. Clean `maxUnavailable: 1` Rollout `st85`)",
            "knob_changed": "`{maxUnavailable: 0, maxSurge: 2}` (`st86`) vs. `{maxUnavailable: 1, maxSurge: 0}` (`st85`).",
            "cross_check": "Demonstrates why Kueue-managed `Deployment`s in tight `ClusterQueue`s need `maxUnavailable >= 1` (or spare surge quota): `admitted_workloads_total` is `4` (`pending=2`) in `st86` vs. `8` (`pending=0`) in `st85`.",
        },
    ],
    "st87": [
        {
            "peer_id": "st88",
            "relationship": "Exact `Deployment` Pause/Resume A/B Twin (`Paused` at `t=+8s` + `Resumed` at `t=+24s` `st87` vs. Immediate Unpaused Upgrade `st88`)",
            "knob_changed": "Wrapping the `t=+11s` `1 -> 4` TPU/pod `Upgrade` in `Paused` (`t=+8s`) and `Resumed` (`t=+24s`) in `st87` vs. unpaused rollout at `t=+11s` in `st88`.",
            "cross_check": "Both admit `6` total replica `Workload`s (`3` `v1` + `3` `v2`), but deferring rollout by `13s` in `st87` keeps `Sec 5.1 AVG` (`~41%` vs. `~59%`) and `Sec 5.4 EFFECTIVE SAT` (`~36%–45%` vs. `~54%–63%`) strictly lower than `st88`.",
        },
    ],
    "st88": [
        {
            "peer_id": "st87",
            "relationship": "Exact `Deployment` Pause/Resume A/B Twin (Immediate Unpaused Upgrade `st88` vs. Paused/Resumed Rollout `st87`)",
            "knob_changed": "Immediate rollout at `t=+11s` (`st88`) vs. paused rollout until `t=+24s` (`st87`).",
            "cross_check": "`Sec 5.4 EFFECTIVE SAT` rises by `~16–20pp` in `st88` because `3 x 4` TPU `v2` pods run for `~29s` (`t=11..40s`) instead of `~16s` (`t=24..40s`).",
        },
    ],
    "st89": [
        {
            "peer_id": "st90",
            "relationship": "`t0` `Deployment` Partial Status & Preemption Healing (`st89`) vs. `t0` `Deployment` Deletion & Re-Creation Lifecycle (`st90`)",
            "knob_changed": "`st89` tests `t0` partial readiness (`4/6`) + `LowerPriority` preemption (`4` evictions) + `ReplicaSet` self-healing; `st90` tests graceful `t0` `Deployment` `Deleted` (`t=+8s`), incarnation-`1` same-name re-creation (`t=+12s`), and `canary-srv` creation/deletion (`0` evictions).",
            "cross_check": "`Sec 5.3` records **`4` preemptions (`Excluded=4`) in `st89`** vs. **`0` evictions (`Excluded=0`, `completed=6` cleanly retired serving replicas) in `st90`**, proving `saturation.py` separates preempted serving pods from cleanly deleted/retired serving pods.",
        },
    ],
    "st90": [
        {
            "peer_id": "st89",
            "relationship": "`t0` `Deployment` Deletion & Re-Creation (`st90`) vs. `t0` `Deployment` Preemption & Self-Healing (`st89`)",
            "knob_changed": "Explicit `Deleted` + `New` re-creation (`st90`, `0` evictions) vs. `LowerPriority` preemption by `urgent-eval` (`st89`, `4` evictions).",
            "cross_check": "`st90` retires `6` serving replicas cleanly (`completed=6, Excluded=0`) and ends with `6` `ephemeral-srv` inc-`1` replicas (`12/16` TPUs), whereas `st89` excludes `4` preempted serving replicas (`Excluded=4`) and heals back to `16/16` TPUs.",
        },
    ],
}
