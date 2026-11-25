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
"""Focused unit tests for scenario loader invariants and time-scaling accuracy."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

SCRIPTS_DIR = Path(__file__).resolve().parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from chamber.scenario import ExecutionTimeIndex, WorkloadExecutionInfo  # noqa: E402
from runner.loader import (  # noqa: E402
    ScheduledWorkflowItem,
    _AdmittedInitialCandidate,
    _InitialBuildOptions,
    _ParsedInfra,
    _allocate_k8s_name,
    _bootstrap_initial_deployment,
    _build_initial_infra_pods,
    _build_kwok_pause_pod_manifest,
    _build_kwok_pause_pod_spec,
    _build_t0_resubmit_item,
    _compute_initial_age_sec,
    _extract_pod_resource_requests,
    _match_initial_pods_to_candidates,
    _materialize_initial_workload_chunks,
    _partition_admitted_candidates,
    _resolve_candidate_priority,
    _synthesize_missing_t0_pods,
    load_scenario_bundle,
)
from runner.validation import _prepare_kueue_or_ns_doc  # noqa: E402

_T0 = datetime(2026, 3, 1, 0, 0, 0, tzinfo=timezone.utc)


def _ts(offset_sec: float) -> str:
    dt = _T0 + timedelta(seconds=offset_sec)
    if dt.microsecond:
        return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _ts_sec(offset_sec: float) -> str:
    dt = _T0 + timedelta(seconds=int(offset_sec))
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _node(name: str, cpu: str = "8", mem: str = "32Gi", offset: float = 0.0) -> dict[str, Any]:
    return {
        "timestamp": _ts(offset),
        "eventType": "New",
        "payload": {
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": name, "labels": {"kubernetes.io/hostname": name}},
            "status": {
                "capacity": {"cpu": cpu, "memory": mem, "pods": "110"},
                "allocatable": {"cpu": cpu, "memory": mem, "pods": "110"},
                "conditions": [{"type": "Ready", "status": "True"}],
            },
        },
    }


def _base_infra(
    nodes: tuple[str, ...] = ("n1", "n2"),
    cq_extra_spec: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    cq_spec: dict[str, Any] = {
        "resourceGroups": [
            {
                "coveredResources": ["cpu", "memory"],
                "flavors": [
                    {
                        "name": "default-flavor",
                        "resources": [
                            {"name": "cpu", "nominalQuota": "100"},
                            {"name": "memory", "nominalQuota": "400Gi"},
                        ],
                    }
                ],
            }
        ]
    }
    if cq_extra_spec:
        cq_spec.update(cq_extra_spec)
    return (
        [_node(n) for n in nodes]
        + [
            {
                "timestamp": _ts(0),
                "eventType": "New",
                "payload": {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "team"}},
            },
            {
                "timestamp": _ts(0),
                "eventType": "New",
                "payload": {
                    "apiVersion": "kueue.x-k8s.io/v1beta2",
                    "kind": "ResourceFlavor",
                    "metadata": {"name": "default-flavor"},
                    "spec": {},
                },
            },
            {
                "timestamp": _ts(0),
                "eventType": "New",
                "payload": {
                    "apiVersion": "kueue.x-k8s.io/v1beta2",
                    "kind": "ClusterQueue",
                    "metadata": {"name": "cq"},
                    "spec": cq_spec,
                },
            },
            {
                "timestamp": _ts(0),
                "eventType": "New",
                "payload": {
                    "apiVersion": "kueue.x-k8s.io/v1beta2",
                    "kind": "LocalQueue",
                    "metadata": {"name": "lq", "namespace": "team"},
                    "spec": {"clusterQueue": "cq"},
                },
            },
        ]
    )


def _job_obj(
    name: str,
    *,
    ns: str = "team",
    lq: str = "lq",
    suspend: bool = True,
    cpu: str = "1",
    mem: str = "1Gi",
    parallelism: int = 1,
    creation_off: float = 0.0,
) -> dict[str, Any]:
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": name,
            "namespace": ns,
            "creationTimestamp": _ts_sec(creation_off),
            "labels": {"kueue.x-k8s.io/queue-name": lq},
        },
        "spec": {
            "suspend": suspend,
            "parallelism": parallelism,
            "completions": parallelism,
            "template": {
                "metadata": {},
                "spec": {
                    "restartPolicy": "Never",
                    "containers": [
                        {
                            "name": "c",
                            "image": "registry.k8s.io/pause:3.10",
                            "resources": {"requests": {"cpu": cpu, "memory": mem}},
                        }
                    ],
                },
            },
        },
    }


def _wf_ev(
    obj: dict[str, Any],
    offset: float,
    *,
    ev_type: str = "New",
    state: str = "NotAdmitted",
    lq: str = "lq",
    creation_off: float | None = None,
) -> dict[str, Any]:
    meta = obj["metadata"]
    pl: dict[str, Any] = {
        "objectKind": obj["kind"],
        "name": meta["name"],
        "namespace": meta["namespace"],
        "localQueueName": lq,
        "object": obj,
    }
    if creation_off is not None:
        pl["creationTime"] = _ts_sec(creation_off)
    return {
        "timestamp": _ts(offset),
        "eventType": ev_type,
        "workloadState": state,
        "payload": pl,
    }


def _exec_ev(
    ev_type: str,
    wl_name: str,
    owner_kind: str,
    owner_name: str,
    offset: float,
    admit_off: float,
    *,
    end_off: float | None = None,
    duration_sec: int | float | None = None,
    duration_str: str | None = None,
    ns: str = "team",
    lq: str = "lq",
    cq: str = "cq",
    preempted: bool | None = None,
    interruption_reason: str | None = None,
) -> dict[str, Any]:
    pl: dict[str, Any] = {
        "workloadName": wl_name,
        "namespace": ns,
        "ownerKind": owner_kind,
        "ownerName": owner_name,
        "localQueueName": lq,
        "clusterQueueName": cq,
        "admittedTime": _ts_sec(admit_off),
    }
    if end_off is not None:
        pl["endTime"] = _ts_sec(end_off)
    if duration_sec is not None:
        pl["executionDurationSeconds"] = duration_sec
    elif end_off is not None:
        pl["executionDurationSeconds"] = int(end_off) - int(admit_off)
    if duration_str is not None:
        pl["executionDuration"] = duration_str
    if preempted is not None:
        pl["preempted"] = preempted
    if interruption_reason is not None:
        pl["interruptionReason"] = interruption_reason
    return {
        "timestamp": _ts(offset),
        "eventType": ev_type,
        "workloadState": "Admitted",
        "payload": pl,
    }


def _infra_pod(
    name: str,
    *,
    ns: str = "kube-system",
    node_name: str = "n1",
    offset: float = 0.0,
    ev_type: str = "New",
    sched_off: float | None = None,
    del_off: float | None = None,
    reqs: dict[str, str] | None = None,
    workload_name: str = "",
    owner_kind: str = "",
    owner_name: str = "",
) -> dict[str, Any]:
    pl: dict[str, Any] = {
        "apiVersion": "v1",
        "kind": "Pod",
        "name": name,
        "namespace": ns,
        "placement": node_name,
        "resourceRequest": {"cpu": "1", "memory": "1Gi"} if reqs is None else dict(reqs),
        "schedulingTime": _ts_sec(offset if sched_off is None else sched_off),
        "firstNoticeTime": _ts_sec(offset if sched_off is None else sched_off),
    }
    if del_off is not None:
        pl["deletionTime"] = _ts_sec(del_off)
    if workload_name:
        pl["workloadName"] = workload_name
    if owner_kind:
        pl["ownerKind"] = owner_kind
    if owner_name:
        pl["ownerName"] = owner_name
    return {"timestamp": _ts(offset), "eventType": ev_type, "payload": pl}


def _pg_pod(
    name: str,
    *,
    group: str = "pg1",
    total: int = 2,
    gated: bool = True,
    node_name: str | None = None,
    creation_off: float = 10.0,
) -> dict[str, Any]:
    spec: dict[str, Any] = {
        "containers": [
            {
                "name": "c",
                "image": "registry.k8s.io/pause:3.10",
                "resources": {"requests": {"cpu": "2", "memory": "2Gi"}},
            }
        ],
        "restartPolicy": "Never",
    }
    if gated:
        spec["schedulingGates"] = [{"name": "kueue.x-k8s.io/admission"}]
    if node_name:
        spec["nodeName"] = node_name
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": name,
            "namespace": "team",
            "creationTimestamp": _ts_sec(creation_off),
            "labels": {"kueue.x-k8s.io/queue-name": "lq", "kueue.x-k8s.io/managed": "true"},
            "annotations": {
                "kueue.x-k8s.io/pod-group-name": group,
                "kueue.x-k8s.io/pod-group-total-count": str(total),
            },
        },
        "spec": spec,
    }


def _load_bundle(
    infra: list[dict[str, Any]],
    wfs: list[dict[str, Any]],
    execs: list[dict[str, Any]],
    **kwargs: Any,
):
    with tempfile.TemporaryDirectory() as td:
        sc = Path(td)
        (sc / "infra.json").write_text("\n".join(json.dumps(x) for x in infra) + "\n", encoding="utf-8")
        (sc / "workflows.json").write_text("\n".join(json.dumps(x) for x in wfs) + "\n", encoding="utf-8")
        (sc / "execution-time.json").write_text("\n".join(json.dumps(x) for x in execs) + "\n", encoding="utf-8")
        return load_scenario_bundle(sc, **kwargs)


class LoaderCoreBugTests(unittest.TestCase):
    """Regression tests for core scenario loader bugs (L1-L6) and --speedup time scaling."""

    def test_l1_strips_v1beta2_admission_checks_strategy(self) -> None:
        """L1: ClusterQueue.spec.admissionChecksStrategy is stripped so Kueue never sees AdmissionCheckNotFound."""
        raw_cq = {
            "apiVersion": "kueue.x-k8s.io/v1beta2",
            "kind": "ClusterQueue",
            "metadata": {"name": "cq-dws"},
            "spec": {
                "admissionChecksStrategy": {
                    "admissionChecks": [
                        {"name": "dws-prov", "onFlavors": ["default-flavor"]},
                        {"name": "multikueue-check"},
                    ]
                }
            },
        }
        doc = _prepare_kueue_or_ns_doc(raw_cq)
        self.assertNotIn("admissionChecksStrategy", doc["spec"])
        self.assertEqual(
            doc["metadata"]["annotations"]["kueue-optimizer.gke.io/stripped-admission-checks"],
            "dws-prov,multikueue-check",
        )

        bundle = _load_bundle(
            _base_infra(
                cq_extra_spec={
                    "admissionChecksStrategy": {
                        "admissionChecks": [{"name": "dws-prov", "onFlavors": ["default-flavor"]}]
                    }
                }
            ),
            [_wf_ev(_job_obj("j1"), 5.0)],
            [],
        )
        cq_doc = next(d for d in bundle.prereq_and_queue_docs if d.get("kind") == "ClusterQueue")
        self.assertNotIn("admissionChecksStrategy", cq_doc["spec"])
        self.assertEqual(
            cq_doc["metadata"]["annotations"]["kueue-optimizer.gke.io/stripped-admission-checks"],
            "dws-prov",
        )

    def test_l2_best_effort_pods_do_not_invent_cpu_or_memory_requests(self) -> None:
        """L2: Pods with empty or ephemeral-storage-only resourceRequest stay BestEffort ({}) instead of 1 CPU / 1Gi."""
        infra = _base_infra(nodes=("n1",)) + [
            _infra_pod("be-init-0", ns="kube-system", node_name="n1", sched_off=-100, reqs={}),
            _infra_pod(
                "ephemeral-only",
                ns="kube-system",
                node_name="n1",
                sched_off=-100,
                reqs={"ephemeral-storage": "1Gi"},
            ),
            _infra_pod("be-post-t0", ns="kube-system", node_name="n1", offset=30.0, reqs={}),
            _infra_pod(
                "be-kueue-0",
                ns="team",
                node_name="n1",
                sched_off=-50,
                reqs={},
                workload_name="job-be-t0-x",
                owner_kind="Job",
                owner_name="be-t0",
            ),
        ]
        wfs = [
            _wf_ev(_job_obj("be-t0", suspend=False, creation_off=-50), 0.0, state="Admitted", creation_off=-50),
            _wf_ev(_job_obj("j1"), 5.0),
        ]
        execs = [_exec_ev("Admitted", "job-be-t0-x", "Job", "be-t0", 0.0, -50)]
        bundle = _load_bundle(infra, wfs, execs)

        self.assertEqual(len(bundle.initial_pods), 2)
        for ip in bundle.initial_pods:
            self.assertEqual(ip.resource_request, {})
            self.assertEqual(ip.manifest["spec"]["containers"][0]["resources"]["requests"], {})
            self.assertEqual(ip.manifest["spec"]["containers"][0]["resources"]["limits"], {})

        post_pods = [ie for ie in bundle.scheduled_infra_events if ie.object_kind == "Pod"]
        self.assertEqual(len(post_pods), 1)
        self.assertEqual(post_pods[0].manifest["spec"]["containers"][0]["resources"]["requests"], {})

        self.assertEqual(len(bundle.initial_workloads), 1)
        iwl = bundle.initial_workloads[0]
        self.assertEqual(iwl.pods[0].resource_request, {})
        self.assertEqual(iwl.pods[0].manifest["spec"]["containers"][0]["resources"]["requests"], {})
        self.assertEqual(iwl.workload_status["admission"]["podSetAssignments"][0]["resourceUsage"], {})

    def test_l3_first_arrival_podgroup_is_not_replayed_twice(self) -> None:
        """L3: Plain-Pod group first seen NotAdmitted and admitted within initial_window_sec is not also pre-admitted."""
        for label, first_off, admit_off in (
            ("post_t0_first_arrival", 10.4, 10.6),
            ("queued_at_t0_admitted_soon", 0.0, 0.4),
        ):
            with self.subTest(label=label):
                wfs = [
                    _wf_ev(_pg_pod("pg1-a", creation_off=first_off), first_off, state="NotAdmitted", creation_off=first_off),
                    _wf_ev(
                        _pg_pod("pg1-b", creation_off=first_off),
                        first_off + 0.01,
                        state="NotAdmitted",
                        creation_off=first_off,
                    ),
                    _wf_ev(
                        _pg_pod("pg1-a", gated=False, node_name="n1", creation_off=first_off),
                        admit_off,
                        ev_type="Update",
                        state="Admitted",
                        creation_off=first_off,
                    ),
                    _wf_ev(
                        _pg_pod("pg1-b", gated=False, node_name="n2", creation_off=first_off),
                        admit_off + 0.01,
                        ev_type="Update",
                        state="Admitted",
                        creation_off=first_off,
                    ),
                    _wf_ev(_job_obj("later"), 60.0),
                ]
                execs = [
                    _exec_ev("Admitted", "pg1", "PodGroup", "pg1", admit_off, admit_off),
                    _exec_ev("FullExecution", "pg1", "PodGroup", "pg1", 130.0, admit_off, end_off=130.0),
                ]
                infra = _base_infra() + [
                    _infra_pod(
                        "pg1-a",
                        ns="team",
                        node_name="n1",
                        offset=admit_off + 0.2,
                        workload_name="pg1",
                        owner_kind="PodGroup",
                        owner_name="pg1",
                        reqs={"cpu": "2", "memory": "2Gi"},
                    ),
                    _infra_pod(
                        "pg1-b",
                        ns="team",
                        node_name="n2",
                        offset=admit_off + 0.2,
                        workload_name="pg1",
                        owner_kind="PodGroup",
                        owner_name="pg1",
                        reqs={"cpu": "2", "memory": "2Gi"},
                    ),
                ]
                bundle = _load_bundle(infra, wfs, execs)
                sched_pg_pods = [w for w in bundle.scheduled_workflows if w.name.startswith("pg1-")]
                self.assertEqual(len(sched_pg_pods), 2)
                self.assertEqual(len(bundle.initial_workloads), 0)

    def test_l4_reused_pod_names_preserve_all_incarnations(self) -> None:
        """L4: StatefulSet-style pod name reuse retains both initial and recreated post-t0 incarnations."""
        infra = _base_infra() + [
            _infra_pod("web-0", ns="svc", node_name="n1", offset=0.0, sched_off=-100, reqs={"cpu": "4", "memory": "4Gi"}),
            _infra_pod(
                "web-0",
                ns="svc",
                node_name="n1",
                offset=20.0,
                ev_type="Deleted",
                sched_off=-100,
                del_off=20.0,
                reqs={"cpu": "4", "memory": "4Gi"},
            ),
            _infra_pod("web-0", ns="svc", node_name="n2", offset=30.0, ev_type="New", reqs={"cpu": "4", "memory": "4Gi"}),
            _infra_pod(
                "web-0",
                ns="svc",
                node_name="n2",
                offset=300.0,
                ev_type="Deleted",
                sched_off=30.0,
                del_off=300.0,
                reqs={"cpu": "4", "memory": "4Gi"},
            ),
            _infra_pod("web-0", ns="svc", node_name="n1", offset=310.0, ev_type="New", reqs={"cpu": "4", "memory": "4Gi"}),
            _infra_pod(
                "web-0",
                ns="svc",
                node_name="n1",
                offset=400.0,
                ev_type="Deleted",
                sched_off=310.0,
                del_off=400.0,
                reqs={"cpu": "4", "memory": "4Gi"},
            ),
            _infra_pod("cron-x", ns="svc", node_name="n1", offset=50.0, ev_type="New"),
            _infra_pod("cron-x", ns="svc", node_name="n1", offset=60.0, ev_type="Deleted", sched_off=50.0, del_off=60.0),
            _infra_pod("cron-x", ns="svc", node_name="n1", offset=110.0, ev_type="New"),
            _infra_pod("cron-x", ns="svc", node_name="n1", offset=120.0, ev_type="Deleted", sched_off=110.0, del_off=120.0),
        ]
        bundle = _load_bundle(infra, [_wf_ev(_job_obj("j1"), 0.0, creation_off=-5)], [])

        init_names = [p.manifest["metadata"]["name"] for p in bundle.initial_pods]
        self.assertEqual(init_names, ["web-0"])
        self.assertAlmostEqual(bundle.initial_pods[0].remaining_duration_sec, 20.0, places=3)

        post_pods = [ie for ie in bundle.scheduled_infra_events if ie.object_kind == "Pod"]
        self.assertEqual(len(post_pods), 4)
        post_offsets = [round(ie.raw_offset_from_t0_sec, 1) for ie in post_pods]
        self.assertEqual(post_offsets, [30.0, 50.0, 110.0, 310.0])
        # Recreated web-0 incarnations get clean sequential suffixes (web-0, web-0-1, web-0-2), not web-0-1-2.
        web_k8s_names = init_names + [
            ie.manifest["metadata"]["name"]
            for ie in post_pods
            if ie.manifest["metadata"]["annotations"]["kueue-optimizer.gke.io/original-name"] == "web-0"
        ]
        self.assertEqual(web_k8s_names, ["web-0", "web-0-1", "web-0-2"])
        all_k8s_pod_names = init_names + [ie.manifest["metadata"]["name"] for ie in post_pods]
        self.assertEqual(len(all_k8s_pod_names), len(set(all_k8s_pod_names)))
        # Recreated web-0 has duration 300 - 30 = 270s on n2.
        self.assertEqual(post_pods[0].manifest["spec"]["nodeName"], "n2")
        self.assertEqual(
            post_pods[0].manifest["metadata"]["annotations"]["kueue-optimizer.gke.io/pod-execution-time-seconds"],
            "270",
        )

    def test_l5_post_t0_fast_admitted_then_preempted_scheduled_at_arrival_time(self) -> None:
        """L5: Post-t0 workflow first seen Admitted and later preempted is scheduled at arrival (+30s), not preemption (+50s)."""
        first = _wf_ev(_job_obj("first"), 5.0, creation_off=5.0)
        victim_new = _wf_ev(_job_obj("victim", suspend=False, creation_off=30.0), 30.3, state="Admitted", creation_off=30.0)
        victim_preempt = _wf_ev(
            _job_obj("victim", suspend=True, creation_off=30.0),
            50.4,
            ev_type="Update",
            state="NotAdmitted",
            creation_off=30.0,
        )
        victim_readmit = _wf_ev(
            _job_obj("victim", suspend=False, creation_off=30.0),
            90.4,
            ev_type="Update",
            state="Admitted",
            creation_off=30.0,
        )
        execs = [
            _exec_ev("Admitted", "job-victim-abcde", "Job", "victim", 30.3, 30.0),
            _exec_ev(
                "PartialExecution",
                "job-victim-abcde",
                "Job",
                "victim",
                50.4,
                30.0,
                end_off=50.0,
                preempted=True,
                interruption_reason="Preempted",
            ),
            _exec_ev("Admitted", "job-victim-abcde", "Job", "victim", 90.4, 90.0),
            _exec_ev("FullExecution", "job-victim-abcde", "Job", "victim", 190.4, 90.0, end_off=190.0),
        ]

        for label, wfs in (
            ("with_update_not_admitted", [first, victim_new, victim_preempt, victim_readmit]),
            ("exec_time_preemption_only", [first, victim_new]),
        ):
            with self.subTest(label=label):
                bundle = _load_bundle(_base_infra(), wfs, execs)
                victims = [w for w in bundle.scheduled_workflows if w.name == "victim"]
                self.assertEqual(len(victims), 1)
                self.assertAlmostEqual(victims[0].raw_offset_from_t0_sec, 30.0, places=3)
                self.assertAlmostEqual(victims[0].raw_offset_sec, 25.0, places=3)
                self.assertEqual(victims[0].trigger_reason, "post_t0_fast_admitted")
                self.assertAlmostEqual(victims[0].execution_duration_sec, 100.0, places=3)

    def test_l6_zero_second_full_execution_is_not_treated_as_missing(self) -> None:
        """L6: FullExecution with 0s duration (same-second finish) is indexed rather than imputed to peer median."""
        wfs = [_wf_ev(_job_obj(f"long-{i}"), 5.0 + i) for i in range(3)] + [_wf_ev(_job_obj("quick"), 20.0)]
        execs: list[dict[str, Any]] = []
        for i in range(3):
            execs.append(_exec_ev("Admitted", f"job-long-{i}-x", "Job", f"long-{i}", 6.0 + i, 6.0 + i))
            execs.append(
                _exec_ev("FullExecution", f"job-long-{i}-x", "Job", f"long-{i}", 3606.0 + i, 6.0 + i, end_off=3606.0 + i)
            )
        execs.append(_exec_ev("Admitted", "job-quick-x", "Job", "quick", 20.2, 20.2))
        execs.append(_exec_ev("FullExecution", "job-quick-x", "Job", "quick", 20.9, 20.2, end_off=20.9, duration_sec=0))

        for policy in ("impute", "fallback", "error"):
            with self.subTest(policy=policy):
                bundle = _load_bundle(_base_infra(), wfs, execs, missing_duration_policy=policy, min_pod_duration_sec=0.5)
                quick = next(w for w in bundle.scheduled_workflows if w.name == "quick")
                self.assertTrue(quick.indexed_from_execution_time)
                self.assertEqual(quick.execution_duration_sec, 0.0)
                self.assertAlmostEqual(quick.compressed_duration_sec, 0.5, places=6)
                self.assertEqual(
                    quick.manifest["metadata"]["annotations"]["kueue-optimizer.gke.io/pod-execution-time-seconds"],
                    "0.5",
                )
                self.assertEqual(bundle.missing_duration_warnings, [])

        # Also check executionDuration="0s" string fallback and non-interrupted classification after PartialExecution.
        execs_str = [
            _exec_ev("Admitted", "job-quick-x", "Job", "quick", 20.0, 20.0),
            _exec_ev("PartialExecution", "job-quick-x", "Job", "quick", 25.0, 20.0, end_off=25.0, preempted=True),
            _exec_ev("Admitted", "job-quick-x", "Job", "quick", 30.1, 30.1),
            {
                "timestamp": _ts(30.8),
                "eventType": "FullExecution",
                "workloadState": "Admitted",
                "payload": {
                    "workloadName": "job-quick-x",
                    "namespace": "team",
                    "ownerKind": "Job",
                    "ownerName": "quick",
                    "localQueueName": "lq",
                    "clusterQueueName": "cq",
                    "admittedTime": _ts_sec(30.1),
                    "endTime": _ts_sec(30.8),
                    "executionDuration": "0s",
                },
            },
        ]
        bundle_str = _load_bundle(_base_infra(), [_wf_ev(_job_obj("quick"), 20.0)], execs_str, missing_duration_policy="error")
        wl_info = bundle_str.exec_index.lookup("team", "Job", "quick")
        self.assertIsNotNone(wl_info)
        assert wl_info is not None
        self.assertFalse(wl_info.is_interrupted_only)
        self.assertEqual(wl_info.full_duration_sec, 0.0)

    def test_speedup_scales_durations_and_arrival_offsets_by_division(self) -> None:
        """Guardrail: --speedup must divide both execution durations and wall offsets (catches * speedup / no-op bugs)."""
        infra = _base_infra() + [
            _infra_pod("late-sys", ns="kube-system", node_name="n1", offset=60.0, ev_type="New"),
            _infra_pod("late-sys", ns="kube-system", node_name="n1", offset=260.0, ev_type="Deleted", sched_off=60.0, del_off=260.0),
        ]
        wfs = [
            _wf_ev(_job_obj("j1"), 10.0),
            _wf_ev(_job_obj("j2"), 110.0),
        ]
        execs = [
            _exec_ev("Admitted", "job-j1-x", "Job", "j1", 10.0, 10.0),
            _exec_ev("FullExecution", "job-j1-x", "Job", "j1", 310.0, 10.0, end_off=310.0),
            _exec_ev("Admitted", "job-j2-x", "Job", "j2", 110.0, 110.0),
            _exec_ev("FullExecution", "job-j2-x", "Job", "j2", 610.0, 110.0, end_off=610.0),
        ]
        bundle = _load_bundle(infra, wfs, execs, speedup=10.0, min_pod_duration_sec=0.1)

        by_name = {w.name: w for w in bundle.scheduled_workflows}
        # Effective start jumps to j1 (+10s), so j1 offset = 0s, j2 raw offset = 100s -> wall offset = 10s.
        self.assertAlmostEqual(bundle.initial_jump_raw_sec, 10.0, places=6)
        self.assertAlmostEqual(bundle.initial_jump_wall_sec, 1.0, places=6)
        self.assertAlmostEqual(by_name["j1"].target_wall_offset_sec, 0.0, places=6)
        self.assertAlmostEqual(by_name["j2"].raw_offset_sec, 100.0, places=6)
        self.assertAlmostEqual(by_name["j2"].target_wall_offset_sec, 10.0, places=6)
        # Durations: j1=300s -> 30s; j2=500s -> 50s.
        self.assertAlmostEqual(by_name["j1"].compressed_duration_sec, 30.0, places=6)
        self.assertAlmostEqual(by_name["j2"].compressed_duration_sec, 50.0, places=6)
        self.assertEqual(
            by_name["j1"].manifest["metadata"]["annotations"]["kueue-optimizer.gke.io/pod-execution-time-seconds"],
            "30",
        )
        self.assertEqual(
            by_name["j2"].manifest["metadata"]["annotations"]["kueue-optimizer.gke.io/pod-execution-time-seconds"],
            "50",
        )
        # Post-t0 infra pod arrives at +60s (+50s after jump -> 5s wall), runs 200s -> 20s compressed.
        post_pod = next(ie for ie in bundle.scheduled_infra_events if ie.object_kind == "Pod")
        self.assertAlmostEqual(post_pod.target_wall_offset_sec, 5.0, places=6)
        self.assertEqual(
            post_pod.manifest["metadata"]["annotations"]["kueue-optimizer.gke.io/pod-execution-time-seconds"],
            "20",
        )


class LoaderEdgeCaseBugTests(unittest.TestCase):
    """Regression tests for scenario loader edge-case bugs (L7-L12)."""

    def test_l7_explicit_linkage_beats_name_prefix_heuristic(self) -> None:
        """L7: Queued Job 'train' must not drop unrelated 'train-*' infra pods or linked 'train-2' pods."""
        infra = _base_infra() + [
            _infra_pod(
                "train-dashboard-7c9f-abcde",
                ns="team",
                node_name="n1",
                offset=0.0,
                sched_off=-500,
                reqs={"cpu": "4", "memory": "8Gi"},
            ),
            _infra_pod("other-svc-1", ns="team", node_name="n1", offset=0.0, sched_off=-500),
            _infra_pod(
                "train-2-xk2lp",
                ns="team",
                node_name="n2",
                offset=0.0,
                sched_off=-300,
                reqs={"cpu": "3", "memory": "1Gi"},
                workload_name="job-train-2-aaaaa",
                owner_kind="Job",
                owner_name="train-2",
            ),
            _infra_pod(
                "train-metrics-exporter-0",
                ns="team",
                node_name="n1",
                offset=40.0,
                reqs={"cpu": "2", "memory": "2Gi"},
            ),
            _infra_pod("other-svc-2", ns="team", node_name="n1", offset=41.0),
        ]
        wfs = [
            _wf_ev(_job_obj("train-2", suspend=False, cpu="3", creation_off=-300), 0.0, state="Admitted", creation_off=-300),
            _wf_ev(_job_obj("train", creation_off=-10), 0.0, state="NotAdmitted", creation_off=-10),
        ]
        execs = [_exec_ev("Admitted", "job-train-2-aaaaa", "Job", "train-2", 0.0, -300)]

        bundle = _load_bundle(infra, wfs, execs)

        # Running Job 'train-2' must keep its real bound pod on n2 (not synthesize a fake pod on n1).
        self.assertEqual(len(bundle.initial_workloads), 1)
        iwl = bundle.initial_workloads[0]
        self.assertEqual([(p.name, p.placement) for p in iwl.pods], [("train-2-xk2lp", "n2")])

        # Unrelated t0 and post-t0 pods sharing the 'train-' prefix must be preserved as infra pods.
        self.assertEqual(
            sorted(p.name for p in bundle.initial_pods),
            ["other-svc-1", "train-dashboard-7c9f-abcde"],
        )
        post_pod_names = sorted(
            ie.manifest["metadata"]["annotations"]["kueue-optimizer.gke.io/original-name"]
            for ie in bundle.scheduled_infra_events
            if ie.object_kind == "Pod"
        )
        self.assertEqual(post_pod_names, ["other-svc-2", "train-metrics-exporter-0"])

    def test_l8_tier4_partial_t0_binding_does_not_double_count_on_request_mismatch(self) -> None:
        """L8: Tier-4 synthesis counts unmatched linked pods when template requests differ from scraped LimitRange requests."""
        job_no_reqs = _job_obj("big", parallelism=4, suspend=False, creation_off=-200)
        job_no_reqs["spec"]["template"]["spec"]["containers"][0].pop("resources", None)
        infra = _base_infra() + [
            _infra_pod(
                f"big-{i}",
                ns="team",
                node_name="n2",
                sched_off=-200,
                reqs={"cpu": "100m", "memory": "128Mi"},
                workload_name="job-big-aaaaa",
                owner_kind="Job",
                owner_name="big",
            )
            for i in range(3)
        ]
        wfs = [
            _wf_ev(job_no_reqs, 0.0, state="Admitted", creation_off=-200),
            _wf_ev(_job_obj("q"), 12.5),
        ]
        execs = [_exec_ev("Admitted", "job-big-aaaaa", "Job", "big", 0.0, -200)]

        bundle = _load_bundle(infra, wfs, execs)
        self.assertEqual(len(bundle.initial_workloads), 1)
        iwl = bundle.initial_workloads[0]
        self.assertEqual(len(iwl.pods), 4)
        self.assertEqual(sum(ps["count"] for ps in iwl.workload_manifest["spec"]["podSets"]), 4)
        for p in iwl.pods:
            self.assertEqual(p.resource_request, {"cpu": "100m", "memory": "128Mi"})

    def test_l9_failed_t0_job_is_not_resubmitted_after_initial_jump(self) -> None:
        """L9: A t0-running Job that ended with PartialExecution reason=Failed is not resubmitted as preempted."""
        infra = _base_infra() + [
            _infra_pod(
                "flaky-abc",
                ns="team",
                node_name="n1",
                sched_off=-100,
                workload_name="job-flaky-x",
                owner_kind="Job",
                owner_name="flaky",
            ),
            _infra_pod(
                "flaky-abc",
                ns="team",
                node_name="n1",
                offset=20.2,
                ev_type="Deleted",
                sched_off=-100,
                del_off=20.0,
                workload_name="job-flaky-x",
                owner_kind="Job",
                owner_name="flaky",
            ),
        ]
        wfs = [
            _wf_ev(_job_obj("flaky", suspend=False, creation_off=-100), 0.0, state="Admitted", creation_off=-100),
            _wf_ev(_job_obj("queued"), 100.3),
        ]
        execs = [
            _exec_ev("Admitted", "job-flaky-x", "Job", "flaky", 0.0, -100),
            _exec_ev(
                "PartialExecution",
                "job-flaky-x",
                "Job",
                "flaky",
                20.2,
                -100,
                end_off=20.0,
                preempted=False,
                interruption_reason="Failed",
            ),
        ]

        bundle = _load_bundle(infra, wfs, execs)
        self.assertEqual([w.name for w in bundle.scheduled_workflows], ["queued"])
        self.assertAlmostEqual(bundle.initial_jump_raw_sec, 100.3, places=3)
        self.assertEqual(len(bundle.initial_workloads), 0)

    def test_l10_accepts_dns_subdomain_node_and_long_pod_names(self) -> None:
        """L10: Dotted FQDN Node/Pod/Queue names and >63-char RFC 1123 subdomain names load cleanly; hostname label stays <=63."""
        long_pod = "kube-proxy-gke-kueue-optimizer-prod-tpu-v6e-pool-1a2b3c4d-x9zq-extra"
        fqdn_node = "ip-10-0-1-23.us-west-2.compute.internal"
        long_node = "gke-prod-tpu-cluster-pool-very-long-node-name-0123456789abcdef-extra"
        dotted_pod = "etcd-ip-10-0-1-23.ec2.internal"

        long_node_ev = _node(long_node)
        long_node_ev["payload"]["metadata"]["labels"] = {}
        infra = (
            [_node(fqdn_node), long_node_ev]
            + _base_infra(nodes=())
            + [
                {
                    "timestamp": _ts(0),
                    "eventType": "New",
                    "payload": {
                        "apiVersion": "kueue.x-k8s.io/v1beta2",
                        "kind": "LocalQueue",
                        "metadata": {"name": "team.lq", "namespace": "team"},
                        "spec": {"clusterQueue": "cq"},
                    },
                },
                _infra_pod(long_pod, ns="kube-system", node_name=fqdn_node, sched_off=-100),
                _infra_pod(dotted_pod, ns="kube-system", node_name=fqdn_node, sched_off=-100),
            ]
        )
        bundle = _load_bundle(infra, [_wf_ev(_job_obj("j1", lq="team.lq"), 5.0, lq="team.lq")], [])

        self.assertEqual([n["metadata"]["name"] for n in bundle.nodes], [fqdn_node, long_node])
        long_node_doc = next(n for n in bundle.nodes if n["metadata"]["name"] == long_node)
        self.assertLessEqual(len(long_node_doc["metadata"]["labels"]["kubernetes.io/hostname"]), 63)
        orig_names = sorted(
            p.manifest["metadata"]["annotations"]["kueue-optimizer.gke.io/original-name"]
            for p in bundle.initial_pods
        )
        self.assertEqual(orig_names, sorted([long_pod, dotted_pod]))
        for p in bundle.initial_pods:
            self.assertEqual(p.placement, fqdn_node)
            self.assertEqual(p.name, p.manifest["metadata"]["name"])
            self.assertLessEqual(len(p.name), 63)

        # Trailing hyphen remains invalid.
        bad_infra = _base_infra() + [_infra_pod(long_pod[:63], ns="kube-system", node_name="n1", sched_off=-10)]
        with self.assertRaisesRegex(ValueError, "RFC 1123 DNS subdomain"):
            _load_bundle(bad_infra, [_wf_ev(_job_obj("j1"), 5.0)], [])

    def test_l11_imputation_stamps_standalone_pods_and_uses_full_queue_median(self) -> None:
        """L11: Imputed duration is stamped onto standalone Pods and queue fallback computes median across all queue samples."""
        solo_pod = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "solo",
                "namespace": "team",
                "labels": {"kueue.x-k8s.io/queue-name": "lq"},
            },
            "spec": {
                "containers": [{"name": "c", "resources": {"requests": {"cpu": "1", "memory": "1Gi"}}}],
                "schedulingGates": [{"name": "kueue.x-k8s.io/admission"}],
            },
        }
        wfs = [_wf_ev(_job_obj("peer"), 5.0), _wf_ev(solo_pod, 9.0), _wf_ev(_job_obj("never"), 9.5)]
        execs = [
            _exec_ev("Admitted", "job-peer-x", "Job", "peer", 5.5, 5.5),
            _exec_ev("FullExecution", "job-peer-x", "Job", "peer", 3605.5, 5.5, end_off=3605.5),
        ]
        bundle = _load_bundle(_base_infra(), wfs, execs)
        solo = next(w for w in bundle.scheduled_workflows if w.name == "solo")
        self.assertAlmostEqual(solo.execution_duration_sec, 3600.0, places=3)
        self.assertEqual(
            solo.manifest["metadata"]["annotations"]["kueue-optimizer.gke.io/pod-execution-time-seconds"],
            "3600",
        )

        # Queue fallback with 3 unscheduled workloads [100, 1000, 5000] must impute median 1000 (not first sample 100).
        execs_q: list[dict[str, Any]] = []
        for idx, dur in enumerate((100, 1000, 5000)):
            execs_q.append(_exec_ev("Admitted", f"wl-{idx}", "Job", f"old-{idx}", 1.0 + idx, 1.0 + idx))
            execs_q.append(
                _exec_ev("FullExecution", f"wl-{idx}", "Job", f"old-{idx}", 1.0 + idx + dur, 1.0 + idx, end_off=1.0 + idx + dur)
            )
        bundle_q = _load_bundle(_base_infra(), [_wf_ev(_job_obj("never", parallelism=3), 9.5)], execs_q)
        self.assertAlmostEqual(bundle_q.scheduled_workflows[0].execution_duration_sec, 1000.0, places=3)

    def test_l12_all_fast_admitted_trace_settles_initial_jump(self) -> None:
        """L12: A trace where every workflow is first seen Admitted settles cleanly with skip_initial_offset=True."""
        wfs: list[dict[str, Any]] = []
        execs: list[dict[str, Any]] = []
        for i in range(5):
            off = 5.0 + 20.0 * i
            wfs.append(
                _wf_ev(
                    _job_obj(f"fast-{i}", suspend=False, creation_off=off),
                    off + 0.3,
                    state="Admitted",
                    creation_off=off,
                )
            )
            execs.append(_exec_ev("Admitted", f"job-fast-{i}-abcde", "Job", f"fast-{i}", off + 0.3, off))
            execs.append(
                _exec_ev(
                    "FullExecution",
                    f"job-fast-{i}-abcde",
                    "Job",
                    f"fast-{i}",
                    off + 10.3,
                    off,
                    end_off=off + 10.0,
                )
            )

        bundle = _load_bundle(_base_infra(), wfs, execs, skip_initial_offset=True)
        self.assertAlmostEqual(bundle.initial_jump_raw_sec, 5.0, places=3)
        self.assertEqual(len(bundle.initial_workloads), 0)
        self.assertEqual(bundle.skipped_already_admitted_workflows, [])
        self.assertEqual([w.name for w in bundle.scheduled_workflows], [f"fast-{i}" for i in range(5)])
        self.assertEqual([round(w.raw_offset_sec, 1) for w in bundle.scheduled_workflows], [0.0, 20.0, 40.0, 60.0, 80.0])

        # Combined L5+L12: earliest workflow is fast-admitted at +30s and preempted at +50s (no earlier NotAdmitted arrival).
        wfs_combo = [
            _wf_ev(_job_obj("solo-victim", suspend=False, creation_off=30.0), 30.3, state="Admitted", creation_off=30.0),
            _wf_ev(
                _job_obj("solo-victim", suspend=True, creation_off=30.0),
                50.4,
                ev_type="Update",
                state="NotAdmitted",
                creation_off=30.0,
            ),
        ]
        execs_combo = [
            _exec_ev("Admitted", "job-solo-victim-x", "Job", "solo-victim", 30.3, 30.0),
            _exec_ev("PartialExecution", "job-solo-victim-x", "Job", "solo-victim", 50.4, 30.0, end_off=50.0, preempted=True),
            _exec_ev("Admitted", "job-solo-victim-x", "Job", "solo-victim", 70.0, 70.0),
            _exec_ev("FullExecution", "job-solo-victim-x", "Job", "solo-victim", 100.0, 70.0, end_off=100.0),
        ]
        bundle_combo = _load_bundle(_base_infra(), wfs_combo, execs_combo, skip_initial_offset=True)
        self.assertAlmostEqual(bundle_combo.initial_jump_raw_sec, 30.0, places=3)
        self.assertEqual(len(bundle_combo.scheduled_workflows), 1)
        self.assertEqual(bundle_combo.scheduled_workflows[0].trigger_reason, "post_t0_fast_admitted")
        self.assertAlmostEqual(bundle_combo.scheduled_workflows[0].raw_offset_from_t0_sec, 30.0, places=3)


class LoaderRefactorSeamTests(unittest.TestCase):
    """Direct behavioral unit tests for extracted loader seams and shared helpers."""

    @staticmethod
    def _minimal_infra(
        *,
        nodes: dict[str, dict[str, Any]] | None = None,
        cqs: dict[str, dict[str, Any]] | None = None,
        lqs: dict[tuple[str, str], dict[str, Any]] | None = None,
        flavors: dict[str, dict[str, Any]] | None = None,
        topologies: dict[str, dict[str, Any]] | None = None,
        priority_classes: dict[str, dict[str, Any]] | None = None,
    ) -> _ParsedInfra:
        default_nodes = {
            "n1": {
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "n1", "labels": {"rack": "r1"}},
                "status": {"allocatable": {"cpu": "8", "memory": "32Gi"}},
            },
            "n2": {
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "n2", "labels": {"rack": "r2"}},
                "status": {"allocatable": {"cpu": "8", "memory": "32Gi"}},
            },
        }
        default_flavors = {
            "default-flavor": {"apiVersion": "kueue.x-k8s.io/v1beta2", "kind": "ResourceFlavor", "metadata": {"name": "default-flavor"}, "spec": {}},
        }
        default_cqs = {
            "cq": {
                "apiVersion": "kueue.x-k8s.io/v1beta2",
                "kind": "ClusterQueue",
                "metadata": {"name": "cq"},
                "spec": {
                    "resourceGroups": [
                        {
                            "coveredResources": ["cpu", "memory"],
                            "flavors": [
                                {
                                    "name": "default-flavor",
                                    "resources": [
                                        {"name": "cpu", "nominalQuota": "100"},
                                        {"name": "memory", "nominalQuota": "400Gi"},
                                    ],
                                }
                            ],
                        }
                    ]
                },
            }
        }
        default_lqs = {
            ("team", "lq"): {
                "apiVersion": "kueue.x-k8s.io/v1beta2",
                "kind": "LocalQueue",
                "metadata": {"name": "lq", "namespace": "team"},
                "spec": {"clusterQueue": "cq"},
            }
        }
        return _ParsedInfra(
            nodes_by_name=nodes if nodes is not None else default_nodes,
            namespaces_by_name={},
            runtime_classes_by_name={},
            k8s_priority_classes_by_name={},
            topologies_by_name=topologies or {},
            flavors_by_name=flavors if flavors is not None else default_flavors,
            cohorts_by_name={},
            priority_classes_by_name=priority_classes or {},
            cqs_by_name=cqs if cqs is not None else default_cqs,
            lqs_by_key=lqs if lqs is not None else default_lqs,
            pod_new_events={},
            pod_delete_epochs={},
            pod_incarnations=[],
            scheduled_infra_events=[],
            post_t0_cq_names=set(),
            post_t0_lq_keys=set(),
            kueue_configuration=None,
        )

    def test_allocate_k8s_name_handles_edge_cases_prefixes_and_63_char_dedup(self) -> None:
        used: set[str] = set()
        self.assertEqual(_allocate_k8s_name("web-0", used), "web-0")
        self.assertEqual(_allocate_k8s_name("web-0", used), "web-0-1")
        self.assertEqual(_allocate_k8s_name("web-0", used), "web-0-2")

        # Empty, uppercase/underscore, trailing hyphens, non-ASCII Unicode, and numeric starts are all sanitized.
        empty_alloc = _allocate_k8s_name("", used)
        self.assertTrue(empty_alloc and empty_alloc[0].isascii() and empty_alloc[0].isalpha())
        self.assertEqual(_allocate_k8s_name("Web_0", used), "web-0-3")
        self.assertEqual(_allocate_k8s_name("web-", used), "web")
        self.assertEqual(_allocate_k8s_name("p-", used), "p")
        self.assertEqual(_allocate_k8s_name("\u03b1-pod", used), "pod")
        self.assertEqual(_allocate_k8s_name("\u03b1-123", used), "p-123")
        self.assertEqual(_allocate_k8s_name("123-numeric", used), "p-123-numeric")

        # Workload mode (prefix="pg", allow_dots=False) sanitizes dots and prefixes numeric names with pg-.
        wl_used: set[str] = set()
        self.assertEqual(_allocate_k8s_name("123.wl", wl_used, prefix="pg", allow_dots=False), "pg-123-wl")
        self.assertEqual(_allocate_k8s_name("123.wl", wl_used, prefix="pg", allow_dots=False), "pg-123-wl-1")

        # Dotted valid pod names are preserved; 63-char boundary dedup stays <= 63 with no trailing punctuation.
        dotted = "etcd-ip-10-0-1-23.ec2.internal"
        self.assertEqual(_allocate_k8s_name(dotted, used), dotted)
        self.assertEqual(_allocate_k8s_name(dotted, used), f"{dotted}-1")

        long_base = "a" * 70
        a1 = _allocate_k8s_name(long_base, used)
        a2 = _allocate_k8s_name(long_base, used)
        a3 = _allocate_k8s_name(long_base, used)
        self.assertEqual(len({a1, a2, a3}), 3)
        for name in (a1, a2, a3):
            self.assertLessEqual(len(name), 63)
            self.assertTrue(name[0].isascii() and name[0].isalpha())
            self.assertFalse(name.endswith(("-", ".")))

    def test_initial_build_options_and_age_validation(self) -> None:
        with self.assertRaises(ValueError):
            _InitialBuildOptions(effective_start_epoch=10.0, speedup=0.0)
        with self.assertRaises(ValueError):
            _InitialBuildOptions(effective_start_epoch=10.0, speedup=-2.0)
        with self.assertRaises(ValueError):
            _InitialBuildOptions(effective_start_epoch=10.0, min_pod_duration_sec=0.0)

        w_info = WorkloadExecutionInfo(
            workload_name="wl", namespace="team", owner_kind="Job", owner_name="j1", admitted_epochs=[60.0]
        )
        cand = _AdmittedInitialCandidate(
            namespace="team",
            orig_namespace="team",
            kind="Job",
            name="j1",
            local_queue_name="lq",
            raw_obj={},
            exec_info=w_info,
            preempt_item=None,
            creation_epoch=40.0,
        )
        # Admitted strictly before effective_start_epoch (100.0): age = (100 - 60) / 4 = 10s.
        self.assertAlmostEqual(_compute_initial_age_sec(cand, 100.0, 4.0), 10.0, places=6)
        # Admitted at/after effective_start_epoch (50.0): falls back to creation_epoch (40.0) -> (50 - 40) / 2 = 5s.
        self.assertAlmostEqual(_compute_initial_age_sec(cand, 50.0, 2.0), 5.0, places=6)
        # Creation in the future clamps to 0.0.
        self.assertEqual(_compute_initial_age_sec(cand, 10.0, 2.0), 0.0)
        with self.assertRaises(ValueError):
            _compute_initial_age_sec(cand, 100.0, 0.0)

    def test_resolve_candidate_priority_precedence(self) -> None:
        infra = self._minimal_infra(
            priority_classes={
                "team.high": {"metadata": {"name": "team.high"}, "value": 500},
            }
        )
        cand_exec = _AdmittedInitialCandidate(
            namespace="team",
            orig_namespace="team",
            kind="Job",
            name="j1",
            local_queue_name="lq",
            raw_obj={},
            exec_info=WorkloadExecutionInfo(
                workload_name="wl1",
                namespace="team",
                owner_kind="Job",
                owner_name="j1",
                priority_class_name="team.high",
                priority=900,
            ),
            preempt_item=None,
        )
        self.assertEqual(
            _resolve_candidate_priority(
                cand_exec,
                infra,
                labels={},
                annotations={"kueue-optimizer.gke.io/priority": "200"},
                fallback_when_named=100,
            ),
            ("team.high", 900),
        )

        cand_no_exec = _AdmittedInitialCandidate(
            namespace="team",
            orig_namespace="team",
            kind="Job",
            name="j2",
            local_queue_name="lq",
            raw_obj={},
            exec_info=None,
            preempt_item=None,
        )
        self.assertEqual(
            _resolve_candidate_priority(
                cand_no_exec,
                infra,
                labels={"kueue.x-k8s.io/priority-class": "team.high"},
                annotations={"kueue-optimizer.gke.io/priority": "250"},
            ),
            ("team.high", 250),
        )
        self.assertEqual(
            _resolve_candidate_priority(
                cand_no_exec,
                infra,
                labels={"kueue.x-k8s.io/priority-class": "team.high"},
                annotations={},
            ),
            ("team.high", 500),
        )
        self.assertEqual(
            _resolve_candidate_priority(
                cand_no_exec,
                infra,
                labels={"kueue.x-k8s.io/priority-class": "unknown.pc"},
                annotations={},
                fallback_when_named=777,
            ),
            ("unknown.pc", 777),
        )

    def test_partition_admitted_candidates_jump_promotion_and_superseded_preempts(self) -> None:
        t0 = _T0.timestamp()
        preempt_item = ScheduledWorkflowItem(
            target_wall_offset_sec=25.0,
            seq=7,
            raw_offset_sec=50.0,
            raw_offset_from_t0_sec=50.0,
            event_timestamp=_ts(50.0),
            object_kind="Job",
            name="late-fast",
            namespace="team",
            local_queue_name="lq",
            workload_state="NotAdmitted",
            trigger_reason="preempted_after_initial_admission",
            execution_duration_sec=20.0,
            compressed_duration_sec=10.0,
            indexed_from_execution_time=True,
            pod_count=1,
            manifest=_job_obj("late-fast"),
        )
        cands = [
            # Active t0 candidate (created before jumped effective_start_epoch=t0+10).
            _AdmittedInitialCandidate(
                namespace="team",
                orig_namespace="team",
                kind="Job",
                name="running-t0",
                local_queue_name="lq",
                raw_obj=_job_obj("running-t0", suspend=False, creation_off=-50),
                exec_info=WorkloadExecutionInfo(
                    workload_name="wl-running-t0",
                    namespace="team",
                    owner_kind="Job",
                    owner_name="running-t0",
                    local_queue_name="lq",
                    admitted_epochs=[t0 - 40.0],
                ),
                preempt_item=None,
                first_seen_epoch=t0,
                creation_epoch=t0 - 50.0,
            ),
            # Post-t0 fast-admitted workflow created at jumped effective_start_epoch -> promoted & supersedes seq=7.
            _AdmittedInitialCandidate(
                namespace="team",
                orig_namespace="team",
                kind="Job",
                name="late-fast",
                local_queue_name="lq",
                raw_obj=_job_obj("late-fast", suspend=False, creation_off=10.0),
                exec_info=WorkloadExecutionInfo(
                    workload_name="wl-late-fast",
                    namespace="team",
                    owner_kind="Job",
                    owner_name="late-fast",
                    local_queue_name="lq",
                    admitted_epochs=[t0 + 10.2],
                    full_duration_sec=40.0,
                ),
                preempt_item=preempt_item,
                first_seen_epoch=t0 + 10.2,
                creation_epoch=t0 + 10.0,
            ),
            # Already finished before effective_start_epoch -> dropped.
            _AdmittedInitialCandidate(
                namespace="team",
                orig_namespace="team",
                kind="Job",
                name="already-done",
                local_queue_name="lq",
                raw_obj=_job_obj("already-done", suspend=False, creation_off=-20),
                exec_info=WorkloadExecutionInfo(
                    workload_name="wl-done",
                    namespace="team",
                    owner_kind="Job",
                    owner_name="already-done",
                    admitted_epochs=[t0 - 20.0],
                    full_end_epoch=t0 + 5.0,
                ),
                preempt_item=None,
                first_seen_epoch=t0,
                creation_epoch=t0 - 20.0,
            ),
        ]
        # Unrecorded t0 PodGroup in exec_index should be discovered unless excluded by scheduled_not_admitted_owners.
        unrec_wl = WorkloadExecutionInfo(
            workload_name="pg-unrecorded",
            namespace="team",
            owner_kind="PodGroup",
            owner_name="pg-unrecorded",
            local_queue_name="lq",
            admitted_epochs=[t0 - 5.0],
        )
        excl_wl = WorkloadExecutionInfo(
            workload_name="pg-scheduled",
            namespace="team",
            owner_kind="PodGroup",
            owner_name="pg-scheduled",
            local_queue_name="lq",
            admitted_epochs=[t0],
        )
        exec_index = ExecutionTimeIndex(
            by_owner_kind_name={
                ("team", "Job", "late-fast"): cands[1].exec_info,  # type: ignore[dict-item]
            },
            by_workload_name={
                ("team", "wl-running-t0"): cands[0].exec_info,  # type: ignore[dict-item]
                ("team", "pg-unrecorded"): unrec_wl,
                ("team", "pg-scheduled"): excl_wl,
            },
        )
        opts = _InitialBuildOptions(
            t0_epoch=t0,
            effective_start_epoch=t0 + 10.0,
            speedup=2.0,
            min_pod_duration_sec=0.5,
        )
        active, post_t0, superseded = _partition_admitted_candidates(
            cands,
            scheduled_not_admitted_owners={("team", "pg-scheduled")},
            exec_index=exec_index,
            next_post_t0_seq=42,
            opts=opts,
        )
        self.assertEqual([c.name for c in active], ["running-t0", "pg-unrecorded"])
        self.assertEqual(superseded, {7})
        self.assertEqual(len(post_t0), 1)
        self.assertEqual(post_t0[0].seq, 42)
        self.assertEqual(post_t0[0].trigger_reason, "post_t0_fast_admitted")
        self.assertAlmostEqual(post_t0[0].raw_offset_from_t0_sec, 10.0, places=6)
        self.assertAlmostEqual(post_t0[0].compressed_duration_sec, 20.0, places=6)

    def test_match_initial_pods_to_candidates_is_non_mutating_across_tiers(self) -> None:
        infra = self._minimal_infra()
        opts = _InitialBuildOptions(effective_start_epoch=0.0)

        # Tier 1: ad-hoc PodGroup pod creates a new candidate in the returned list without mutating caller's list.
        orig_cands = [
            _AdmittedInitialCandidate(
                namespace="team",
                orig_namespace="team",
                kind="Job",
                name="j1",
                local_queue_name="lq",
                raw_obj=_job_obj("j1", suspend=False),
                exec_info=WorkloadExecutionInfo(
                    workload_name="job-j1-x",
                    namespace="team",
                    owner_kind="Job",
                    owner_name="j1",
                    local_queue_name="lq",
                ),
                preempt_item=None,
            )
        ]
        t1_pods = [
            ("team", "j1-0", 0.0, {"placement": "n1", "workloadName": "job-j1-x", "resourceRequest": {"cpu": "1"}}, None),
            ("team", "adhoc-0", 0.0, {"placement": "n2", "workloadName": "pg-adhoc", "resourceRequest": {"cpu": "1"}}, None),
        ]
        exec_index = ExecutionTimeIndex(
            by_workload_name={
                ("team", "pg-adhoc"): WorkloadExecutionInfo(
                    workload_name="pg-adhoc",
                    namespace="team",
                    owner_kind="PodGroup",
                    owner_name="pg-adhoc",
                    local_queue_name="lq",
                )
            }
        )
        updated_cands, matched, claimed = _match_initial_pods_to_candidates(
            orig_cands, t1_pods, infra, exec_index, set(), opts=opts
        )
        self.assertEqual(len(orig_cands), 1)  # Caller's list was NOT mutated.
        self.assertEqual([c.name for c in updated_cands], ["j1", "pg-adhoc"])
        self.assertEqual([p[1] for p in matched[0]], ["j1-0"])
        self.assertEqual([p[1] for p in matched[1]], ["adhoc-0"])
        self.assertEqual(claimed, {0, 1})

        # Legacy Tiers 2 (prefix) and 3 (hashed profile fallback) when no enriched pod linkage is present.
        hashed_name = "h-" + ("a" * 32)
        legacy_cands = [
            _AdmittedInitialCandidate(
                namespace="team",
                orig_namespace="team",
                kind="Job",
                name="pref",
                local_queue_name="lq",
                raw_obj=_job_obj("pref", cpu="1", mem="1Gi", suspend=False),
                exec_info=None,
                preempt_item=None,
            ),
            _AdmittedInitialCandidate(
                namespace="team",
                orig_namespace="team",
                kind="Job",
                name=hashed_name,
                local_queue_name="lq",
                raw_obj=_job_obj(hashed_name, cpu="3", mem="4Gi", suspend=False),
                exec_info=None,
                preempt_item=None,
            ),
        ]
        legacy_pods = [
            ("team", "pref-abc", 0.0, {"placement": "n1", "resourceRequest": {"cpu": "1", "memory": "1Gi"}}, None),
            ("team", "h-" + ("b" * 32), 0.0, {"placement": "n2", "resourceRequest": {"cpu": "3", "memory": "4Gi"}}, None),
        ]
        _, legacy_matched, legacy_claimed = _match_initial_pods_to_candidates(
            legacy_cands, legacy_pods, infra, ExecutionTimeIndex(), set(), opts=opts
        )
        self.assertEqual([p[1] for p in legacy_matched[0]], ["pref-abc"])
        self.assertEqual([p[1] for p in legacy_matched[1]], ["h-" + ("b" * 32)])
        self.assertEqual(legacy_claimed, {0, 1})

    def test_bootstrap_initial_deployment_caps_ready_replicas_and_updates_node_alloc(self) -> None:
        infra = self._minimal_infra()
        dep_obj = {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "serve", "namespace": "team", "labels": {"kueue.x-k8s.io/queue-name": "lq"}},
            "spec": {
                "replicas": 4,
                "selector": {"matchLabels": {"app": "serve"}},
                "template": {
                    "metadata": {"labels": {"app": "serve"}},
                    "spec": {
                        "containers": [
                            {"name": "c", "image": "pause", "resources": {"requests": {"cpu": "2", "memory": "4Gi"}}}
                        ]
                    },
                },
            },
        }
        cand = _AdmittedInitialCandidate(
            namespace="team",
            orig_namespace="team",
            kind="Deployment",
            name="serve",
            local_queue_name="lq",
            raw_obj=dep_obj,
            exec_info=None,
            preempt_item=None,
            ready_replicas=2,
            creation_epoch=80.0,
        )
        rem_alloc = {"n1": {"cpu": 8.0, "memory": 32.0}, "n2": {"cpu": 8.0, "memory": 32.0}}
        cand_pods = [("team", "serve-0", 0.0, {"placement": "n1", "resourceRequest": {"cpu": "2", "memory": "4Gi"}}, None)]
        opts = _InitialBuildOptions(t0_epoch=100.0, effective_start_epoch=100.0, speedup=2.0)

        boot = _bootstrap_initial_deployment(
            cand,
            cand_pods,
            lq_name="lq",
            cq_name="cq",
            parsed_infra=infra,
            rem_node_alloc=rem_alloc,
            opts=opts,
        )
        self.assertEqual(boot.desired_replicas, 4)
        self.assertEqual(boot.admitted_replicas, 2)
        self.assertEqual(boot.placements, ["n1", "n2"])  # 2nd replica spreads to unused node n2.
        self.assertAlmostEqual(rem_alloc["n2"]["cpu"], 6.0, places=6)
        self.assertAlmostEqual(boot.original_age_sec, 10.0, places=6)

    def test_synthesize_missing_t0_pods_returns_new_list_and_respects_ta_and_duration(self) -> None:
        infra = self._minimal_infra()
        exec_info = WorkloadExecutionInfo(
            workload_name="job-dist-x",
            namespace="team",
            owner_kind="Job",
            owner_name="dist",
            admitted_epochs=[0.0],
            preempted_epochs=[15.0],
            partial_durations_sec=[15.0],
            pod_set_assignments=[
                {
                    "name": "main",
                    "topologyAssignment": {
                        "levels": ["rack"],
                        "slices": [{"domainCount": 1, "valuesPerLevel": [{"universal": "r2"}]}],
                    },
                }
            ],
        )
        cand = _AdmittedInitialCandidate(
            namespace="team",
            orig_namespace="team",
            kind="Job",
            name="dist",
            local_queue_name="lq",
            raw_obj=_job_obj("dist", parallelism=2, cpu="2", mem="4Gi", suspend=False),
            exec_info=exec_info,
            preempt_item=None,
        )
        input_pods = [("team", "dist-0", 0.0, {"placement": "n1", "resourceRequest": {"cpu": "2", "memory": "4Gi"}}, 20.0)]
        rem_alloc = {"n1": {"cpu": 6.0, "memory": 28.0}, "n2": {"cpu": 8.0, "memory": 32.0}}
        opts = _InitialBuildOptions(
            t0_epoch=0.0,
            effective_start_epoch=10.0,
            interrupted_duration_multiplier=2.0,
        )

        result_pods = _synthesize_missing_t0_pods(
            cand,
            input_pods,
            lq_name="lq",
            cq_name="cq",
            parsed_infra=infra,
            rem_node_alloc=rem_alloc,
            opts=opts,
        )
        self.assertEqual(len(input_pods), 1)  # Input list was NOT mutated.
        self.assertEqual(len(result_pods), 2)
        synth_pod = result_pods[1]
        self.assertEqual(synth_pod[3]["placement"], "n2")  # Matched TA rack=r2.
        self.assertAlmostEqual(rem_alloc["n2"]["cpu"], 6.0, places=6)
        self.assertAlmostEqual(synth_pod[4] or 0.0, 30.0, places=6)  # 15s * 2.0 = 30s from admit=0.0.

    def test_materialize_initial_workload_chunks_splits_over_8_podsets_and_attaches_resubmit_once(self) -> None:
        infra = self._minimal_infra()
        opts = _InitialBuildOptions(t0_epoch=0.0, effective_start_epoch=10.0, speedup=2.0, min_pod_duration_sec=0.5)
        cand = _AdmittedInitialCandidate(
            namespace="team",
            orig_namespace="team",
            kind="Job",
            name="wide",
            local_queue_name="lq",
            raw_obj=_job_obj("wide", suspend=False),
            exec_info=WorkloadExecutionInfo(
                workload_name="job-wide-x",
                namespace="team",
                owner_kind="Job",
                owner_name="wide",
                admitted_epochs=[0.0],
            ),
            preempt_item=None,
        )
        # 9 distinct resource profiles -> must split into 2 Workloads (8 PodSets + 1 PodSet).
        cand_pods = [
            (
                "team",
                f"wide-{i}",
                0.0,
                {"placement": "n1", "podSetName": f"ps{i}", "resourceRequest": {"cpu": str(i + 1), "memory": "1Gi"}},
                50.0,
            )
            for i in range(9)
        ]
        exec_index = ExecutionTimeIndex(
            by_owner_kind_name={
                ("team", "Job", "wide"): WorkloadExecutionInfo(
                    workload_name="job-wide-x",
                    namespace="team",
                    owner_kind="Job",
                    owner_name="wide",
                    full_duration_sec=40.0,
                )
            }
        )
        resub = _build_t0_resubmit_item(
            cand, c_idx=0, lq_name="lq", admit_iso="2026-03-01T00:00:10Z", exec_index=exec_index, opts=opts
        )
        self.assertIsNotNone(resub)

        used_wls: dict[str, set[str]] = {}
        used_pods: dict[str, set[str]] = {}
        chunks = _materialize_initial_workload_chunks(
            cand,
            cand_pods,
            c_idx=0,
            lq_name="lq",
            cq_name="cq",
            parsed_infra=infra,
            used_wl_names_by_ns=used_wls,
            used_pod_names_by_ns=used_pods,
            admit_iso="2026-03-01T00:00:10Z",
            resubmit_item=resub,
            opts=opts,
        )
        self.assertEqual(len(chunks), 2)
        self.assertEqual(len(chunks[0].workload_manifest["spec"]["podSets"]), 8)
        self.assertEqual(len(chunks[1].workload_manifest["spec"]["podSets"]), 1)
        self.assertEqual([c.workload_name for c in chunks], ["t0pg-wide-0", "t0pg-wide-1"])
        self.assertIs(chunks[0].resubmit_item, resub)
        self.assertIsNone(chunks[1].resubmit_item)
        self.assertAlmostEqual(chunks[0].pods[0].remaining_duration_sec, 20.0, places=6)  # (50 - 10) / 2

    def test_kwok_pause_pod_builders_and_request_extraction(self) -> None:
        self.assertEqual(_extract_pod_resource_requests({"resourceRequest": {}}), {})
        self.assertEqual(
            _extract_pod_resource_requests({"resourceRequest": {"cpu": "2", "ephemeral-storage": "5Gi"}}),
            {"cpu": "2"},
        )
        spec_no_node = _build_kwok_pause_pod_spec("pause:3.10", {"cpu": "1"})
        self.assertNotIn("nodeName", spec_no_node)
        self.assertEqual(spec_no_node["containers"][0]["resources"]["requests"], {"cpu": "1"})

        manifest = _build_kwok_pause_pod_manifest(
            name="sys-0",
            namespace="infra-pods",
            labels={"app": "sys"},
            annotations={"k": "v"},
            pause_img="pause:3.10",
            reqs={},
            placement="n1",
        )
        self.assertEqual(manifest["metadata"]["name"], "sys-0")
        self.assertEqual(manifest["spec"]["nodeName"], "n1")
        self.assertEqual(manifest["spec"]["containers"][0]["resources"], {"requests": {}, "limits": {}})

    def test_build_initial_infra_pods_skips_claimed_scales_and_deduplicates_cross_namespace(self) -> None:
        qualifying = [
            ("team", "claimed-0", 0.0, {"placement": "n1", "resourceRequest": {"cpu": "2"}}, 100.0),
            ("ns-a", "sys-0", 0.0, {"placement": "n1", "resourceRequest": {}}, 120.0),
            ("ns-b", "sys-0", 0.0, {"placement": "n2", "resourceRequest": {"cpu": "1"}}, None),
        ]
        pods = _build_initial_infra_pods(
            qualifying,
            claimed_pod_indices={0},
            opts=_InitialBuildOptions(
                effective_start_epoch=20.0,
                speedup=10.0,
                min_pod_duration_sec=0.5,
                infinite_pod_duration_sec=864000.0,
                pause_image_override="pause:3.10",
                infra_pods_namespace="infra-pods",
            ),
        )
        self.assertEqual([p.name for p in pods], ["sys-0", "sys-0-1"])
        self.assertEqual([p.manifest["metadata"]["name"] for p in pods], ["sys-0", "sys-0-1"])
        self.assertEqual(
            [p.manifest["metadata"]["annotations"]["kueue-optimizer.gke.io/original-name"] for p in pods],
            ["sys-0", "sys-0"],
        )
        self.assertEqual(
            [p.manifest["metadata"]["annotations"]["kueue-optimizer.gke.io/original-namespace"] for p in pods],
            ["ns-a", "ns-b"],
        )
        self.assertFalse(pods[0].is_infinite)
        self.assertAlmostEqual(pods[0].remaining_duration_sec, 10.0, places=6)
        self.assertEqual(pods[0].resource_request, {})
        self.assertTrue(pods[1].is_infinite)
        self.assertEqual(pods[1].manifest["metadata"]["annotations"].get("kueue-optimizer.gke.io/infinite-duration"), "true")


if __name__ == "__main__":
    unittest.main()
