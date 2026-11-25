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

"""Focused table-driven unit tests for R6 (TTL/activeDeadlineSeconds), R7 (FIFO tier spacing),
R8 (finite t0 bootstrap setup lag), and R10 (chamber control-plane health checks)."""

from __future__ import annotations

import contextlib
import copy
import dataclasses
import io
import json
import os
from pathlib import Path
import signal
import statistics
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from chamber.constants import CHAMBER_CORE_HEALTH_COMPONENTS
from chamber.lifecycle import (
    ChamberHealthSnapshot,
    ChamberManager,
    ClusterStatusSnapshot,
    ComponentHealthStatus,
    verify_chamber_control_plane_health,
)
from chamber.parsers import parse_positive_duration_annotation
from runner.loader import (
    InitialPodSpec,
    InitialWorkloadPodGroup,
    ScheduledWorkflowDeletion,
    load_scenario_bundle,
)
import runner.replay as replay
from runner.replay import (
    ReplayResult,
    apply_scenario_queues_and_wait,
    run_scenario_replay,
)
from runner.validation import (
    _inject_pod_execution_time_and_sanitize_workflow,
    _scale_active_deadline_seconds,
)
import testrunner


class _FakeMgr:
    """In-memory ChamberManager stub for unit tests (never contacts any cluster)."""

    def __init__(
        self,
        *,
        responder=None,
        apply_latency: float = 0.0,
        health_snapshots: list[ChamberHealthSnapshot] | None = None,
    ) -> None:
        self.t0 = time.monotonic()
        self._responder = responder
        self.apply_latency = apply_latency
        self._health_snapshots = list(health_snapshots) if health_snapshots is not None else None
        self._health_idx = 0
        self.health_calls = 0
        self.applied: list[tuple[float, str, str]] = []
        self.applied_docs: list[dict] = []
        self.requests: list[tuple[str, str, dict | None, int]] = []
        self.events: list[str] = []
        self.workload_namespace = "default"
        self.chamber_namespace = "kueue-optimizer-chamber-unit"
        self._retired_serving_workloads: dict = {}

    def ssa_apply_doc(self, doc: dict) -> None:
        if self.apply_latency:
            time.sleep(self.apply_latency)
        kind = str(doc.get("kind") or "")
        name = str((doc.get("metadata") or {}).get("name") or "")
        self.applied.append((time.monotonic() - self.t0, kind, name))
        self.applied_docs.append(copy.deepcopy(doc))
        self.events.append(f"apply:{kind}:{name}")

    def apply_manifests_parallel(self, docs: list[dict], max_workers: int = 24) -> None:
        if max_workers <= 1 or len(docs) == 1:
            for d in docs:
                self.ssa_apply_doc(d)
        else:
            if self.apply_latency:
                time.sleep(self.apply_latency)
            now = time.monotonic() - self.t0
            for d in docs:
                kind = str(d.get("kind") or "")
                name = str((d.get("metadata") or {}).get("name") or "")
                self.applied.append((now, kind, name))
                self.applied_docs.append(copy.deepcopy(d))
                self.events.append(f"apply:{kind}:{name}")

    def apply_pre_admitted_workloads_parallel(self, pairs) -> None:
        for wl_doc, _st in pairs:
            name = str((wl_doc.get("metadata") or {}).get("name") or "")
            self.events.append(f"wl:{name}")

    def wait_for_pods_running(self, keys, timeout_sec: float = 90.0, require_ready: bool = False) -> None:
        self.events.append(f"wait_pods:{len(keys)}")

    def api_request(
        self,
        method: str,
        path: str,
        body: bytes | None = None,
        headers: dict | None = None,
        content_type: str | None = None,
    ) -> tuple[int, bytes]:
        parsed_body = json.loads(body.decode("utf-8")) if body else None
        resp = self._responder(method, path, parsed_body) if self._responder is not None else None
        status, data = resp if resp is not None else (200, b'{"items": []}')
        self.requests.append((method, path, parsed_body, status))
        return status, data

    def api_delete(self, path: str, obj=None, ignore_not_found: bool = True) -> tuple[int, bytes]:
        return self.api_request("DELETE", path)

    def check_chamber_health_snapshot(self, *, components=None) -> ChamberHealthSnapshot:
        self.health_calls += 1
        if not self._health_snapshots:
            return _healthy_snapshot()
        snap = self._health_snapshots[min(self._health_idx, len(self._health_snapshots) - 1)]
        self._health_idx += 1
        return snap


def _healthy_snapshot(
    *,
    etcd_restarts: int = 0,
    api_restarts: int = 0,
    kueue_restarts: int = 0,
    etcd_ready: int = 1,
    api_ready: int = 1,
    kueue_ready: int = 1,
    etcd_uid: str = "uid-etcd-1",
    api_uid: str = "uid-api-1",
    kueue_uid: str = "uid-kueue-1",
    overrides: dict[str, ComponentHealthStatus] | None = None,
) -> ChamberHealthSnapshot:
    comps = {
        "kwok-etcd": ComponentHealthStatus("kwok-etcd", etcd_ready, 1, etcd_restarts, (etcd_uid,)),
        "kwok-kube-apiserver": ComponentHealthStatus("kwok-kube-apiserver", api_ready, 1, api_restarts, (api_uid,)),
        "kwok-kube-controller-manager": ComponentHealthStatus(
            "kwok-kube-controller-manager", 1, 1, 0, ("uid-kcm-1",)
        ),
        "kwok-kube-scheduler": ComponentHealthStatus("kwok-kube-scheduler", 1, 1, 0, ("uid-sched-1",)),
        "kwok-controller": ComponentHealthStatus("kwok-controller", 1, 1, 0, ("uid-kwok-1",)),
        "jobset-controller-manager": ComponentHealthStatus(
            "jobset-controller-manager", 1, 1, 0, ("uid-jobset-1",)
        ),
        "kueue-controller-manager": ComponentHealthStatus(
            "kueue-controller-manager", kueue_ready, 1, kueue_restarts, (kueue_uid,)
        ),
        "prometheus": ComponentHealthStatus("prometheus", 1, 1, 0, ("uid-prom-1",)),
    }
    if overrides:
        comps.update(overrides)
    return ChamberHealthSnapshot(components=comps, query_ok=True)


def _load_bundle(scenario_name: str = "st08-single-priority-deep-backlog-waves", speedup: float = 1.0):
    return load_scenario_bundle(
        scenario_dir=REPO_ROOT / "testscenarios" / "self-tests" / scenario_name,
        speedup=speedup,
        skip_initial_offset=True,
    )


def _synth_items(template, count: int, wall_gap: float, creation_gap: float, prefix: str = "wf"):
    out = []
    for i in range(count):
        man = copy.deepcopy(template.manifest)
        man.setdefault("metadata", {})["name"] = f"{prefix}-{i:03d}"
        out.append(
            dataclasses.replace(
                template,
                target_wall_offset_sec=i * wall_gap,
                seq=i,
                raw_offset_sec=i * creation_gap,
                raw_offset_from_t0_sec=i * creation_gap,
                name=f"{prefix}-{i:03d}",
                manifest=man,
                creation_epoch=template.creation_epoch + i * creation_gap,
                incarnation_index=0,
            )
        )
    return out


class ManifestTTLAndDeadlineTests(unittest.TestCase):
    """R6: strip ttlSecondsAfterFinished and scale activeDeadlineSeconds by --speedup."""

    def test_job_ttl_stripped_and_active_deadline_scaled_table(self) -> None:
        cases = [
            ("speedup_1_exact", 1.0, 300, 120, 300, 120),
            ("speedup_10_exact", 10.0, 600, 100, 60, 10),
            ("speedup_10_ceil", 10.0, 15, 21, 2, 3),
            ("speedup_100_floor_1", 100.0, 5, 1, 1, 1),
            ("speedup_50_zero_preserved", 50.0, 0, 0, 0, 0),
        ]
        for label, speedup, job_deadline, pod_deadline, exp_job_dl, exp_pod_dl in cases:
            with self.subTest(case=label):
                raw_job = {
                    "apiVersion": "batch/v1",
                    "kind": "Job",
                    "metadata": {"name": "j-r6", "namespace": "ai-lab"},
                    "spec": {
                        "ttlSecondsAfterFinished": 0,
                        "activeDeadlineSeconds": job_deadline,
                        "parallelism": 1,
                        "completions": 1,
                        "template": {
                            "spec": {
                                "activeDeadlineSeconds": pod_deadline,
                                "containers": [{"name": "c", "image": "pause:3.10"}],
                            }
                        },
                    },
                }
                doc, _ = _inject_pod_execution_time_and_sanitize_workflow(
                    raw_job,
                    exec_str="5",
                    raw_duration_sec=50.0,
                    compressed_duration_sec=5.0,
                    speedup=speedup,
                )
                self.assertNotIn("ttlSecondsAfterFinished", doc["spec"])
                self.assertEqual(doc["spec"]["activeDeadlineSeconds"], exp_job_dl)
                self.assertEqual(doc["spec"]["template"]["spec"]["activeDeadlineSeconds"], exp_pod_dl)

    def test_jobset_ttl_stripped_and_deadlines_scaled_at_all_levels(self) -> None:
        raw_js = {
            "apiVersion": "jobset.x-k8s.io/v1alpha2",
            "kind": "JobSet",
            "metadata": {"name": "js-r6", "namespace": "ai-lab"},
            "spec": {
                "ttlSecondsAfterFinished": 30,
                "activeDeadlineSeconds": 1000,
                "replicatedJobs": [
                    {
                        "name": "worker",
                        "replicas": 2,
                        "template": {
                            "spec": {
                                "ttlSecondsAfterFinished": 0,
                                "activeDeadlineSeconds": 250,
                                "parallelism": 2,
                                "completions": 2,
                                "template": {
                                    "spec": {
                                        "activeDeadlineSeconds": 3,
                                        "containers": [{"name": "c", "image": "pause:3.10"}],
                                    }
                                },
                            }
                        },
                    }
                ],
            },
        }
        doc, _ = _inject_pod_execution_time_and_sanitize_workflow(
            raw_js,
            exec_str="5",
            raw_duration_sec=500.0,
            compressed_duration_sec=5.0,
            speedup=100.0,
        )
        self.assertNotIn("ttlSecondsAfterFinished", doc["spec"])
        self.assertEqual(doc["spec"]["activeDeadlineSeconds"], 10)
        rj_spec = doc["spec"]["replicatedJobs"][0]["template"]["spec"]
        self.assertNotIn("ttlSecondsAfterFinished", rj_spec)
        self.assertEqual(rj_spec["activeDeadlineSeconds"], 3)
        self.assertEqual(rj_spec["template"]["spec"]["activeDeadlineSeconds"], 1)

    def test_standalone_pod_active_deadline_scaled_and_end_to_end_loader(self) -> None:
        raw_pod = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "p-r6",
                "namespace": "ai-lab",
                "labels": {"kueue.x-k8s.io/queue-name": "lq-main"},
            },
            "spec": {
                "activeDeadlineSeconds": 450,
                "containers": [{"name": "c", "image": "pause:3.10"}],
            },
        }
        doc, _ = _inject_pod_execution_time_and_sanitize_workflow(
            raw_pod,
            exec_str="2",
            raw_duration_sec=20.0,
            compressed_duration_sec=2.0,
            speedup=10.0,
        )
        self.assertEqual(doc["spec"]["activeDeadlineSeconds"], 45)

        # Verify through full load_scenario_bundle on a synthetic scenario with ttlSecondsAfterFinished + activeDeadlineSeconds
        with tempfile.TemporaryDirectory() as tmp:
            sc_dir = Path(tmp)
            ts0 = "2026-01-01T00:00:00Z"
            ts1 = "2026-01-01T00:00:10Z"
            (sc_dir / "infra.json").write_text(
                "\n".join(
                    [
                        json.dumps(
                            {
                                "timestamp": ts0,
                                "eventType": "New",
                                "payload": {
                                    "apiVersion": "v1",
                                    "kind": "Node",
                                    "metadata": {"name": "n0"},
                                    "status": {"allocatable": {"cpu": "4", "memory": "16Gi", "pods": "110"}},
                                },
                            }
                        ),
                        json.dumps(
                            {
                                "timestamp": ts0,
                                "eventType": "New",
                                "payload": {
                                    "apiVersion": "kueue.x-k8s.io/v1beta2",
                                    "kind": "ClusterQueue",
                                    "metadata": {"name": "cq-main"},
                                    "spec": {"namespaceSelector": {}},
                                },
                            }
                        ),
                        json.dumps(
                            {
                                "timestamp": ts0,
                                "eventType": "New",
                                "payload": {
                                    "apiVersion": "kueue.x-k8s.io/v1beta2",
                                    "kind": "LocalQueue",
                                    "metadata": {"name": "lq-main", "namespace": "ai-lab"},
                                    "spec": {"clusterQueue": "cq-main"},
                                },
                            }
                        ),
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (sc_dir / "workflows.json").write_text(
                json.dumps(
                    {
                        "timestamp": ts1,
                        "eventType": "New",
                        "workloadState": "NotAdmitted",
                        "payload": {
                            "objectKind": "Job",
                            "namespace": "ai-lab",
                            "name": "ttl-job",
                            "localQueueName": "lq-main",
                            "object": {
                                "apiVersion": "batch/v1",
                                "kind": "Job",
                                "metadata": {
                                    "name": "ttl-job",
                                    "namespace": "ai-lab",
                                    "labels": {"kueue.x-k8s.io/queue-name": "lq-main"},
                                },
                                "spec": {
                                    "ttlSecondsAfterFinished": 5,
                                    "activeDeadlineSeconds": 300,
                                    "parallelism": 1,
                                    "completions": 1,
                                    "template": {
                                        "spec": {
                                            "activeDeadlineSeconds": 120,
                                            "containers": [{"name": "c", "image": "pause:3.10"}],
                                        }
                                    },
                                },
                            },
                        },
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            (sc_dir / "execution-time.json").write_text(
                json.dumps(
                    {
                        "timestamp": ts1,
                        "eventType": "FullExecution",
                        "payload": {
                            "workloadName": "job-ttl-job-abcde",
                            "namespace": "ai-lab",
                            "ownerKind": "Job",
                            "ownerName": "ttl-job",
                            "durationSeconds": 60.0,
                        },
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            bundle = load_scenario_bundle(sc_dir, speedup=20.0, skip_initial_offset=True)
            self.assertEqual(len(bundle.scheduled_workflows), 1)
            j_spec = bundle.scheduled_workflows[0].manifest["spec"]
            self.assertNotIn("ttlSecondsAfterFinished", j_spec)
            self.assertEqual(j_spec["activeDeadlineSeconds"], 15)
            self.assertEqual(j_spec["template"]["spec"]["activeDeadlineSeconds"], 6)

    def test_rayjob_translated_active_deadline_and_min_pod_duration_floor(self) -> None:
        raw_ray = {
            "apiVersion": "ray.io/v1",
            "kind": "RayJob",
            "metadata": {
                "name": "ray-r6",
                "namespace": "ai-lab",
                "labels": {"kueue.x-k8s.io/queue-name": "lq-main"},
            },
            "spec": {
                "ttlSecondsAfterFinished": 45,
                "activeDeadlineSeconds": 60,
                "rayClusterSpec": {
                    "headGroupSpec": {
                        "template": {
                            "spec": {
                                "activeDeadlineSeconds": 50,
                                "containers": [{"name": "ray-head", "image": "pause:3.10"}],
                            }
                        }
                    },
                    "workerGroupSpecs": [
                        {
                            "groupName": "workers",
                            "replicas": 2,
                            "template": {
                                "spec": {
                                    "containers": [{"name": "ray-worker", "image": "pause:3.10"}],
                                }
                            },
                        }
                    ],
                },
            },
        }
        # Raw duration 30s < raw deadlines (60s, 50s); under 100x speedup + --min-pod-duration 2.0,
        # naive ceil(60/100)=1 would invert healthy deadlines below 2.0s; floor must lift to ceil(2.0)+1 = 3.
        translated, _ = _inject_pod_execution_time_and_sanitize_workflow(
            raw_ray,
            exec_str="2",
            raw_duration_sec=30.0,
            compressed_duration_sec=2.0,
            speedup=100.0,
            min_pod_duration_sec=2.0,
            translate_unsupported_crds=True,
        )
        self.assertEqual(translated["kind"], "JobSet")
        self.assertNotIn("ttlSecondsAfterFinished", translated["spec"])
        self.assertEqual(translated["spec"]["activeDeadlineSeconds"], 3)
        head_pod_spec = translated["spec"]["replicatedJobs"][0]["template"]["spec"]["template"]["spec"]
        self.assertEqual(head_pod_spec["activeDeadlineSeconds"], 3)

        # Also verify untranslated RayJob preserves the same scaling and floor.
        untranslated, _ = _inject_pod_execution_time_and_sanitize_workflow(
            raw_ray,
            exec_str="2.3",
            raw_duration_sec=30.0,
            compressed_duration_sec=2.3,
            speedup=100.0,
            min_pod_duration_sec=2.3,
            translate_unsupported_crds=False,
        )
        self.assertEqual(untranslated["kind"], "RayJob")
        self.assertNotIn("ttlSecondsAfterFinished", untranslated["spec"])
        self.assertEqual(untranslated["spec"]["activeDeadlineSeconds"], 4)

    def test_scale_active_deadline_guards_invalid_values_and_preserves_tight_deadlines(self) -> None:
        invalid_cases = [
            ("bool_true", True),
            ("bool_false", False),
            ("negative_int", -10),
            ("non_int_float", 12.5),
            ("string_val", "300"),
            ("none_val", None),
        ]
        for label, bad_val in invalid_cases:
            with self.subTest(case=label):
                spec = {"activeDeadlineSeconds": bad_val}
                _scale_active_deadline_seconds(spec, 10.0, raw_duration_sec=10.0, compressed_duration_sec=2.0)
                self.assertEqual(spec["activeDeadlineSeconds"], bad_val)

        # Intentionally tighter raw deadline (20 <= raw_duration 30) must NOT receive the healthy floor.
        tight_spec = {"activeDeadlineSeconds": 20}
        _scale_active_deadline_seconds(tight_spec, 100.0, raw_duration_sec=30.0, compressed_duration_sec=2.0)
        self.assertEqual(tight_spec["activeDeadlineSeconds"], 1)

        # Healthy raw deadline (31 > raw_duration 30) receives ceil(2.0) + 1 = 3 floor.
        healthy_spec = {"activeDeadlineSeconds": 31}
        _scale_active_deadline_seconds(healthy_spec, 100.0, raw_duration_sec=30.0, compressed_duration_sec=2.0)
        self.assertEqual(healthy_spec["activeDeadlineSeconds"], 3)


class FifoTierSpacingTests(unittest.TestCase):
    """R7: eliminate unnecessary 1s FIFO tier spacing lag on dense traces while preserving backlog ordering."""

    def test_dense_post_t0_stream_with_slow_poll_has_no_cascading_fifo_stall(self) -> None:
        base = _load_bundle(speedup=100.0)
        tmpl = base.scheduled_workflows[0]
        items = _synth_items(tmpl, count=20, wall_gap=0.10, creation_gap=10.0)
        bundle = dataclasses.replace(
            base,
            scheduled_workflows=items,
            scheduled_infra_events=[],
            scheduled_workflow_updates=[],
            scheduled_workflow_deletions=[],
            initial_workloads=[],
            initial_deployments=[],
            initial_pods=[],
        )

        fake_now = [1000.0]
        created: list[tuple[str, float]] = []

        def fake_mono() -> float:
            return fake_now[0]

        def fake_wall() -> float:
            return fake_now[0]

        def fake_sleep(sec: float) -> None:
            fake_now[0] += max(0.0, float(sec))

        mgr = _FakeMgr()

        def fake_apply(docs, max_workers=24):
            for d in docs:
                fake_now[0] += 0.03
                created.append((d["metadata"]["name"], fake_now[0] - 1000.0))

        def fake_slow_poll(*_a, **_kw):
            fake_now[0] += 0.30
            return ClusterStatusSnapshot(workloads_query_ok=True)

        with (
            mock.patch.object(mgr, "apply_manifests_parallel", side_effect=fake_apply),
            mock.patch.object(replay, "query_cluster_status", side_effect=fake_slow_poll),
            mock.patch("runner.replay.time.monotonic", side_effect=fake_mono),
            mock.patch("runner.replay.time.time", side_effect=fake_wall),
            mock.patch("runner.replay.time.sleep", side_effect=fake_sleep),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            res = run_scenario_replay(
                mgr=mgr,
                bundle=bundle,
                speedup=100.0,
                max_run_seconds=None,
                status_interval_sec=3.0,
                wait_for_completion=False,
            )

        self.assertEqual(res.stop_reason, "completed")
        self.assertEqual([nm for nm, _ in created], [it.name for it in items])
        targets = {it.name: it.target_wall_offset_sec for it in items}
        lags = [t - targets[nm] for nm, t in created]
        self.assertLess(statistics.mean(lags), 0.15, f"unexpected mean lag: {lags}")
        self.assertLess(max(lags), 0.45, f"unexpected max lag: {lags}")
        self.assertLess(created[-1][1], items[-1].target_wall_offset_sec + 0.25)

    def test_same_target_tiers_space_across_seconds_without_extra_delay_past_boundary(self) -> None:
        base = _load_bundle(speedup=1.0)
        tmpl = base.scheduled_workflows[0]

        # 3 simultaneous t0 backlog items at target=0.0, followed by 3 dense post-t0 arrivals at 0.5, 0.6, 0.7
        t0_tiers = _synth_items(tmpl, count=3, wall_gap=0.0, creation_gap=5.0, prefix="t0")
        dense_post = []
        for idx in range(3):
            man = copy.deepcopy(tmpl.manifest)
            man.setdefault("metadata", {})["name"] = f"post-{idx:03d}"
            dense_post.append(
                dataclasses.replace(
                    tmpl,
                    target_wall_offset_sec=0.5 + idx * 0.1,
                    seq=10 + idx,
                    raw_offset_sec=50.0 + idx * 10.0,
                    raw_offset_from_t0_sec=50.0 + idx * 10.0,
                    name=f"post-{idx:03d}",
                    manifest=man,
                    creation_epoch=tmpl.creation_epoch + 50.0 + idx * 10.0,
                    incarnation_index=0,
                )
            )

        bundle = dataclasses.replace(
            base,
            scheduled_workflows=t0_tiers + dense_post,
            scheduled_infra_events=[],
            scheduled_workflow_updates=[],
            scheduled_workflow_deletions=[],
            initial_workloads=[],
            initial_deployments=[],
            initial_pods=[],
        )

        # Start at fractional second 1000.85 so next integer second is only 0.17s away (NOT +1.0s!)
        fake_now = [1000.85]
        created: list[tuple[str, float, int]] = []

        def fake_apply(docs, max_workers=24):
            for d in docs:
                created.append((d["metadata"]["name"], fake_now[0], int(fake_now[0])))

        mgr = _FakeMgr()
        with (
            mock.patch.object(mgr, "apply_manifests_parallel", side_effect=fake_apply),
            mock.patch.object(
                replay,
                "query_cluster_status",
                return_value=ClusterStatusSnapshot(workloads_query_ok=True),
            ),
            mock.patch("runner.replay.time.monotonic", side_effect=lambda: fake_now[0]),
            mock.patch("runner.replay.time.time", side_effect=lambda: fake_now[0]),
            mock.patch(
                "runner.replay.time.sleep",
                side_effect=lambda s: fake_now.__setitem__(0, fake_now[0] + max(0.0, float(s))),
            ),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            run_scenario_replay(
                mgr=mgr,
                bundle=bundle,
                speedup=1.0,
                max_run_seconds=20.0,
                status_interval_sec=5.0,
                wait_for_completion=False,
            )

        self.assertEqual([c[0] for c in created], ["t0-000", "t0-001", "t0-002", "post-000", "post-001", "post-002"])
        # t0-000 at 1000.85 (sec 1000), t0-001 at 1001.02 (sec 1001, only +0.17s!), t0-002 at 1002.02 (sec 1002)
        self.assertEqual([c[2] for c in created[:3]], [1000, 1001, 1002])
        self.assertLess(created[1][1] - created[0][1], 0.30)
        # post-000..002 start in strictly later second (1003) than t0-002 (1002), and flush together without 1s gaps!
        self.assertGreater(created[3][2], created[2][2])
        self.assertEqual(created[3][2], created[5][2])

    def test_leading_cluster_fifo_drain_survives_many_post_t0_arrivals(self) -> None:
        base = _load_bundle(speedup=1.0)
        tmpl = base.scheduled_workflows[0]
        # 3 simultaneous t0 backlog tiers followed by 30 dense post-t0 arrivals (>12 tiers and >24 items total).
        t0_tiers = _synth_items(tmpl, count=3, wall_gap=0.0, creation_gap=5.0, prefix="t0")
        post_items = []
        for idx in range(30):
            man = copy.deepcopy(tmpl.manifest)
            man.setdefault("metadata", {})["name"] = f"post-{idx:03d}"
            post_items.append(
                dataclasses.replace(
                    tmpl,
                    target_wall_offset_sec=0.05 + idx * 0.05,
                    seq=100 + idx,
                    raw_offset_sec=10.0 + idx * 5.0,
                    raw_offset_from_t0_sec=10.0 + idx * 5.0,
                    name=f"post-{idx:03d}",
                    manifest=man,
                    creation_epoch=tmpl.creation_epoch + 10.0 + idx * 5.0,
                    incarnation_index=0,
                )
            )
        bundle = dataclasses.replace(
            base,
            scheduled_workflows=t0_tiers + post_items,
            scheduled_infra_events=[],
            scheduled_workflow_updates=[],
            scheduled_workflow_deletions=[],
            initial_workloads=[],
            initial_deployments=[],
            initial_pods=[],
        )

        fake_now = [1000.85]
        created: list[tuple[str, float, int]] = []

        def fake_apply(docs, max_workers=24):
            for d in docs:
                created.append((d["metadata"]["name"], fake_now[0], int(fake_now[0])))

        mgr = _FakeMgr()
        with (
            mock.patch.object(mgr, "apply_manifests_parallel", side_effect=fake_apply),
            mock.patch.object(
                replay,
                "query_cluster_status",
                return_value=ClusterStatusSnapshot(workloads_query_ok=True),
            ),
            mock.patch("runner.replay.time.monotonic", side_effect=lambda: fake_now[0]),
            mock.patch("runner.replay.time.time", side_effect=lambda: fake_now[0]),
            mock.patch(
                "runner.replay.time.sleep",
                side_effect=lambda s: fake_now.__setitem__(0, fake_now[0] + max(0.0, float(s))),
            ),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            run_scenario_replay(
                mgr=mgr,
                bundle=bundle,
                speedup=1.0,
                max_run_seconds=20.0,
                status_interval_sec=5.0,
                wait_for_completion=False,
            )

        self.assertEqual([c[0] for c in created[:3]], ["t0-000", "t0-001", "t0-002"])
        # All 3 t0 tiers remain spaced across distinct RFC3339 seconds despite 30 post-t0 arrivals joining due_batch.
        self.assertEqual([c[2] for c in created[:3]], [1000, 1001, 1002])
        # Post-t0 items flush cleanly in second 1003 without 1s gaps.
        self.assertGreater(created[3][2], created[2][2])
        self.assertEqual(created[3][2], created[-1][2])
        self.assertEqual(len(created), 33)

    def test_non_colliding_prefix_dispatches_without_delay_ahead_of_colliding_suffix(self) -> None:
        base = _load_bundle(speedup=1.0)
        tmpl = base.scheduled_workflows[0]
        specs = [
            ("pre-0", 0.05, 10.0, 1),
            ("pre-1", 0.10, 20.0, 2),
            ("col-a", 0.20, 30.0, 3),
            ("col-b", 0.20, 40.0, 4),
        ]
        items = []
        for nm, wall_off, c_off, seq in specs:
            man = copy.deepcopy(tmpl.manifest)
            man.setdefault("metadata", {})["name"] = nm
            items.append(
                dataclasses.replace(
                    tmpl,
                    target_wall_offset_sec=wall_off,
                    seq=seq,
                    raw_offset_sec=c_off,
                    raw_offset_from_t0_sec=c_off,
                    name=nm,
                    manifest=man,
                    creation_epoch=tmpl.creation_epoch + c_off,
                    incarnation_index=0,
                )
            )
        bundle = dataclasses.replace(
            base,
            scheduled_workflows=items,
            scheduled_infra_events=[],
            scheduled_workflow_updates=[],
            scheduled_workflow_deletions=[],
            initial_workloads=[],
            initial_deployments=[],
            initial_pods=[],
        )

        # Initial slow status poll advances wall clock from 1000.0 to 1000.35 so all 4 items coalesce into one batch.
        fake_now = [1000.0]
        apply_calls: list[tuple[list[str], int, float, int]] = []

        def fake_apply(docs, max_workers=24):
            apply_calls.append(
                ([d["metadata"]["name"] for d in docs], max_workers, fake_now[0], int(fake_now[0]))
            )

        def fake_poll(*_a, **_kw):
            if fake_now[0] < 1000.30:
                fake_now[0] = 1000.35
            return ClusterStatusSnapshot(workloads_query_ok=True)

        mgr = _FakeMgr()
        with (
            mock.patch.object(mgr, "apply_manifests_parallel", side_effect=fake_apply),
            mock.patch.object(replay, "query_cluster_status", side_effect=fake_poll),
            mock.patch("runner.replay.time.monotonic", side_effect=lambda: fake_now[0]),
            mock.patch("runner.replay.time.time", side_effect=lambda: fake_now[0]),
            mock.patch(
                "runner.replay.time.sleep",
                side_effect=lambda s: fake_now.__setitem__(0, fake_now[0] + max(0.0, float(s))),
            ),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            run_scenario_replay(
                mgr=mgr,
                bundle=bundle,
                speedup=1.0,
                max_run_seconds=20.0,
                status_interval_sec=5.0,
                wait_for_completion=False,
            )

        # Call 1: non-colliding prefix [pre-0, pre-1] dispatched sequentially (max_workers=1) right after poll with 0s extra delay.
        # Call 2: colliding pair first tier [col-a] immediately in the same timestamp (sec 1000).
        # Call 3: colliding pair second tier [col-b] in next whole second (sec 1001).
        self.assertEqual([c[0] for c in apply_calls], [["pre-0", "pre-1"], ["col-a"], ["col-b"]])
        self.assertEqual(apply_calls[0][1], 1)
        self.assertLess(apply_calls[0][2], 1000.40)
        self.assertAlmostEqual(apply_calls[0][2], apply_calls[1][2], places=3)
        self.assertEqual(apply_calls[0][3], 1000)
        self.assertEqual(apply_calls[1][3], 1000)
        self.assertEqual(apply_calls[2][3], 1001)

    def test_cancelled_deferred_fifo_tier_does_not_leak_spacing_to_next_batch(self) -> None:
        base = _load_bundle(speedup=1.0)
        tmpl = base.scheduled_workflows[0]
        t0_items = _synth_items(tmpl, count=2, wall_gap=0.0, creation_gap=5.0, prefix="t0")
        post_items = []
        for idx in range(2):
            man = copy.deepcopy(tmpl.manifest)
            man.setdefault("metadata", {})["name"] = f"post-{idx:03d}"
            post_items.append(
                dataclasses.replace(
                    tmpl,
                    target_wall_offset_sec=0.5 + idx * 0.1,
                    seq=20 + idx,
                    raw_offset_sec=50.0 + idx * 10.0,
                    raw_offset_from_t0_sec=50.0 + idx * 10.0,
                    name=f"post-{idx:03d}",
                    manifest=man,
                    creation_epoch=tmpl.creation_epoch + 50.0 + idx * 10.0,
                    incarnation_index=0,
                )
            )
        cancel_t0_1 = ScheduledWorkflowDeletion(
            target_wall_offset_sec=0.0,
            seq=1,
            raw_offset_sec=0.0,
            raw_offset_from_t0_sec=0.0,
            event_timestamp="2026-01-01T00:00:00Z",
            object_kind=t0_items[1].object_kind,
            name=t0_items[1].name,
            applied_name=t0_items[1].name,
            namespace=t0_items[1].namespace,
            incarnation_index=0,
            manifest_api_version=str(t0_items[1].manifest.get("apiVersion") or ""),
            manifest_kind=str(t0_items[1].manifest.get("kind") or t0_items[1].object_kind),
            delete_reason="cancelled",
        )
        bundle = dataclasses.replace(
            base,
            scheduled_workflows=t0_items + post_items,
            scheduled_infra_events=[],
            scheduled_workflow_updates=[],
            scheduled_workflow_deletions=[cancel_t0_1],
            initial_workloads=[],
            initial_deployments=[],
            initial_pods=[],
        )

        fake_now = [1000.85]
        created: list[tuple[str, int]] = []

        def fake_apply(docs, max_workers=24):
            for d in docs:
                created.append((d["metadata"]["name"], int(fake_now[0])))

        mgr = _FakeMgr()
        with (
            mock.patch.object(mgr, "apply_manifests_parallel", side_effect=fake_apply),
            mock.patch.object(
                replay,
                "query_cluster_status",
                return_value=ClusterStatusSnapshot(workloads_query_ok=True),
            ),
            mock.patch("runner.replay.time.monotonic", side_effect=lambda: fake_now[0]),
            mock.patch("runner.replay.time.time", side_effect=lambda: fake_now[0]),
            mock.patch(
                "runner.replay.time.sleep",
                side_effect=lambda s: fake_now.__setitem__(0, fake_now[0] + max(0.0, float(s))),
            ),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            run_scenario_replay(
                mgr=mgr,
                bundle=bundle,
                speedup=1.0,
                max_run_seconds=20.0,
                status_interval_sec=5.0,
                wait_for_completion=False,
            )

        self.assertEqual([c[0] for c in created], ["t0-000", "post-000", "post-001"])
        # Both post items run together without extra 1s FIFO spacing once t0-001 was cancelled.
        self.assertEqual(created[1][1], created[2][1])


class BootstrapSetupLagCompensationTests(unittest.TestCase):
    """R8: measure finite t0 bootstrap setup lag and compensate finite t0 pods before replay starts."""

    def _make_initial_pod(self, name: str, remaining_sec: float, is_infinite: bool) -> InitialPodSpec:
        return InitialPodSpec(
            name=name,
            namespace="ai-lab",
            placement="node-0",
            resource_request={"cpu": "1"},
            remaining_duration_sec=remaining_sec,
            is_infinite=is_infinite,
            manifest={
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": name,
                    "namespace": "ai-lab",
                    "annotations": {"kueue-optimizer.gke.io/pod-execution-time-seconds": f"{remaining_sec:g}"},
                },
                "spec": {"nodeName": "node-0", "containers": [{"name": "pause", "image": "pause:3.10"}]},
            },
        )

    def test_apply_scenario_queues_returns_timestamp_only_for_finite_t0_pods(self) -> None:
        cq_doc = {"apiVersion": "kueue.x-k8s.io/v1beta2", "kind": "ClusterQueue", "metadata": {"name": "cq-1"}}
        lq_doc = {
            "apiVersion": "kueue.x-k8s.io/v1beta2",
            "kind": "LocalQueue",
            "metadata": {"name": "lq-1", "namespace": "ai-lab"},
            "spec": {"clusterQueue": "cq-1"},
        }

        def active_responder(method, path, _body):
            if method == "GET" and path.endswith("/clusterqueues"):
                return 200, json.dumps(
                    {"items": [{"metadata": {"name": "cq-1"}, "status": {"conditions": [{"type": "Active", "status": "True"}]}}]}
                ).encode("utf-8")
            if method == "GET" and path.endswith("/localqueues"):
                return 200, json.dumps(
                    {
                        "items": [
                            {
                                "metadata": {"name": "lq-1", "namespace": "ai-lab"},
                                "status": {"conditions": [{"type": "Active", "status": "True"}]},
                            }
                        ]
                    }
                ).encode("utf-8")
            return 200, b'{"items": []}'

        # Infinite-only pod -> returns None
        mgr_inf = _FakeMgr(responder=active_responder)
        inf_pod = self._make_initial_pod("inf-p", 86400.0, is_infinite=True)
        with contextlib.redirect_stdout(io.StringIO()):
            ts_inf = apply_scenario_queues_and_wait(mgr_inf, [cq_doc, lq_doc], initial_pods=[inf_pod])
        self.assertIsNone(ts_inf)

        # Finite pod -> returns monotonic timestamp captured after wait_for_pods_running and BEFORE CQ activation
        mgr_fin = _FakeMgr(responder=active_responder)
        fin_pod = self._make_initial_pod("fin-p", 10.0, is_infinite=False)
        fake_clk = [500.0]

        def _tick_after_wait(keys, timeout_sec: float = 90.0, require_ready: bool = False) -> None:
            mgr_fin.events.append(f"wait_pods:{len(keys)}")
            fake_clk[0] = 500.25

        with (
            mock.patch.object(mgr_fin, "wait_for_pods_running", side_effect=_tick_after_wait),
            mock.patch("runner.replay.time.monotonic", side_effect=lambda: fake_clk[0]),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            ts_fin = apply_scenario_queues_and_wait(mgr_fin, [cq_doc, lq_doc], initial_pods=[fin_pod])
        self.assertEqual(ts_fin, 500.25)
        # Verify bootstrap ordering invariant: pinned pod applied and waited BEFORE ClusterQueue
        self.assertLess(mgr_fin.events.index("apply:Pod:fin-p"), mgr_fin.events.index("apply:ClusterQueue:cq-1"))
        self.assertLess(mgr_fin.events.index("wait_pods:1"), mgr_fin.events.index("apply:ClusterQueue:cq-1"))

    def test_apply_scenario_queues_holds_finite_t0_pods_without_mutating_manifest(self) -> None:
        cq_doc = {"apiVersion": "kueue.x-k8s.io/v1beta2", "kind": "ClusterQueue", "metadata": {"name": "cq-1"}}
        lq_doc = {
            "apiVersion": "kueue.x-k8s.io/v1beta2",
            "kind": "LocalQueue",
            "metadata": {"name": "lq-1", "namespace": "ai-lab"},
            "spec": {"clusterQueue": "cq-1"},
        }

        def active_responder(method, path, _body):
            if method == "GET" and path.endswith("/clusterqueues"):
                return 200, json.dumps(
                    {"items": [{"metadata": {"name": "cq-1"}, "status": {"conditions": [{"type": "Active", "status": "True"}]}}]}
                ).encode("utf-8")
            if method == "GET" and path.endswith("/localqueues"):
                return 200, json.dumps(
                    {
                        "items": [
                            {
                                "metadata": {"name": "lq-1", "namespace": "ai-lab"},
                                "status": {"conditions": [{"type": "Active", "status": "True"}]},
                            }
                        ]
                    }
                ).encode("utf-8")
            return 200, b'{"items": []}'

        fin_pod = self._make_initial_pod("fin-held", 3.5, is_infinite=False)
        inf_pod = self._make_initial_pod("inf-held", 864000.0, is_infinite=True)
        orig_manifest_copy = copy.deepcopy(fin_pod.manifest)

        mgr = _FakeMgr(responder=active_responder)
        with contextlib.redirect_stdout(io.StringIO()):
            ts_fin = apply_scenario_queues_and_wait(
                mgr,
                [cq_doc, lq_doc],
                initial_pods=[fin_pod, inf_pod],
                hold_finite_t0_pods=True,
            )

        self.assertIsNotNone(ts_fin)
        # Original InitialPodSpec.manifest must remain completely unmutated (preserving --dry-run output).
        self.assertEqual(fin_pod.manifest, orig_manifest_copy)
        applied_by_name = {
            (d.get("metadata") or {}).get("name"): d for d in mgr.applied_docs if d.get("kind") == "Pod"
        }
        fin_anns = applied_by_name["fin-held"]["metadata"]["annotations"]
        self.assertEqual(fin_anns["kueue-optimizer.gke.io/pod-execution-time-seconds"], "86400")
        self.assertNotIn("kueue-optimizer.gke.io/long-running-pod", fin_anns)
        inf_anns = applied_by_name["inf-held"]["metadata"]["annotations"]
        self.assertEqual(inf_anns["kueue-optimizer.gke.io/pod-execution-time-seconds"], "864000")

    def test_run_scenario_replay_hold_and_release_preserves_exact_remaining_duration(self) -> None:
        base = _load_bundle()
        # Both tiny lag (0.01s) and lag > remaining (4.0s > 3.0s) MUST release held pods to exact remaining_duration_sec.
        cases = [
            ("tiny_lag_still_releases_hold", 0.01, 10.0, "10"),
            ("normal_lag_exact_remaining_not_sum", 2.0, 10.0, "10"),
            ("lag_exceeds_remaining_exact_remaining_no_warn", 4.0, 3.0, "3"),
        ]
        for label, lag_sec, rem_sec, expected_exec in cases:
            with self.subTest(case=label):
                fin_wl_pod = self._make_initial_pod("t0-wl-pod", rem_sec, is_infinite=False)
                fin_infra_pod = self._make_initial_pod("t0-infra-pod", rem_sec, is_infinite=False)
                inf_infra_pod = self._make_initial_pod("t0-inf-pod", 86400.0, is_infinite=True)
                iwl = InitialWorkloadPodGroup(
                    workload_name="wl-t0",
                    namespace="ai-lab",
                    local_queue_name="lq-1",
                    cluster_queue_name="cq-1",
                    owner_kind="Job",
                    owner_name="j-t0",
                    priority_class_name="",
                    priority=None,
                    is_tas=False,
                    pods=[fin_wl_pod],
                    workload_manifest={"metadata": {"name": "wl-t0", "namespace": "ai-lab"}},
                    workload_status={},
                )
                bundle = dataclasses.replace(
                    base,
                    scheduled_workflows=[],
                    scheduled_infra_events=[],
                    scheduled_workflow_updates=[],
                    scheduled_workflow_deletions=[],
                    initial_workloads=[iwl],
                    initial_deployments=[],
                    initial_pods=[fin_infra_pod, inf_infra_pod],
                )
                mgr = _FakeMgr()
                out_buf = io.StringIO()
                t_start_mono = 1000.0
                with (
                    mock.patch("runner.replay.time.monotonic", return_value=t_start_mono),
                    mock.patch.object(
                        replay,
                        "query_cluster_status",
                        return_value=ClusterStatusSnapshot(
                            workloads_query_ok=True,
                            present_workload_keys={("ai-lab", "wl-t0")},
                            finished_workload_keys={("ai-lab", "wl-t0")},
                        ),
                    ),
                    contextlib.redirect_stdout(out_buf),
                ):
                    res = run_scenario_replay(
                        mgr=mgr,
                        bundle=bundle,
                        speedup=1.0,
                        max_run_seconds=5.0,
                        status_interval_sec=1.0,
                        wait_for_completion=False,
                        t0_pods_started_mono=t_start_mono - lag_sec,
                        hold_finite_t0_pods=True,
                    )

                self.assertAlmostEqual(res.bootstrap_lag_sec, lag_sec, places=3)
                self.assertEqual(res.compensated_t0_pods, 2)
                pod_patches = [
                    (p, b) for m, p, b, _ in mgr.requests if m == "PATCH" and "/pods/" in p
                ]
                patched_paths = {p for p, _ in pod_patches}
                self.assertEqual(
                    patched_paths,
                    {"/api/v1/namespaces/ai-lab/pods/t0-wl-pod", "/api/v1/namespaces/ai-lab/pods/t0-infra-pod"},
                )
                for _path, body in pod_patches:
                    anns = body["metadata"]["annotations"]
                    self.assertEqual(anns["kueue-optimizer.gke.io/pod-execution-time-seconds"], expected_exec)
                    # Section 5.4 cfg_dur reader must recover exact remaining_duration_sec, never remaining + lag.
                    cfg_dur = parse_positive_duration_annotation(
                        anns, "kueue-optimizer.gke.io/pod-execution-time-seconds"
                    )
                    self.assertAlmostEqual(cfg_dur, rem_sec, places=3)
                    self.assertEqual(
                        anns["kueue-optimizer.gke.io/bootstrap-lag-compensation-sec"],
                        f"{lag_sec:.3f}",
                    )
                self.assertNotIn("WARN:", out_buf.getvalue())

    def test_run_scenario_replay_non_held_fallback_checks_phase_and_skips_succeeded(self) -> None:
        base = _load_bundle()
        cases = [
            ("tiny_lag_skipped", 0.01, 10.0, {}, 0, set(), False),
            (
                "running_pods_reset_to_exact_remaining",
                2.0,
                10.0,
                {"t0-wl-pod": "Running", "t0-infra-pod": "Running"},
                2,
                {"/api/v1/namespaces/ai-lab/pods/t0-wl-pod", "/api/v1/namespaces/ai-lab/pods/t0-infra-pod"},
                False,
            ),
            (
                "already_succeeded_pod_skipped_and_warns",
                4.0,
                3.0,
                {"t0-wl-pod": "Succeeded", "t0-infra-pod": "Running"},
                1,
                {"/api/v1/namespaces/ai-lab/pods/t0-infra-pod"},
                True,
            ),
        ]
        for label, lag_sec, rem_sec, phases_by_name, exp_comp, exp_paths, expect_warn in cases:
            with self.subTest(case=label):
                fin_wl_pod = self._make_initial_pod("t0-wl-pod", rem_sec, is_infinite=False)
                fin_infra_pod = self._make_initial_pod("t0-infra-pod", rem_sec, is_infinite=False)
                inf_infra_pod = self._make_initial_pod("t0-inf-pod", 86400.0, is_infinite=True)
                iwl = InitialWorkloadPodGroup(
                    workload_name="wl-t0",
                    namespace="ai-lab",
                    local_queue_name="lq-1",
                    cluster_queue_name="cq-1",
                    owner_kind="Job",
                    owner_name="j-t0",
                    priority_class_name="",
                    priority=None,
                    is_tas=False,
                    pods=[fin_wl_pod],
                    workload_manifest={"metadata": {"name": "wl-t0", "namespace": "ai-lab"}},
                    workload_status={},
                )
                bundle = dataclasses.replace(
                    base,
                    scheduled_workflows=[],
                    scheduled_infra_events=[],
                    scheduled_workflow_updates=[],
                    scheduled_workflow_deletions=[],
                    initial_workloads=[iwl],
                    initial_deployments=[],
                    initial_pods=[fin_infra_pod, inf_infra_pod],
                )

                def pod_phase_responder(method, path, _body, _phases=phases_by_name):
                    if method == "GET" and "/pods/" in path:
                        pod_nm = path.rsplit("/", 1)[-1]
                        ph = _phases.get(pod_nm, "Running")
                        return 200, json.dumps({"status": {"phase": ph}}).encode("utf-8")
                    if method == "PATCH" and "/pods/" in path:
                        pod_nm = path.rsplit("/", 1)[-1]
                        ph = _phases.get(pod_nm, "Running")
                        return 200, json.dumps({"status": {"phase": ph}}).encode("utf-8")
                    return 200, b'{"items": []}'

                mgr = _FakeMgr(responder=pod_phase_responder)
                out_buf = io.StringIO()
                t_start_mono = 1000.0
                with (
                    mock.patch("runner.replay.time.monotonic", return_value=t_start_mono),
                    mock.patch.object(
                        replay,
                        "query_cluster_status",
                        return_value=ClusterStatusSnapshot(
                            workloads_query_ok=True,
                            present_workload_keys={("ai-lab", "wl-t0")},
                            finished_workload_keys={("ai-lab", "wl-t0")},
                        ),
                    ),
                    contextlib.redirect_stdout(out_buf),
                ):
                    res = run_scenario_replay(
                        mgr=mgr,
                        bundle=bundle,
                        speedup=1.0,
                        max_run_seconds=5.0,
                        status_interval_sec=1.0,
                        wait_for_completion=False,
                        t0_pods_started_mono=t_start_mono - lag_sec,
                        hold_finite_t0_pods=False,
                    )

                self.assertAlmostEqual(res.bootstrap_lag_sec, lag_sec, places=3)
                self.assertEqual(res.compensated_t0_pods, exp_comp)
                pod_patches = [
                    (p, b) for m, p, b, _ in mgr.requests if m == "PATCH" and "/pods/" in p
                ]
                self.assertEqual({p for p, _ in pod_patches}, exp_paths)
                for _path, body in pod_patches:
                    anns = body["metadata"]["annotations"]
                    self.assertEqual(
                        anns["kueue-optimizer.gke.io/pod-execution-time-seconds"],
                        f"{int(rem_sec)}",
                    )
                    self.assertEqual(
                        anns["kueue-optimizer.gke.io/bootstrap-lag-compensation-sec"],
                        f"{lag_sec:.3f}",
                    )
                if expect_warn:
                    self.assertIn("WARN:", out_buf.getvalue())
                else:
                    self.assertNotIn("WARN:", out_buf.getvalue())


class ControlPlaneHealthCheckTests(unittest.TestCase):
    """R10: detect chamber control-plane pod restarts, replacements, and unreadiness."""

    def _mk_pod(
        self,
        name: str,
        component: str,
        *,
        ready: bool = True,
        restarts: int = 0,
        uid: str = "",
        phase: str | None = None,
    ) -> dict:
        eff_phase = phase if phase is not None else ("Running" if ready else "Pending")
        return {
            "metadata": {
                "name": name,
                "uid": uid or f"uid-{name}",
                "labels": {"app.kubernetes.io/component": component},
            },
            "status": {
                "phase": eff_phase,
                "conditions": [{"type": "Ready", "status": "True" if ready else "False"}],
                "containerStatuses": [{"name": "main", "restartCount": restarts}],
            },
        }

    def _all_eight_healthy_pods(self) -> list[dict]:
        return [
            self._mk_pod("kwok-etcd-abc", "etcd", ready=True, restarts=0, uid="u-etcd-1"),
            self._mk_pod("kwok-kube-apiserver-abc", "kube-apiserver", ready=True, restarts=0, uid="u-api-1"),
            self._mk_pod(
                "kwok-kube-controller-manager-abc", "kube-controller-manager", ready=True, restarts=0, uid="u-kcm-1"
            ),
            self._mk_pod("kwok-kube-scheduler-abc", "kube-scheduler", ready=True, restarts=0, uid="u-sched-1"),
            self._mk_pod("kwok-controller-abc", "kwok-controller", ready=True, restarts=0, uid="u-kwok-1"),
            self._mk_pod(
                "jobset-controller-manager-abc",
                "jobset-controller-manager",
                ready=True,
                restarts=0,
                uid="u-jobset-1",
            ),
            self._mk_pod(
                "kueue-controller-manager-abc",
                "kueue-controller-manager",
                ready=True,
                restarts=0,
                uid="u-kueue-1",
            ),
            self._mk_pod("prometheus-abc", "prometheus", ready=True, restarts=0, uid="u-prom-1"),
        ]

    def test_core_health_components_cover_all_eight_runtime_deployments(self) -> None:
        self.assertEqual(
            set(CHAMBER_CORE_HEALTH_COMPONENTS.values()),
            {
                "kwok-etcd",
                "kwok-kube-apiserver",
                "kwok-kube-controller-manager",
                "kwok-kube-scheduler",
                "kwok-controller",
                "jobset-controller-manager",
                "kueue-controller-manager",
                "prometheus",
            },
        )
        # Verify every configured component label exists in deploy/base/*.yaml
        base_yaml_text = "\n".join(
            p.read_text(encoding="utf-8") for p in sorted((REPO_ROOT / "deploy" / "base").glob("*.yaml"))
        )
        for comp_label in CHAMBER_CORE_HEALTH_COMPONENTS:
            self.assertIn(f"app.kubernetes.io/component: {comp_label}", base_yaml_text)

    def test_chamber_manager_health_snapshot_and_verifier_table(self) -> None:
        base_pods = self._all_eight_healthy_pods()

        def _with_replaced(idx: int, new_pod: dict) -> list[dict]:
            out = list(base_pods)
            out[idx] = new_pod
            return out

        cases = [
            ("all_healthy", base_pods, 0, True, ()),
            (
                "etcd_restarted",
                _with_replaced(0, self._mk_pod("kwok-etcd-abc", "etcd", ready=True, restarts=1, uid="u-etcd-1")),
                0,
                False,
                ("kwok-etcd restarted",),
            ),
            (
                "kueue_pod_replaced",
                _with_replaced(
                    6,
                    self._mk_pod(
                        "kueue-controller-manager-xyz",
                        "kueue-controller-manager",
                        ready=True,
                        restarts=0,
                        uid="u-kueue-2",
                    ),
                ),
                0,
                False,
                ("kueue-controller-manager pod replaced",),
            ),
            (
                "pod_replaced_and_restarted_reports_both",
                _with_replaced(
                    4,
                    self._mk_pod(
                        "kwok-controller-xyz",
                        "kwok-controller",
                        ready=True,
                        restarts=2,
                        uid="u-kwok-2",
                    ),
                ),
                0,
                False,
                ("kwok-controller restarted", "kwok-controller pod replaced"),
            ),
            (
                "apiserver_unready",
                _with_replaced(
                    1,
                    self._mk_pod("kwok-kube-apiserver-abc", "kube-apiserver", ready=False, restarts=0, uid="u-api-1"),
                ),
                0,
                False,
                ("kwok-kube-apiserver unready",),
            ),
            (
                "prometheus_restarted",
                _with_replaced(7, self._mk_pod("prometheus-abc", "prometheus", ready=True, restarts=1, uid="u-prom-1")),
                0,
                False,
                ("prometheus restarted",),
            ),
            ("kubectl_failed", [], 1, False, ("failed to query",)),
        ]

        mgr = ChamberManager(chamber_namespace="kueue-optimizer-chamber-health-ut", apiserver_port=16498)
        try:
            with mock.patch.object(
                mgr,
                "host_kubectl",
                return_value=subprocess.CompletedProcess(
                    args=[], returncode=0, stdout=json.dumps({"items": base_pods}), stderr=""
                ),
            ):
                ok_base, issues_base, baseline = verify_chamber_control_plane_health(mgr, retries=0)
            self.assertTrue(ok_base, issues_base)

            for label, cur_pods, rc, expect_ok, expect_substrs in cases:
                with self.subTest(case=label):
                    proc = subprocess.CompletedProcess(
                        args=[],
                        returncode=rc,
                        stdout=json.dumps({"items": cur_pods}) if rc == 0 else "",
                        stderr="connection refused" if rc != 0 else "",
                    )
                    with mock.patch.object(mgr, "host_kubectl", return_value=proc):
                        ok, issues, _snap = verify_chamber_control_plane_health(mgr, baseline=baseline, retries=0)
                    self.assertEqual(ok, expect_ok, issues)
                    for substr in expect_substrs:
                        self.assertTrue(any(substr in msg for msg in issues), issues)
        finally:
            mgr.close()

    def test_health_snapshot_handles_timeout_oserror_empty_components_and_reused_evicted_pods(self) -> None:
        mgr = ChamberManager(chamber_namespace="kueue-optimizer-chamber-health-edge", apiserver_port=16497)
        try:
            # 1. TimeoutExpired and OSError return query_ok=False instead of raising.
            for exc in (
                subprocess.TimeoutExpired(cmd=["kubectl"], timeout=15.0),
                OSError("kubectl binary missing"),
            ):
                with self.subTest(exc=type(exc).__name__):
                    with mock.patch.object(mgr, "host_kubectl", side_effect=exc):
                        snap = mgr.check_chamber_health_snapshot()
                    self.assertFalse(snap.query_ok)
                    self.assertTrue(snap.error)

            # 2. Explicit empty components={} is honored (not replaced with CHAMBER_CORE_HEALTH_COMPONENTS).
            with mock.patch.object(
                mgr,
                "host_kubectl",
                return_value=subprocess.CompletedProcess(args=[], returncode=0, stdout='{"items": []}', stderr=""),
            ):
                empty_snap = mgr.check_chamber_health_snapshot(components={})
            self.assertTrue(empty_snap.query_ok)
            self.assertEqual(empty_snap.components, {})

            # 3. Reused chamber with baseline restartCount > 0 and an old terminal Failed (Evicted) Pod is healthy.
            reused_pods = self._all_eight_healthy_pods()
            reused_pods[0] = self._mk_pod("kwok-etcd-abc", "etcd", ready=True, restarts=3, uid="u-etcd-1")
            reused_pods.append(
                self._mk_pod(
                    "kwok-etcd-old-evicted",
                    "etcd",
                    ready=False,
                    restarts=0,
                    uid="u-etcd-evicted",
                    phase="Failed",
                )
            )
            with mock.patch.object(
                mgr,
                "host_kubectl",
                return_value=subprocess.CompletedProcess(
                    args=[], returncode=0, stdout=json.dumps({"items": reused_pods}), stderr=""
                ),
            ):
                ok_base, issues_base, base_snap = verify_chamber_control_plane_health(mgr, retries=0)
                self.assertTrue(ok_base, issues_base)
                self.assertEqual(base_snap.components["kwok-etcd"].total_pods, 1)
                self.assertEqual(base_snap.components["kwok-etcd"].ready_pods, 1)
                self.assertEqual(base_snap.components["kwok-etcd"].restart_count, 3)
                # Same baseline state at replay end remains healthy.
                ok_end, issues_end, _ = verify_chamber_control_plane_health(mgr, baseline=base_snap, retries=0)
                self.assertTrue(ok_end, issues_end)

            # 4. Transient query_ok=False recovers via the 1 short retry.
            transient_mgr = _FakeMgr(
                health_snapshots=[
                    ChamberHealthSnapshot(query_ok=False, error="temporary EOF"),
                    _healthy_snapshot(),
                ]
            )
            with mock.patch("chamber.lifecycle.time.sleep") as m_sleep:
                ok_retry, issues_retry, _ = verify_chamber_control_plane_health(
                    transient_mgr, retries=1, retry_delay_sec=0.5
                )
            self.assertTrue(ok_retry, issues_retry)
            self.assertEqual(transient_mgr.health_calls, 2)
            m_sleep.assert_called_once_with(0.5)
        finally:
            mgr.close()

    def test_signal_handlers_restored_on_startup_exception(self) -> None:
        base = _load_bundle()
        bundle = dataclasses.replace(
            base,
            scheduled_workflows=[],
            scheduled_infra_events=[],
            scheduled_workflow_updates=[],
            scheduled_workflow_deletions=[],
            initial_workloads=[],
            initial_deployments=[],
            initial_pods=[],
        )
        orig_int = signal.getsignal(signal.SIGINT)
        orig_term = signal.getsignal(signal.SIGTERM)
        mgr = _FakeMgr()
        with (
            mock.patch.object(
                mgr,
                "check_chamber_health_snapshot",
                side_effect=RuntimeError("unexpected startup boom"),
            ),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            with self.assertRaisesRegex(RuntimeError, "unexpected startup boom"):
                run_scenario_replay(
                    mgr=mgr,
                    bundle=bundle,
                    speedup=1.0,
                    max_run_seconds=5.0,
                    status_interval_sec=1.0,
                    wait_for_completion=False,
                    check_control_plane_health=True,
                )
        self.assertIs(signal.getsignal(signal.SIGINT), orig_int)
        self.assertIs(signal.getsignal(signal.SIGTERM), orig_term)

    def test_pre_replay_unhealthy_skips_t0_release_and_serving_sync(self) -> None:
        base = _load_bundle()
        fin_pod = InitialPodSpec(
            name="t0-fin-pod",
            namespace="ai-lab",
            placement="node-0",
            resource_request={"cpu": "1"},
            remaining_duration_sec=5.0,
            is_infinite=False,
            manifest={
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": "t0-fin-pod",
                    "namespace": "ai-lab",
                    "annotations": {"kueue-optimizer.gke.io/pod-execution-time-seconds": "5"},
                },
                "spec": {"nodeName": "node-0", "containers": [{"name": "pause", "image": "pause:3.10"}]},
            },
        )
        bundle = dataclasses.replace(
            base,
            scheduled_workflows=[],
            scheduled_infra_events=[],
            scheduled_workflow_updates=[],
            scheduled_workflow_deletions=[],
            initial_workloads=[],
            initial_deployments=[],
            initial_pods=[fin_pod],
        )
        mgr = _FakeMgr(health_snapshots=[_healthy_snapshot(api_ready=0)])
        with (
            mock.patch("chamber.lifecycle.time.sleep"),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            res = run_scenario_replay(
                mgr=mgr,
                bundle=bundle,
                speedup=1.0,
                max_run_seconds=5.0,
                status_interval_sec=1.0,
                wait_for_completion=False,
                t0_pods_started_mono=time.monotonic() - 1.0,
                hold_finite_t0_pods=True,
                check_control_plane_health=True,
            )
        self.assertEqual(res.stop_reason, "control_plane_unhealthy")
        self.assertEqual(res.exit_code, 1)
        self.assertEqual(res.compensated_t0_pods, 0)
        # Zero KWOK apiserver requests (neither t0 pod PATCHes nor finally _sync_serving_workloads GETs).
        self.assertEqual(mgr.requests, [])

    def test_sigint_racing_health_check_exits_interrupted_without_unhealthy_error(self) -> None:
        base = _load_bundle()
        bundle = dataclasses.replace(
            base,
            scheduled_workflows=[],
            scheduled_infra_events=[],
            scheduled_workflow_updates=[],
            scheduled_workflow_deletions=[],
            initial_workloads=[],
            initial_deployments=[],
            initial_pods=[],
        )
        mgr = _FakeMgr()

        def _sigint_during_health(*_a, **_kw):
            os.kill(os.getpid(), signal.SIGINT)
            return ChamberHealthSnapshot(query_ok=False, error="kubectl terminated by signal")

        out_buf = io.StringIO()
        with (
            mock.patch.object(mgr, "check_chamber_health_snapshot", side_effect=_sigint_during_health),
            contextlib.redirect_stdout(out_buf),
        ):
            res = run_scenario_replay(
                mgr=mgr,
                bundle=bundle,
                speedup=1.0,
                max_run_seconds=5.0,
                status_interval_sec=1.0,
                wait_for_completion=False,
                check_control_plane_health=True,
            )
        self.assertEqual(res.stop_reason, "interrupted")
        self.assertEqual(res.signal_num, int(signal.SIGINT))
        self.assertEqual(res.exit_code, 130)
        self.assertEqual(res.health_issues, ())
        self.assertNotIn("control plane unhealthy", out_buf.getvalue().lower())

    def test_mid_run_and_end_of_run_restarts_fail_replay_with_nonzero_exit(self) -> None:
        base = _load_bundle()
        tmpl = base.scheduled_workflows[0]
        items = _synth_items(tmpl, count=2, wall_gap=0.05, creation_gap=1.0)
        bundle = dataclasses.replace(
            base,
            scheduled_workflows=items,
            scheduled_infra_events=[],
            scheduled_workflow_updates=[],
            scheduled_workflow_deletions=[],
            initial_workloads=[],
            initial_deployments=[],
            initial_pods=[],
        )

        # Case 1: kwok-etcd restarts mid-run (caught by periodic health check)
        mgr_mid = _FakeMgr(
            health_snapshots=[
                _healthy_snapshot(etcd_restarts=0),
                _healthy_snapshot(etcd_restarts=1),
            ]
        )
        fake_now = [1000.0]
        with (
            mock.patch.object(
                replay,
                "query_cluster_status",
                return_value=ClusterStatusSnapshot(workloads_query_ok=True, workloads_admitted=2),
            ),
            mock.patch("runner.replay.time.monotonic", side_effect=lambda: fake_now[0]),
            mock.patch("runner.replay.time.time", side_effect=lambda: fake_now[0]),
            mock.patch(
                "runner.replay.time.sleep",
                side_effect=lambda s: fake_now.__setitem__(0, fake_now[0] + max(0.0, float(s))),
            ),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            res_mid = run_scenario_replay(
                mgr=mgr_mid,
                bundle=bundle,
                speedup=1.0,
                max_run_seconds=30.0,
                status_interval_sec=1.0,
                wait_for_completion=True,
                check_control_plane_health=True,
                health_check_interval_sec=1.0,
            )
        self.assertEqual(res_mid.stop_reason, "control_plane_unhealthy")
        self.assertTrue(res_mid.control_plane_unhealthy)
        self.assertEqual(res_mid.exit_code, 1)
        self.assertTrue(any("kwok-etcd restarted" in msg for msg in res_mid.health_issues), res_mid.health_issues)

        # Case 2: kueue-controller-manager restarts before replay end (caught by post-replay check)
        mgr_end = _FakeMgr(
            health_snapshots=[
                _healthy_snapshot(kueue_restarts=0),
                _healthy_snapshot(kueue_restarts=1),
            ]
        )
        with (
            mock.patch.object(
                replay,
                "query_cluster_status",
                return_value=ClusterStatusSnapshot(workloads_query_ok=True),
            ),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            res_end = run_scenario_replay(
                mgr=mgr_end,
                bundle=bundle,
                speedup=1.0,
                max_run_seconds=10.0,
                status_interval_sec=0.05,
                wait_for_completion=False,
                check_control_plane_health=True,
                health_check_interval_sec=60.0,
            )
        self.assertEqual(res_end.stop_reason, "control_plane_unhealthy")
        self.assertEqual(res_end.exit_code, 1)
        self.assertTrue(
            any("kueue-controller-manager restarted" in msg for msg in res_end.health_issues),
            res_end.health_issues,
        )

        # Case 3: default check_control_plane_health=False never calls health snapshot
        mgr_off = _FakeMgr(health_snapshots=[_healthy_snapshot(etcd_ready=0)])
        with (
            mock.patch.object(
                replay,
                "query_cluster_status",
                return_value=ClusterStatusSnapshot(workloads_query_ok=True),
            ),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            res_off = run_scenario_replay(
                mgr=mgr_off,
                bundle=bundle,
                speedup=1.0,
                max_run_seconds=10.0,
                status_interval_sec=0.05,
                wait_for_completion=False,
            )
        self.assertEqual(res_off.stop_reason, "completed")
        self.assertEqual(res_off.exit_code, 0)
        self.assertEqual(mgr_off.health_calls, 0)

    def test_testrunner_main_propagates_unhealthy_exit_code_and_skips_metrics(self) -> None:
        unhealthy_res = ReplayResult(
            start_epoch=100.0,
            end_epoch=105.0,
            submitted_count=2,
            total_pods=4,
            stop_reason="control_plane_unhealthy",
            bootstrap_lag_sec=1.25,
            health_issues=("kwok-etcd restarted during replay (restartCount 0 -> 1)",),
            compensated_t0_pods=3,
        )
        demo_dir = REPO_ROOT / "testscenarios" / "demo"
        out_buf = io.StringIO()
        with (
            mock.patch.object(testrunner, "managed_chamber") as m_mc,
            mock.patch.object(ChamberManager, "ensure_kwok_connection"),
            mock.patch.object(ChamberManager, "wait_for_crds"),
            mock.patch.object(ChamberManager, "apply_kwok_stages"),
            mock.patch.object(ChamberManager, "ensure_kueue_webhook_namespace_exclusions"),
            mock.patch.object(ChamberManager, "sync_kueue_configuration_to_kwok_apiserver"),
            mock.patch.object(ChamberManager, "clear_workload_namespace"),
            mock.patch.object(ChamberManager, "sync_kwok_nodes"),
            mock.patch.object(ChamberManager, "run_kueue_metrics_report") as m_metrics,
            mock.patch.object(testrunner, "apply_scenario_queues_and_wait", return_value=98.75) as m_apply_q,
            mock.patch.object(testrunner, "run_scenario_replay", return_value=unhealthy_res) as m_replay,
            mock.patch.object(
                testrunner,
                "query_cluster_status",
                return_value=ClusterStatusSnapshot(workloads_query_ok=True),
            ) as m_query,
            contextlib.redirect_stdout(out_buf),
        ):
            m_mc.return_value.__enter__.return_value = None
            m_mc.return_value.__exit__.return_value = False
            rc = testrunner.main([str(demo_dir), "--namespace", "kueue-optimizer-chamber-health-main"])

        self.assertEqual(rc, 1)
        m_metrics.assert_not_called()
        m_query.assert_not_called()
        self.assertTrue(m_apply_q.call_args.kwargs["hold_finite_t0_pods"])
        replay_kw = m_replay.call_args.kwargs
        self.assertEqual(replay_kw["t0_pods_started_mono"], 98.75)
        self.assertTrue(replay_kw["hold_finite_t0_pods"])
        self.assertTrue(replay_kw["check_control_plane_health"])
        stdout_txt = out_buf.getvalue()
        self.assertIn("control_plane_unhealthy", stdout_txt)
        self.assertIn("kwok-etcd restarted during replay", stdout_txt)
        self.assertIn("Bootstrap Setup Lag     : 1.25s (compensated on 3 finite t0 pod(s))", stdout_txt)

        # Also verify bool return from apply_scenario_queues_and_wait is ignored and zero compensated pods omits note.
        no_comp_res = ReplayResult(
            start_epoch=100.0,
            end_epoch=105.0,
            submitted_count=2,
            total_pods=4,
            stop_reason="completed",
            bootstrap_lag_sec=0.80,
            compensated_t0_pods=0,
        )
        out_buf2 = io.StringIO()
        with (
            mock.patch.object(testrunner, "managed_chamber") as m_mc2,
            mock.patch.object(ChamberManager, "ensure_kwok_connection"),
            mock.patch.object(ChamberManager, "wait_for_crds"),
            mock.patch.object(ChamberManager, "apply_kwok_stages"),
            mock.patch.object(ChamberManager, "ensure_kueue_webhook_namespace_exclusions"),
            mock.patch.object(ChamberManager, "sync_kueue_configuration_to_kwok_apiserver"),
            mock.patch.object(ChamberManager, "clear_workload_namespace"),
            mock.patch.object(ChamberManager, "sync_kwok_nodes"),
            mock.patch.object(ChamberManager, "run_kueue_metrics_report"),
            mock.patch.object(testrunner, "apply_scenario_queues_and_wait", return_value=True),
            mock.patch.object(testrunner, "run_scenario_replay", return_value=no_comp_res) as m_replay2,
            mock.patch.object(
                testrunner,
                "query_cluster_status",
                return_value=ClusterStatusSnapshot(workloads_query_ok=True),
            ) as m_query2,
            contextlib.redirect_stdout(out_buf2),
        ):
            m_mc2.return_value.__enter__.return_value = None
            m_mc2.return_value.__exit__.return_value = False
            rc2 = testrunner.main([str(demo_dir), "--namespace", "kueue-optimizer-chamber-health-main2"])

        self.assertEqual(rc2, 0)
        m_query2.assert_called_once()
        self.assertIsNone(m_replay2.call_args.kwargs["t0_pods_started_mono"])
        self.assertIn("Bootstrap Setup Lag     : 0.80s\n", out_buf2.getvalue())
        self.assertNotIn("compensated on", out_buf2.getvalue())


if __name__ == "__main__":
    unittest.main()
