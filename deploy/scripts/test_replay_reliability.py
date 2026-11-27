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

"""Focused unit tests for replay reliability (R1-R5, R9), duration parsing, and dead-branch cleanup."""

from __future__ import annotations

import contextlib
import copy
import dataclasses
import inspect
import io
import json
import os
from pathlib import Path
import shutil
import signal
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from chamber.lifecycle import ChamberManager, ClusterStatusSnapshot
from chamber.parsers import parse_duration_seconds, parse_positive_duration_annotation
from chamber.scenario import index_execution_times
from runner.loader import ScheduledWorkflowDeletion, load_scenario_bundle
import runner.replay as replay
from runner.replay import ReplayResult, _execute_scheduled_infra_event, run_scenario_replay
from runner.validation import _prepare_node_manifest
import testrunner


class _FakeMgr:
    """In-memory ChamberManager stub that records API calls without contacting any cluster."""

    def __init__(self, responder=None) -> None:
        self.t0 = time.monotonic()
        self._responder = responder
        self.applied: list[tuple[str, str]] = []
        self.requests: list[tuple[str, str, dict | None, int]] = []
        self.workload_namespace = "default"
        self.chamber_namespace = "kueue-optimizer-chamber-unit"
        self._retired_serving_workloads: dict = {}

    def ssa_apply_doc(self, doc: dict) -> None:
        self.applied.append((str(doc.get("kind") or ""), str((doc.get("metadata") or {}).get("name") or "")))

    def apply_manifests_parallel(self, docs: list[dict], max_workers: int = 24) -> None:
        for d in docs:
            self.ssa_apply_doc(d)

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


def _load_bundle(scenario_name: str = "st08-single-priority-deep-backlog-waves"):
    return load_scenario_bundle(
        scenario_dir=REPO_ROOT / "testscenarios" / "self-tests" / scenario_name,
        speedup=1.0,
        skip_initial_offset=True,
    )


def _synth_items(template, count: int, wall_gap: float = 0.0, prefix: str = "wf"):
    out = []
    for i in range(count):
        man = copy.deepcopy(template.manifest)
        man.setdefault("metadata", {})["name"] = f"{prefix}-{i:03d}"
        out.append(
            dataclasses.replace(
                template,
                target_wall_offset_sec=i * wall_gap,
                seq=i,
                raw_offset_sec=i * wall_gap,
                raw_offset_from_t0_sec=i * wall_gap,
                name=f"{prefix}-{i:03d}",
                manifest=man,
                creation_epoch=template.creation_epoch + i * wall_gap,
                incarnation_index=0,
            )
        )
    return out


class NodeUpdateAndFailFastTests(unittest.TestCase):
    """Tests for R1 (Node Update labels/allocatable/capacity) and R2 (fail-fast HTTP checks)."""

    def test_node_update_patches_labels_allocatable_and_capacity(self) -> None:
        b61 = _load_bundle("st61-tas-single-node-failure-hotswap-same-rack")
        ie0 = next(ie for ie in b61.scheduled_infra_events if ie.object_kind == "Node")
        node0 = next(n for n in b61.nodes if n["metadata"]["name"] == ie0.name)

        cases = [
            ("healthy_update", False, False, "True", None, True),
            ("unhealthy_not_ready", True, False, "False", "true", True),
            ("no_execute_taint_only", False, True, "True", "true", False),
        ]
        for label, is_not_ready, has_no_exec, ready_st, expected_failed_ann, expect_conditions in cases:
            with self.subTest(case=label):
                scraped = copy.deepcopy(node0)
                scraped["status"]["allocatable"]["google.com/tpu"] = "16"
                scraped["status"]["capacity"]["google.com/tpu"] = "16"
                scraped["metadata"]["labels"]["cloud.google.com/gke-nodepool"] = "resized-pool"
                scraped["status"]["conditions"] = [{"type": "Ready", "status": ready_st}]
                if has_no_exec:
                    scraped.setdefault("spec", {})["taints"] = [
                        {"key": "maintenance", "value": "true", "effect": "NoExecute"}
                    ]
                man = _prepare_node_manifest(scraped, 0)
                ie = dataclasses.replace(
                    ie0,
                    event_type="Update",
                    is_not_ready=is_not_ready,
                    has_no_execute_taint=has_no_exec,
                    ready_status=ready_st,
                    manifest=man,
                    has_explicit_capacity=True,
                )
                mgr = _FakeMgr()
                with contextlib.redirect_stdout(io.StringIO()):
                    _execute_scheduled_infra_event(mgr, b61, ie, 1.0)

                node_patches = [b for m, p, b, _ in mgr.requests if m == "PATCH" and p == f"/api/v1/nodes/{ie.name}"]
                status_patches = [
                    b for m, p, b, _ in mgr.requests if m == "PATCH" and p == f"/api/v1/nodes/{ie.name}/status"
                ]
                self.assertEqual(len(node_patches), 1)
                self.assertEqual(len(status_patches), 1)

                n_body = node_patches[0]
                self.assertEqual(n_body["metadata"]["labels"]["cloud.google.com/gke-nodepool"], "resized-pool")
                self.assertEqual(
                    n_body["metadata"]["annotations"]["kueue-optimizer.gke.io/node-failed"],
                    expected_failed_ann,
                )
                self.assertEqual(n_body["metadata"]["annotations"]["kueue-optimizer.gke.io/node-accel-count"], "16")

                s_body = status_patches[0]["status"]
                self.assertEqual(s_body["allocatable"]["google.com/tpu"], "16")
                self.assertEqual(s_body["capacity"]["google.com/tpu"], "16")
                if expect_conditions:
                    self.assertEqual(s_body["conditions"][0]["status"], ready_st)
                else:
                    self.assertNotIn("conditions", s_body)

    def test_node_update_without_raw_capacity_never_touches_capacity(self) -> None:
        """A taint-only or condition-only Node Update must not push the synthetic default capacity."""
        b61 = _load_bundle("st61-tas-single-node-failure-hotswap-same-rack")
        ie0 = next(ie for ie in b61.scheduled_infra_events if ie.object_kind == "Node")
        node0 = next(n for n in b61.nodes if n["metadata"]["name"] == ie0.name)
        res_anns = (
            "kueue-optimizer.gke.io/node-cpu",
            "kueue-optimizer.gke.io/node-memory",
            "kueue-optimizer.gke.io/node-pods",
            "kueue-optimizer.gke.io/node-accel-resource",
            "kueue-optimizer.gke.io/node-accel-count",
        )
        cases = [
            ("healthy_update", False, False, "True"),
            ("unhealthy_not_ready", True, False, "False"),
            ("no_execute_taint_only", False, True, "True"),
        ]
        for label, is_not_ready, has_no_exec, ready_st in cases:
            with self.subTest(case=label):
                scraped = copy.deepcopy(node0)
                scraped["status"] = {"conditions": [{"type": "Ready", "status": ready_st}]}
                if has_no_exec:
                    scraped.setdefault("spec", {})["taints"] = [
                        {"key": "maintenance", "value": "true", "effect": "NoExecute"}
                    ]
                man = _prepare_node_manifest(scraped, 0)
                # The prepared manifest carries the synthetic default, not the node's real 16 CPU / 4 TPU.
                self.assertEqual(man["status"]["allocatable"], {"cpu": "4", "memory": "16Gi", "pods": "110"})
                ie = dataclasses.replace(
                    ie0,
                    event_type="Update",
                    is_not_ready=is_not_ready,
                    has_no_execute_taint=has_no_exec,
                    ready_status=ready_st,
                    manifest=man,
                    has_explicit_capacity=False,
                )
                mgr = _FakeMgr()
                with contextlib.redirect_stdout(io.StringIO()):
                    _execute_scheduled_infra_event(mgr, b61, ie, 1.0)
                for m, p, b, _ in mgr.requests:
                    if m != "PATCH" or not p.startswith(f"/api/v1/nodes/{ie.name}"):
                        continue
                    if p.endswith("/status"):
                        for key in ("allocatable", "capacity", "nodeInfo"):
                            self.assertNotIn(key, b["status"])
                    else:
                        for key in res_anns:
                            self.assertNotIn(key, (b.get("metadata") or {}).get("annotations") or {})

    def test_loader_marks_explicit_capacity_from_raw_node_payload(self) -> None:
        """has_explicit_capacity reflects the raw infra.json payload, not the prepared manifest."""
        src = REPO_ROOT / "testscenarios" / "self-tests" / "st61-tas-single-node-failure-hotswap-same-rack"
        with tempfile.TemporaryDirectory() as td:
            dst = Path(td) / "sc"
            shutil.copytree(src, dst)
            lines = (dst / "infra.json").read_text(encoding="utf-8").splitlines()
            node_updates = [
                i
                for i, ln in enumerate(lines)
                if ln.strip()
                and json.loads(ln).get("eventType") == "Update"
                and (json.loads(ln).get("payload") or {}).get("kind") == "Node"
            ]
            self.assertGreaterEqual(len(node_updates), 1)
            with_cap = load_scenario_bundle(scenario_dir=dst, speedup=1.0, skip_initial_offset=True)
            upd_with = [
                ie for ie in with_cap.scheduled_infra_events if ie.object_kind == "Node" and ie.event_type == "Update"
            ]
            self.assertTrue(upd_with and all(ie.has_explicit_capacity for ie in upd_with))

            for i in node_updates:
                ev = json.loads(lines[i])
                st = ev["payload"].get("status") or {}
                st.pop("allocatable", None)
                st.pop("capacity", None)
                ev["payload"]["status"] = st
                lines[i] = json.dumps(ev, separators=(",", ":"))
            (dst / "infra.json").write_text("\n".join(lines) + "\n", encoding="utf-8")
            without_cap = load_scenario_bundle(scenario_dir=dst, speedup=1.0, skip_initial_offset=True)
            upd_without = [
                ie
                for ie in without_cap.scheduled_infra_events
                if ie.object_kind == "Node" and ie.event_type == "Update"
            ]
            self.assertTrue(upd_without and not any(ie.has_explicit_capacity for ie in upd_without))
            self.assertTrue(all((ie.manifest.get("status") or {}).get("allocatable") for ie in upd_without))

    def test_node_update_for_missing_node_is_skipped_with_warning(self) -> None:
        """An Update for a Node that no longer exists is skipped (GET, spec PATCH, or /status PATCH 404) without timeline drift."""
        b61 = _load_bundle("st61-tas-single-node-failure-hotswap-same-rack")
        ie_upd = next(ie for ie in b61.scheduled_infra_events if ie.object_kind == "Node" and ie.event_type == "Update")
        ie_healthy = dataclasses.replace(ie_upd, is_not_ready=False, has_no_execute_taint=False, ready_status="True")
        cases = [
            ("unhealthy_get_404", ie_upd, "GET", f"/api/v1/nodes/{ie_upd.name}", []),
            ("healthy_get_404", ie_healthy, "GET", f"/api/v1/nodes/{ie_upd.name}", []),
            ("unhealthy_spec_patch_404", ie_upd, "PATCH", f"/api/v1/nodes/{ie_upd.name}", [f"/api/v1/nodes/{ie_upd.name}"]),
            ("healthy_spec_patch_404", ie_healthy, "PATCH", f"/api/v1/nodes/{ie_upd.name}", [f"/api/v1/nodes/{ie_upd.name}"]),
            (
                "unhealthy_status_patch_404",
                ie_upd,
                "PATCH",
                f"/api/v1/nodes/{ie_upd.name}/status",
                [f"/api/v1/nodes/{ie_upd.name}", f"/api/v1/nodes/{ie_upd.name}/status"],
            ),
            (
                "healthy_status_patch_404",
                ie_healthy,
                "PATCH",
                f"/api/v1/nodes/{ie_upd.name}/status",
                [f"/api/v1/nodes/{ie_upd.name}", f"/api/v1/nodes/{ie_upd.name}/status"],
            ),
        ]
        for label, ie, fail_method, fail_path, expected_patches in cases:
            with self.subTest(case=label):
                mgr = _FakeMgr(
                    responder=lambda m, p, _b, fm=fail_method, fp=fail_path: (404, b'{"kind":"Status","code":404}')
                    if m == fm and p == fp
                    else None
                )
                out = io.StringIO()
                with contextlib.redirect_stdout(out):
                    deferred, reason = _execute_scheduled_infra_event(mgr, b61, ie, 1.0)
                self.assertFalse(deferred)
                self.assertEqual(reason, "not_found")
                self.assertIn("does not exist in the chamber", out.getvalue())
                self.assertEqual(
                    [p for m, p, _b, _s in mgr.requests if m == "PATCH"],
                    expected_patches,
                )

    def test_node_update_merge_patch_nulls_dropped_resources_labels_and_annotations(self) -> None:
        """RFC 7396 merge-patch emits explicit null values for resources/labels/annotations removed by a Node Update."""
        b61 = _load_bundle("st61-tas-single-node-failure-hotswap-same-rack")
        ie0 = next(ie for ie in b61.scheduled_infra_events if ie.object_kind == "Node")
        node0 = next(n for n in b61.nodes if n["metadata"]["name"] == ie0.name)

        live_node = copy.deepcopy(node0)
        live_node["metadata"]["labels"]["custom.io/removed-label"] = "present"
        live_node["metadata"].setdefault("annotations", {})["kueue-optimizer.gke.io/node-accel-resource"] = "google.com/tpu"
        live_node["metadata"]["annotations"]["kueue-optimizer.gke.io/node-accel-count"] = "4"

        updated = copy.deepcopy(node0)
        updated["status"]["allocatable"].pop("google.com/tpu", None)
        updated["status"]["capacity"].pop("google.com/tpu", None)
        updated["status"]["conditions"] = [{"type": "Ready", "status": "True"}]
        man = _prepare_node_manifest(updated, 0)
        ie = dataclasses.replace(
            ie0,
            event_type="Update",
            is_not_ready=False,
            has_no_execute_taint=False,
            ready_status="True",
            manifest=man,
            has_explicit_capacity=True,
        )
        live_bytes = json.dumps(live_node).encode("utf-8")
        mgr = _FakeMgr(
            responder=lambda m, p, _b: (200, live_bytes) if m == "GET" and p == f"/api/v1/nodes/{ie.name}" else None
        )
        with contextlib.redirect_stdout(io.StringIO()):
            deferred, reason = _execute_scheduled_infra_event(mgr, b61, ie, 1.0)
        self.assertFalse(deferred)
        self.assertEqual(reason, "")

        node_patches = [b for m, p, b, _ in mgr.requests if m == "PATCH" and p == f"/api/v1/nodes/{ie.name}"]
        status_patches = [b for m, p, b, _ in mgr.requests if m == "PATCH" and p == f"/api/v1/nodes/{ie.name}/status"]
        self.assertEqual(len(node_patches), 1)
        self.assertEqual(len(status_patches), 1)

        self.assertIn("custom.io/removed-label", node_patches[0]["metadata"]["labels"])
        self.assertIsNone(node_patches[0]["metadata"]["labels"]["custom.io/removed-label"])
        self.assertIsNone(node_patches[0]["metadata"]["annotations"]["kueue-optimizer.gke.io/node-accel-resource"])
        self.assertIsNone(node_patches[0]["metadata"]["annotations"]["kueue-optimizer.gke.io/node-accel-count"])
        self.assertIn("google.com/tpu", status_patches[0]["status"]["allocatable"])
        self.assertIsNone(status_patches[0]["status"]["allocatable"]["google.com/tpu"])
        self.assertIn("google.com/tpu", status_patches[0]["status"]["capacity"])
        self.assertIsNone(status_patches[0]["status"]["capacity"]["google.com/tpu"])

    def test_skipped_404_node_update_records_no_capacity_timeline_transition(self) -> None:
        """A Node Update skipped due to HTTP 404 must never append phantom capacity to _node_capacity_timeline."""
        b61 = _load_bundle("st61-tas-single-node-failure-hotswap-same-rack")
        ie0 = next(ie for ie in b61.scheduled_infra_events if ie.object_kind == "Node" and ie.event_type == "Update")
        node0 = next(n for n in b61.nodes if n["metadata"]["name"] == ie0.name)
        resized = copy.deepcopy(node0)
        resized["status"]["allocatable"]["google.com/tpu"] = "16"
        resized["status"]["capacity"]["google.com/tpu"] = "16"
        resized_man = _prepare_node_manifest(resized, 0)
        ie_upd = dataclasses.replace(
            ie0,
            target_wall_offset_sec=0.05,
            event_type="Update",
            is_not_ready=False,
            has_no_execute_taint=False,
            ready_status="True",
            manifest=resized_man,
            has_explicit_capacity=True,
        )
        bundle = dataclasses.replace(
            b61,
            scheduled_workflows=[],
            scheduled_infra_events=[ie_upd],
            scheduled_workflow_updates=[],
            scheduled_workflow_deletions=[],
            initial_workloads=[],
            initial_deployments=[],
            initial_pods=[],
        )
        mgr = _FakeMgr(
            responder=lambda m, p, _b: (404, b'{"kind":"Status","code":404}')
            if p == f"/api/v1/nodes/{ie_upd.name}"
            else None
        )
        with (
            mock.patch.object(
                replay,
                "query_cluster_status",
                return_value=ClusterStatusSnapshot(workloads_query_ok=True),
            ),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            run_scenario_replay(
                mgr=mgr,
                bundle=bundle,
                speedup=1.0,
                max_run_seconds=0.3,
                status_interval_sec=0.05,
                wait_for_completion=False,
            )
        # Only the initial t0 snapshot should exist; the 404-skipped resize must not append a transition.
        self.assertEqual(len(mgr._node_capacity_timeline), 1)

    def test_fail_pods_bound_to_node_tolerates_404_and_fails_fast_on_5xx(self) -> None:
        pod_list = json.dumps(
            {
                "items": [
                    {
                        "metadata": {"name": "pod-0", "namespace": "default"},
                        "spec": {"containers": [{"name": "c", "image": "pause"}]},
                        "status": {"phase": "Running"},
                    }
                ]
            }
        ).encode("utf-8")

        mgr_404 = _FakeMgr(
            responder=lambda m, p, _b: (200, pod_list)
            if m == "GET" and p.startswith("/api/v1/pods?")
            else ((404, b'{"code":404}') if m == "PATCH" else None)
        )
        self.assertEqual(
            replay._fail_pods_bound_to_node(mgr_404, {"default"}, "node-0", "NodeFailure", "gone"),
            0,
        )

        mgr_503 = _FakeMgr(
            responder=lambda m, p, _b: (200, pod_list)
            if m == "GET" and p.startswith("/api/v1/pods?")
            else ((503, b'{"code":503}') if m == "PATCH" else None)
        )
        with self.assertRaisesRegex(RuntimeError, "503"):
            replay._fail_pods_bound_to_node(mgr_503, {"default"}, "node-0", "NodeFailure", "err")

    def test_node_patch_and_delete_fail_fast_on_non_2xx(self) -> None:
        b61 = _load_bundle("st61-tas-single-node-failure-hotswap-same-rack")
        ie_upd = next(ie for ie in b61.scheduled_infra_events if ie.object_kind == "Node" and ie.event_type == "Update")
        ie_healthy = dataclasses.replace(ie_upd, is_not_ready=False, has_no_execute_taint=False, ready_status="True")
        ie_del = dataclasses.replace(ie_upd, event_type="Deleted")

        fail_cases = [
            (
                "node_patch_503",
                ie_upd,
                lambda m, p, _b: (503, b'{"kind":"Status","code":503,"message":"apiserver busy"}')
                if m == "PATCH" and p == f"/api/v1/nodes/{ie_upd.name}"
                else None,
                "503",
            ),
            (
                "node_status_patch_500",
                ie_healthy,
                lambda m, p, _b: (500, b'{"kind":"Status","code":500,"message":"internal error"}')
                if m == "PATCH" and p == f"/api/v1/nodes/{ie_upd.name}/status"
                else None,
                "500",
            ),
            (
                "node_delete_503",
                ie_del,
                lambda m, p, _b: (503, b'{"kind":"Status","code":503}')
                if m == "DELETE" and p == f"/api/v1/nodes/{ie_upd.name}"
                else None,
                "503",
            ),
        ]
        for label, ie, responder, expected_code in fail_cases:
            with self.subTest(case=label):
                mgr = _FakeMgr(responder=responder)
                with contextlib.redirect_stdout(io.StringIO()):
                    with self.assertRaisesRegex(RuntimeError, expected_code):
                        _execute_scheduled_infra_event(mgr, b61, ie, 1.0)

        # Idempotent 404 on Node DELETE must succeed cleanly
        mgr_404 = _FakeMgr(
            responder=lambda m, p, _b: (404, b'{"kind":"Status","code":404}')
            if m == "DELETE" and p == f"/api/v1/nodes/{ie_del.name}"
            else None
        )
        with contextlib.redirect_stdout(io.StringIO()):
            _execute_scheduled_infra_event(mgr_404, b61, ie_del, 1.0)

    def test_workflow_delete_failure_raises_instead_of_counting_cancelled(self) -> None:
        base = _load_bundle()
        tmpl = base.scheduled_workflows[0]
        ns = tmpl.namespace
        items = _synth_items(tmpl, 2)
        del_ev = ScheduledWorkflowDeletion(
            target_wall_offset_sec=0.05,
            seq=0,
            raw_offset_sec=0.05,
            raw_offset_from_t0_sec=0.05,
            event_timestamp="",
            object_kind="JobSet",
            manifest_kind="JobSet",
            manifest_api_version="jobset.x-k8s.io/v1alpha2",
            name="wf-000",
            applied_name="wf-000",
            namespace=ns,
        )
        bundle = dataclasses.replace(
            base,
            scheduled_workflows=items,
            scheduled_infra_events=[],
            scheduled_workflow_updates=[],
            scheduled_workflow_deletions=[del_ev],
            initial_workloads=[],
            initial_deployments=[],
            initial_pods=[],
        )

        # 503 on DELETE must raise RuntimeError rather than silently marking wf-000 cancelled
        mgr_503 = _FakeMgr(responder=lambda m, p, _b: (503, b'{"code":503}') if m == "DELETE" else None)
        with (
            mock.patch.object(
                replay,
                "query_cluster_status",
                return_value=ClusterStatusSnapshot(workloads_query_ok=True),
            ),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            with self.assertRaisesRegex(RuntimeError, "503"):
                run_scenario_replay(
                    mgr=mgr_503,
                    bundle=bundle,
                    speedup=1.0,
                    max_run_seconds=5.0,
                    status_interval_sec=0.05,
                    wait_for_completion=True,
                )

        # 404 on DELETE is idempotent and allows completion once remaining workflow finishes
        k1 = (ns, "jobset-wf-001-bbbbb")
        mgr_404 = _FakeMgr(responder=lambda m, p, _b: (404, b'{"code":404}') if m == "DELETE" else None)
        with (
            mock.patch.object(
                replay,
                "query_cluster_status",
                return_value=ClusterStatusSnapshot(
                    workloads_query_ok=True,
                    present_workload_keys={k1},
                    finished_workload_keys={k1},
                    workloads_finished=1,
                ),
            ),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            res = run_scenario_replay(
                mgr=mgr_404,
                bundle=bundle,
                speedup=1.0,
                max_run_seconds=5.0,
                status_interval_sec=0.05,
                wait_for_completion=True,
            )
        self.assertEqual(res.stop_reason, "completed")


class VanishedWorkloadAndInitialResubmitTests(unittest.TestCase):
    """Tests for R3 (vanished-before-poll terminal workload handling) and R9 (finished t0 Workload not resubmitted)."""

    def test_vanished_after_seen_completes_without_hanging(self) -> None:
        base = _load_bundle()
        tmpl = base.scheduled_workflows[0]
        ns = tmpl.namespace
        items = _synth_items(tmpl, 2)
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
        k0, k1 = (ns, "jobset-wf-000-aaaaa"), (ns, "jobset-wf-001-bbbbb")
        snaps = [
            # Poll 1: both running and present
            ClusterStatusSnapshot(
                workloads_query_ok=True,
                present_workload_keys={k0, k1},
                workloads_admitted=2,
                jobsets_total=2,
                jobsets_active=2,
            ),
            # Poll 2: wf-001 finished AND TTL-deleted between polls (never observed Finished=True); wf-000 still running
            ClusterStatusSnapshot(
                workloads_query_ok=True,
                present_workload_keys={k0},
                workloads_admitted=1,
                jobsets_total=1,
                jobsets_active=1,
            ),
            # Poll 3: wf-000 finishes normally
            ClusterStatusSnapshot(
                workloads_query_ok=True,
                present_workload_keys={k0},
                finished_workload_keys={k0},
                workloads_finished=1,
                jobsets_total=1,
                jobsets_completed=1,
            ),
        ]
        idx = {"i": 0}

        def _next_snap(*_a, **_kw):
            s = snaps[min(idx["i"], len(snaps) - 1)]
            idx["i"] += 1
            return s

        mgr = _FakeMgr()
        t_start = time.monotonic()
        with (
            mock.patch.object(replay, "query_cluster_status", side_effect=_next_snap),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            res = run_scenario_replay(
                mgr=mgr,
                bundle=bundle,
                speedup=1.0,
                max_run_seconds=8.0,
                status_interval_sec=0.05,
                wait_for_completion=True,
            )
        self.assertEqual(res.stop_reason, "completed")
        self.assertEqual(res.submitted_count, 2)
        self.assertLess(time.monotonic() - t_start, 3.0)
        self.assertEqual(idx["i"], 3)

    def test_workload_absent_for_one_poll_then_back_is_not_counted_finished(self) -> None:
        """A Workload deleted and re-created under the same name between polls must keep the replay open."""
        base = _load_bundle()
        tmpl = base.scheduled_workflows[0]
        ns = tmpl.namespace
        bundle = dataclasses.replace(
            base,
            scheduled_workflows=_synth_items(tmpl, 1),
            scheduled_infra_events=[],
            scheduled_workflow_updates=[],
            scheduled_workflow_deletions=[],
            initial_workloads=[],
            initial_deployments=[],
            initial_pods=[],
        )
        k0 = (ns, "jobset-wf-000-aaaaa")
        running = ClusterStatusSnapshot(workloads_query_ok=True, present_workload_keys={k0}, workloads_admitted=1)
        gone = ClusterStatusSnapshot(workloads_query_ok=True)
        done = ClusterStatusSnapshot(
            workloads_query_ok=True, present_workload_keys={k0}, finished_workload_keys={k0}, workloads_finished=1
        )
        snaps = [running, gone, running, running, done]
        idx = {"i": 0}

        def _next_snap(*_a, **_kw):
            s = snaps[min(idx["i"], len(snaps) - 1)]
            idx["i"] += 1
            return s

        with (
            mock.patch.object(replay, "query_cluster_status", side_effect=_next_snap),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            res = run_scenario_replay(
                mgr=_FakeMgr(),
                bundle=bundle,
                speedup=1.0,
                max_run_seconds=8.0,
                status_interval_sec=0.05,
                wait_for_completion=True,
            )
        self.assertEqual(res.stop_reason, "completed")
        # The replay must not stop at the poll where the Workload was briefly absent.
        self.assertEqual(idx["i"], 5)

    def test_cancelled_workflow_vanishing_is_not_double_counted(self) -> None:
        base = _load_bundle()
        tmpl = base.scheduled_workflows[0]
        ns = tmpl.namespace
        items = _synth_items(tmpl, 2)
        del_ev = ScheduledWorkflowDeletion(
            target_wall_offset_sec=0.12,
            seq=0,
            raw_offset_sec=0.12,
            raw_offset_from_t0_sec=0.12,
            event_timestamp="",
            object_kind="JobSet",
            manifest_kind="JobSet",
            manifest_api_version="jobset.x-k8s.io/v1alpha2",
            name="wf-000",
            applied_name="wf-000",
            namespace=ns,
        )
        bundle = dataclasses.replace(
            base,
            scheduled_workflows=items,
            scheduled_infra_events=[],
            scheduled_workflow_updates=[],
            scheduled_workflow_deletions=[del_ev],
            initial_workloads=[],
            initial_deployments=[],
            initial_pods=[],
        )
        k0, k1 = (ns, "jobset-wf-000-aaaaa"), (ns, "jobset-wf-001-bbbbb")
        state = {"deleted": False, "polls_after_del": 0}

        def _resp(m, p, _b):
            if m == "DELETE" and p.endswith("/jobsets/wf-000"):
                state["deleted"] = True
            return None

        def _snap(*_a, **_kw):
            if not state["deleted"]:
                return ClusterStatusSnapshot(
                    workloads_query_ok=True,
                    present_workload_keys={k0, k1},
                    workloads_admitted=2,
                )
            state["polls_after_del"] += 1
            if state["polls_after_del"] < 3:
                # wf-000 is gone (cancelled), wf-001 is STILL running -> must NOT exit early!
                return ClusterStatusSnapshot(
                    workloads_query_ok=True,
                    present_workload_keys={k1},
                    workloads_admitted=1,
                )
            return ClusterStatusSnapshot(
                workloads_query_ok=True,
                present_workload_keys={k1},
                finished_workload_keys={k1},
                workloads_finished=1,
            )

        mgr = _FakeMgr(responder=_resp)
        with (
            mock.patch.object(replay, "query_cluster_status", side_effect=_snap),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            res = run_scenario_replay(
                mgr=mgr,
                bundle=bundle,
                speedup=1.0,
                max_run_seconds=5.0,
                status_interval_sec=0.05,
                wait_for_completion=True,
            )
        self.assertEqual(res.stop_reason, "completed")
        self.assertGreaterEqual(state["polls_after_del"], 3)

    def test_vanished_before_first_poll_checks_parent_404_after_grace(self) -> None:
        base = _load_bundle()
        tmpl = base.scheduled_workflows[0]
        ns = tmpl.namespace
        items = _synth_items(tmpl, 1)
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
        parent_path = f"/apis/jobset.x-k8s.io/v1alpha2/namespaces/{ns}/jobsets/wf-000"

        # Case 1: parent STILL exists (200) -> must NOT prematurely treat as vanished
        mgr_present = _FakeMgr(responder=lambda m, p, _b: (200, b"{}") if m == "GET" and p == parent_path else None)
        with (
            mock.patch.object(
                replay,
                "query_cluster_status",
                return_value=ClusterStatusSnapshot(workloads_query_ok=True),
            ),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            res_wait = run_scenario_replay(
                mgr=mgr_present,
                bundle=bundle,
                speedup=1.0,
                max_run_seconds=2.3,
                status_interval_sec=0.05,
                wait_for_completion=True,
            )
        self.assertEqual(res_wait.stop_reason, "duration_reached")

        # Case 2: parent returns 404 (TTL-deleted before first poll) -> completes once grace elapses
        mgr_gone = _FakeMgr(responder=lambda m, p, _b: (404, b'{"code":404}') if m == "GET" and p == parent_path else None)
        t_start = time.monotonic()
        with (
            mock.patch.object(
                replay,
                "query_cluster_status",
                return_value=ClusterStatusSnapshot(workloads_query_ok=True),
            ),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            res_gone = run_scenario_replay(
                mgr=mgr_gone,
                bundle=bundle,
                speedup=1.0,
                max_run_seconds=6.0,
                status_interval_sec=0.05,
                wait_for_completion=True,
            )
        elapsed = time.monotonic() - t_start
        self.assertEqual(res_gone.stop_reason, "completed")
        self.assertGreaterEqual(elapsed, 1.9)
        self.assertLess(elapsed, 4.5)

    def test_finished_t0_workload_that_disappears_is_never_resubmitted(self) -> None:
        ib = _load_bundle("st37-t0-preempt-within-cq-lower-priority-resubmit")
        iw = next(w for w in ib.initial_workloads if w.resubmit_item is not None)
        key = (iw.namespace, iw.workload_name)
        late = dataclasses.replace(ib.scheduled_workflows[0], target_wall_offset_sec=30.0)
        bundle = dataclasses.replace(
            ib,
            scheduled_workflows=[late],
            scheduled_infra_events=[],
            scheduled_workflow_updates=[],
            scheduled_workflow_deletions=[],
            initial_workloads=[iw],
            initial_deployments=[],
            initial_pods=[],
        )

        # Scenario A: t0 workload finishes normally, then disappears (GC) -> MUST NOT resubmit
        snaps = [
            ClusterStatusSnapshot(workloads_query_ok=True, present_workload_keys={key}, workloads_admitted=1),
            ClusterStatusSnapshot(
                workloads_query_ok=True,
                present_workload_keys={key},
                finished_workload_keys={key},
                workloads_finished=1,
            ),
            ClusterStatusSnapshot(workloads_query_ok=True),
        ]
        poll_i = {"i": 0}

        def _snap_finish_then_gc(*_a, **_kw):
            s = snaps[min(poll_i["i"], len(snaps) - 1)]
            poll_i["i"] += 1
            return s

        mgr = _FakeMgr()
        with (
            mock.patch.object(replay, "query_cluster_status", side_effect=_snap_finish_then_gc),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            res = run_scenario_replay(
                mgr=mgr,
                bundle=bundle,
                speedup=1.0,
                max_run_seconds=0.35,
                status_interval_sec=0.05,
                wait_for_completion=True,
            )
        self.assertEqual(res.stop_reason, "duration_reached")
        self.assertEqual(mgr.applied, [])
        self.assertEqual(res.submitted_count, 0)

        # Scenario B (control): t0 workload disappears WITHOUT ever finishing (preempted) -> MUST resubmit
        mgr_preempt = _FakeMgr()
        with (
            mock.patch.object(
                replay,
                "query_cluster_status",
                return_value=ClusterStatusSnapshot(workloads_query_ok=True),
            ),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            res_p = run_scenario_replay(
                mgr=mgr_preempt,
                bundle=bundle,
                speedup=1.0,
                max_run_seconds=0.2,
                status_interval_sec=0.05,
                wait_for_completion=True,
            )
        self.assertEqual(len(mgr_preempt.applied), 1)
        self.assertEqual(res_p.submitted_count, 1)

    def test_workload_name_matches_owner_avoids_prefix_collisions(self) -> None:
        """Kueue Workload matching requires the exact 5-hex suffix so sibling prefixes never collide."""
        long_owner = "a" * 60
        trunc_jobset = f"jobset-{long_owner}"[:57]
        cases = [
            ("exact_5hex", "jobset-train-abcde", "JobSet", "train", True),
            ("exact_5hex_digits", "job-train-01234", "Job", "train", True),
            ("sibling_prefix_rejected", "jobset-train-eval-abcde", "JobSet", "train", False),
            ("extra_suffix_rejected", "jobset-train-abcde-extra", "JobSet", "train", False),
            ("non_hex_suffix_rejected", "jobset-train-zzzzz", "JobSet", "train", False),
            ("truncated_long_owner_valid", f"{trunc_jobset}-1a2b3", "JobSet", long_owner, True),
            ("truncated_long_owner_non_hex", f"{trunc_jobset}-ghijk", "JobSet", long_owner, False),
        ]
        for label, wl_name, kind, owner_name, expected in cases:
            with self.subTest(case=label):
                self.assertEqual(replay._workload_name_matches_owner(wl_name, kind, owner_name), expected)

    def test_recreated_incarnation_vanishing_and_scale_burst_probe_rate_limit(self) -> None:
        """Recreated same-name incarnations are not masked by incarnation 0, and burst probes are capped/cooldown-limited."""
        base = _load_bundle()
        tmpl = base.scheduled_workflows[0]
        ns = tmpl.namespace

        # Subtest 1: incarnation 0 finishes normally, is deleted (before_recreate), incarnation 1 vanishes before poll.
        inc0 = _synth_items(tmpl, 1)[0]
        inc1 = dataclasses.replace(inc0, target_wall_offset_sec=0.15, seq=1, incarnation_index=1)
        del_inc0 = ScheduledWorkflowDeletion(
            target_wall_offset_sec=0.10,
            seq=0,
            raw_offset_sec=0.10,
            raw_offset_from_t0_sec=0.10,
            event_timestamp="",
            object_kind="JobSet",
            manifest_kind="JobSet",
            manifest_api_version="jobset.x-k8s.io/v1alpha2",
            name="wf-000",
            applied_name="wf-000",
            namespace=ns,
            incarnation_index=0,
            delete_reason="before_recreate",
            has_later_incarnation=True,
        )
        bundle_recreate = dataclasses.replace(
            base,
            scheduled_workflows=[inc0, inc1],
            scheduled_infra_events=[],
            scheduled_workflow_updates=[],
            scheduled_workflow_deletions=[del_inc0],
            initial_workloads=[],
            initial_deployments=[],
            initial_pods=[],
        )
        k0 = (ns, "jobset-wf-000-aaaaa")
        parent_path = f"/apis/jobset.x-k8s.io/v1alpha2/namespaces/{ns}/jobsets/wf-000"
        wls_list_resp = json.dumps(
            {
                "items": [
                    {
                        "metadata": {
                            "name": k0[1],
                            "namespace": ns,
                            "ownerReferences": [{"name": "wf-000"}],
                        },
                        "status": {"conditions": [{"type": "Finished", "status": "True"}]},
                    }
                ]
            }
        ).encode("utf-8")
        recreate_state = {"deleted_inc0": False}

        def _recreate_resp(m, p, _b):
            if m == "GET" and p.endswith("/workloads"):
                return (200, wls_list_resp)
            if m == "DELETE" and p == parent_path:
                recreate_state["deleted_inc0"] = True
                return (200, b"{}")
            if m == "GET" and p == parent_path and recreate_state["deleted_inc0"]:
                return (404, b'{"code":404}')
            return None

        def _recreate_snap(*_a, **_kw):
            if not recreate_state["deleted_inc0"]:
                return ClusterStatusSnapshot(
                    workloads_query_ok=True,
                    present_workload_keys={k0},
                    finished_workload_keys={k0},
                    workloads_finished=1,
                )
            return ClusterStatusSnapshot(workloads_query_ok=True)

        mgr_rec = _FakeMgr(responder=_recreate_resp)
        t0 = time.monotonic()
        with (
            mock.patch.object(replay, "query_cluster_status", side_effect=_recreate_snap),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            res_rec = run_scenario_replay(
                mgr=mgr_rec,
                bundle=bundle_recreate,
                speedup=1.0,
                max_run_seconds=6.0,
                status_interval_sec=0.05,
                wait_for_completion=True,
            )
        self.assertEqual(res_rec.stop_reason, "completed")
        self.assertLess(time.monotonic() - t0, 4.5)

        # Subtest 2: scale-burst probe cooldown prevents hammering GET on every 0.05s poll.
        burst_items = _synth_items(tmpl, 40)
        bundle_burst = dataclasses.replace(
            base,
            scheduled_workflows=burst_items,
            scheduled_infra_events=[],
            scheduled_workflow_updates=[],
            scheduled_workflow_deletions=[],
            initial_workloads=[],
            initial_deployments=[],
            initial_pods=[],
        )
        parent_gets: dict[str, int] = {}

        def _burst_resp(m, p, _b):
            if m == "GET" and "/jobsets/wf-" in p:
                parent_gets[p] = parent_gets.get(p, 0) + 1
                return (200, b"{}")
            return None

        mgr_burst = _FakeMgr(responder=_burst_resp)
        with (
            mock.patch.object(
                replay,
                "query_cluster_status",
                return_value=ClusterStatusSnapshot(workloads_query_ok=True),
            ),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            res_burst = run_scenario_replay(
                mgr=mgr_burst,
                bundle=bundle_burst,
                speedup=1.0,
                max_run_seconds=2.5,
                status_interval_sec=0.05,
                wait_for_completion=True,
            )
        self.assertEqual(res_burst.stop_reason, "duration_reached")
        # Every record should have been probed at most once during the 5s cooldown window.
        self.assertGreater(len(parent_gets), 0)
        self.assertTrue(all(cnt == 1 for cnt in parent_gets.values()), f"Repeated probes during cooldown: {parent_gets}")


class ParseDurationSecondsTests(unittest.TestCase):
    """Tests for R4 (Go compound duration parsing)."""

    def test_parse_duration_seconds_table(self) -> None:
        valid_cases = [
            ("300", 300.0),
            ("0", 0.0),
            ("0s", 0.0),
            ("1.5", 1.5),
            (".5s", 0.5),
            ("5.", 5.0),
            ("45s", 45.0),
            ("10m", 600.0),
            ("1.5h", 5400.0),
            ("1m30s", 90.0),
            ("5m0s", 300.0),
            ("1h30m", 5400.0),
            ("1h2m3.5s", 3723.5),
            ("168h0m0s", 604800.0),
            ("500ms", 0.5),
            ("100us", 0.0001),
            ("100\u00b5s", 0.0001),
            ("100\u03bcs", 0.0001),
            ("500ns", 5e-7),
            ("2d12h", 216000.0),
            ("1m30s500ms", 90.5),
            ("1m 30s", 90.0),
            (" 12 s ", 12.0),
            ("1 h", 3600.0),
            ("2D", 172800.0),
            ("30sec", 30.0),
            ("2mins", 120.0),
            ("+1m30s", 90.0),
            ("+45s", 45.0),
            ("+10", 10.0),
            (0, 0.0),
            (42, 42.0),
            (2.5, 2.5),
        ]
        for raw, expected in valid_cases:
            with self.subTest(valid=raw):
                got = parse_duration_seconds(raw)
                self.assertIsNotNone(got)
                self.assertAlmostEqual(got, expected, places=9)

        for raw in (None, "", "   "):
            with self.subTest(empty=raw):
                self.assertIsNone(parse_duration_seconds(raw))

        invalid_cases = [
            "-5s",
            -3,
            -0.1,
            float("nan"),
            float("inf"),
            float("-inf"),
            True,
            False,
            "abc",
            "PT5M",
            "1w",
            "1m30",
            "1.2.3s",
            "1e3",
            "9" * 400,
            "9" * 400 + "s",
            "1" + "0" * 308 + "d",
            "+",
            "+-5s",
            "++5s",
        ]
        for raw in invalid_cases:
            with self.subTest(invalid=raw):
                with self.assertRaises(ValueError):
                    parse_duration_seconds(raw)

    def test_compound_durations_in_annotations_and_execution_time_index(self) -> None:
        self.assertAlmostEqual(parse_positive_duration_annotation({"k": "1m30s"}, "k"), 90.0)
        self.assertAlmostEqual(parse_positive_duration_annotation({"k": "5m0s"}, "k"), 300.0)
        self.assertIsNone(parse_positive_duration_annotation({"k": "-5s"}, "k"))
        self.assertIsNone(parse_positive_duration_annotation({"k": "0s"}, "k"))

        with tempfile.TemporaryDirectory() as tmp:
            et_path = Path(tmp) / "execution-time.json"
            ev = {
                "timestamp": "2026-10-30T12:01:30Z",
                "eventType": "FullExecution",
                "payload": {
                    "workloadName": "jobset-demo-abcde",
                    "namespace": "ai-lab",
                    "ownerKind": "JobSet",
                    "ownerName": "demo",
                    "executionDuration": "1m30s",
                },
            }
            et_path.write_text(json.dumps(ev) + "\n", encoding="utf-8")
            idx = index_execution_times(et_path)
            info = idx.lookup("ai-lab", "JobSet", "demo")
            self.assertIsNotNone(info)
            self.assertAlmostEqual(info.full_duration_sec, 90.0)


class ReplayResultSignalAndCleanupTests(unittest.TestCase):
    """Tests for R5 (ReplayResult, stop_reason, signal exit codes) and dead bootstrap branch removal."""

    def test_dead_bootstrap_parameter_removed(self) -> None:
        sig = inspect.signature(run_scenario_replay)
        self.assertNotIn("initial_state_already_bootstrapped", sig.parameters)

    def test_replay_result_tuple_unpacking_and_exit_codes(self) -> None:
        ok = ReplayResult(10.0, 20.0, 3, 12, stop_reason="completed")
        s, e, sub, pods = ok
        self.assertEqual((s, e, sub, pods), (10.0, 20.0, 3, 12))
        self.assertEqual(len(ok), 4)
        self.assertEqual(ok[2], 3)
        self.assertFalse(ok.interrupted)
        self.assertEqual(ok.exit_code, 0)

        dur = ReplayResult(10.0, 20.0, 2, 8, stop_reason="duration_reached")
        self.assertFalse(dur.interrupted)
        self.assertEqual(dur.exit_code, 0)

        sigint = ReplayResult(10.0, 12.0, 1, 4, stop_reason="interrupted", signal_num=int(signal.SIGINT))
        self.assertTrue(sigint.interrupted)
        self.assertEqual(sigint.exit_code, 128 + int(signal.SIGINT))

        sigterm = ReplayResult(10.0, 12.0, 1, 4, stop_reason="interrupted", signal_num=int(signal.SIGTERM))
        self.assertTrue(sigterm.interrupted)
        self.assertEqual(sigterm.exit_code, 128 + int(signal.SIGTERM))

    def test_sigint_and_sigterm_return_interrupted_replay_result(self) -> None:
        base = _load_bundle()
        tmpl = base.scheduled_workflows[0]
        items = _synth_items(tmpl, 3, wall_gap=5.0)
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

        for sig in (signal.SIGINT, signal.SIGTERM):
            with self.subTest(signal=sig.name):
                mgr = _FakeMgr()
                timer = threading.Timer(0.15, lambda s=sig: os.kill(os.getpid(), s))
                timer.start()
                try:
                    with (
                        mock.patch.object(
                            replay,
                            "query_cluster_status",
                            return_value=ClusterStatusSnapshot(workloads_query_ok=True),
                        ),
                        contextlib.redirect_stdout(io.StringIO()),
                    ):
                        res = run_scenario_replay(
                            mgr=mgr,
                            bundle=bundle,
                            speedup=1.0,
                            max_run_seconds=10.0,
                            status_interval_sec=0.05,
                            wait_for_completion=True,
                        )
                finally:
                    timer.cancel()

                self.assertIsInstance(res, ReplayResult)
                self.assertTrue(res.interrupted)
                self.assertEqual(res.stop_reason, "interrupted")
                self.assertEqual(res.signal_num, int(sig))
                self.assertEqual(res.exit_code, 128 + int(sig))

    def test_testrunner_main_returns_signal_exit_code_and_skips_metrics_on_interrupt(self) -> None:
        cases = [
            ("completed", ReplayResult(100.0, 105.0, 2, 4, stop_reason="completed"), 0, True),
            ("sigint", ReplayResult(100.0, 101.0, 1, 2, stop_reason="interrupted", signal_num=int(signal.SIGINT)), 130, False),
            ("sigterm", ReplayResult(100.0, 101.0, 1, 2, stop_reason="interrupted", signal_num=int(signal.SIGTERM)), 143, False),
        ]
        demo_dir = str(REPO_ROOT / "testscenarios" / "demo")
        for label, replay_res, expected_rc, expect_metrics in cases:
            with self.subTest(case=label):
                with (
                    mock.patch.object(ChamberManager, "deploy_chamber"),
                    mock.patch.object(ChamberManager, "cleanup_chamber") as m_cleanup,
                    mock.patch.object(ChamberManager, "ensure_kwok_connection"),
                    mock.patch.object(ChamberManager, "wait_for_crds"),
                    mock.patch.object(ChamberManager, "apply_kwok_stages"),
                    mock.patch.object(ChamberManager, "ensure_kueue_webhook_namespace_exclusions"),
                    mock.patch.object(ChamberManager, "sync_kueue_configuration_to_kwok_apiserver"),
                    mock.patch.object(ChamberManager, "clear_workload_namespace"),
                    mock.patch.object(ChamberManager, "sync_kwok_nodes"),
                    mock.patch.object(ChamberManager, "run_kueue_metrics_report") as m_metrics,
                    mock.patch.object(testrunner, "apply_scenario_queues_and_wait"),
                    mock.patch.object(testrunner, "run_scenario_replay", return_value=replay_res),
                    mock.patch.object(
                        testrunner,
                        "query_cluster_status",
                        return_value=ClusterStatusSnapshot(workloads_query_ok=True),
                    ),
                    contextlib.redirect_stdout(io.StringIO()) as out_buf,
                ):
                    rc = testrunner.main(
                        [demo_dir, "--namespace", "kueue-optimizer-chamber-sigtest", "--port", "16491"]
                    )
                self.assertEqual(rc, expected_rc)
                m_cleanup.assert_called_once()
                if expect_metrics:
                    m_metrics.assert_called_once()
                else:
                    m_metrics.assert_not_called()
                    self.assertIn("Stop Reason", out_buf.getvalue())


if __name__ == "__main__":
    unittest.main()
