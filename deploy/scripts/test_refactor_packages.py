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

"""Focused unit tests for the modularized chamber, metrics, runner, and scenario_readme packages."""

from __future__ import annotations

import io
from pathlib import Path
import sys
import unittest
from unittest import mock

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from chamber.cli import managed_chamber
from chamber.lifecycle import ChamberManager
import demo
from metrics.aggregations import _index_quota_series_by_resource
from metrics.saturation import (
    _aggregate_cluster_capacity,
    _index_pods_by_owner,
    completed_effective_duration,
)
from runner.validation import (
    _max_workflow_name_len,
    _prepare_kueue_or_ns_doc,
    _prepare_node_manifest,
    validate_workflow_events,
)
from scenario_readme.analysis import ScenarioAnalysis, analyze_scenario
from scenario_readme.render import render_scenario_readme


class ManagedChamberLifecycleTests(unittest.TestCase):
    def _make_mgr(self, ns: str = "kueue-optimizer-chamber-unit") -> ChamberManager:
        return ChamberManager(chamber_namespace=ns, apiserver_port=16480)

    def test_managed_chamber_cleans_up_on_success_when_requested(self) -> None:
        mgr = self._make_mgr()
        try:
            with (
                mock.patch.object(mgr, "deploy_chamber") as m_deploy,
                mock.patch.object(mgr, "cleanup_chamber") as m_cleanup,
                mock.patch.object(mgr, "close") as m_close,
                mock.patch("sys.stdout", new_callable=io.StringIO),
            ):
                with managed_chamber(mgr, skip_deploy=False, should_cleanup=True, initial_nodes=[{"kind": "Node"}]):
                    pass
                m_deploy.assert_called_once_with(initial_nodes=[{"kind": "Node"}])
                m_cleanup.assert_called_once()
                m_close.assert_not_called()
        finally:
            mgr.close()

    def test_managed_chamber_preserves_preexisting_namespace_on_error(self) -> None:
        mgr = self._make_mgr()
        try:
            with (
                mock.patch.object(mgr, "deploy_chamber", side_effect=RuntimeError("pre-apply fail")),
                mock.patch.object(mgr, "cleanup_chamber") as m_cleanup,
                mock.patch.object(mgr, "close") as m_close,
            ):
                with self.assertRaisesRegex(RuntimeError, "pre-apply fail"):
                    with managed_chamber(mgr, skip_deploy=False, should_cleanup=True):
                        pass
                m_cleanup.assert_not_called()
                m_close.assert_called_once()
        finally:
            mgr.close()

    def test_managed_chamber_cleans_up_newly_created_namespace_on_error_and_preserves_original_exc(self) -> None:
        mgr = self._make_mgr()
        try:
            def _deploy_and_fail(initial_nodes: object = None) -> None:
                mgr._created_namespace_in_this_run = True
                raise RuntimeError("rollout failed")

            with (
                mock.patch.object(mgr, "deploy_chamber", side_effect=_deploy_and_fail),
                mock.patch.object(mgr, "cleanup_chamber", side_effect=RuntimeError("cleanup hiccup")) as m_cleanup,
                mock.patch("sys.stdout", new_callable=io.StringIO),
                mock.patch("sys.stderr", new_callable=io.StringIO) as err_buf,
            ):
                with self.assertRaisesRegex(RuntimeError, "rollout failed"):
                    with managed_chamber(mgr, skip_deploy=False, should_cleanup=True):
                        pass
                m_cleanup.assert_called_once()
                self.assertIn("WARNING: Chamber cleanup after error failed", err_buf.getvalue())
        finally:
            mgr.close()

    def test_managed_chamber_skip_deploy_never_deploys_or_deletes_on_error(self) -> None:
        mgr = self._make_mgr()
        try:
            with (
                mock.patch.object(mgr, "deploy_chamber") as m_deploy,
                mock.patch.object(mgr, "cleanup_chamber") as m_cleanup,
                mock.patch.object(mgr, "close") as m_close,
            ):
                with self.assertRaisesRegex(RuntimeError, "connection boom"):
                    with managed_chamber(mgr, skip_deploy=True, should_cleanup=True):
                        raise RuntimeError("connection boom")
                m_deploy.assert_not_called()
                m_cleanup.assert_not_called()
                m_close.assert_called_once()
        finally:
            mgr.close()

    def test_managed_chamber_only_closes_when_cleanup_not_requested(self) -> None:
        mgr = self._make_mgr()
        try:
            with (
                mock.patch.object(mgr, "deploy_chamber") as m_deploy,
                mock.patch.object(mgr, "cleanup_chamber") as m_cleanup,
                mock.patch.object(mgr, "close") as m_close,
            ):
                with managed_chamber(mgr, skip_deploy=False, should_cleanup=False):
                    pass
                m_deploy.assert_called_once_with(initial_nodes=None)
                m_cleanup.assert_not_called()
                m_close.assert_called_once()
        finally:
            mgr.close()

    def test_managed_chamber_reused_chamber_is_torn_down_after_success_when_requested(self) -> None:
        # --skip-chamber-deploy --cleanup-after: never deploys, but a successful run still deletes the chamber
        mgr = self._make_mgr()
        try:
            with (
                mock.patch.object(mgr, "deploy_chamber") as m_deploy,
                mock.patch.object(mgr, "cleanup_chamber") as m_cleanup,
                mock.patch.object(mgr, "close") as m_close,
                mock.patch("sys.stdout", new_callable=io.StringIO),
            ):
                with managed_chamber(mgr, skip_deploy=True, should_cleanup=True):
                    pass
                m_deploy.assert_not_called()
                m_cleanup.assert_called_once()
                m_close.assert_not_called()
        finally:
            mgr.close()

    def test_demo_main_uses_managed_chamber_lifecycle_safely(self) -> None:
        # Failure BEFORE namespace creation must NOT call cleanup_chamber
        with (
            mock.patch.object(ChamberManager, "deploy_chamber", side_effect=RuntimeError("pre-apply boom")),
            mock.patch.object(ChamberManager, "cleanup_chamber") as m_cleanup,
            mock.patch("sys.stdout", new_callable=io.StringIO),
        ):
            with self.assertRaisesRegex(RuntimeError, "pre-apply boom"):
                demo.main(["--namespace", "kueue-optimizer-chamber-demo-pre", "--port", "16481"])
            m_cleanup.assert_not_called()

        # Failure AFTER namespace creation DOES call cleanup_chamber
        def _create_then_fail(self_mgr: ChamberManager, initial_nodes: object = None) -> None:
            self_mgr._created_namespace_in_this_run = True
            raise RuntimeError("demo rollout boom")

        with (
            mock.patch.object(ChamberManager, "deploy_chamber", autospec=True, side_effect=_create_then_fail),
            mock.patch.object(ChamberManager, "cleanup_chamber") as m_cleanup,
            mock.patch("sys.stdout", new_callable=io.StringIO),
        ):
            with self.assertRaisesRegex(RuntimeError, "demo rollout boom"):
                demo.main(["--namespace", "kueue-optimizer-chamber-demo-new", "--port", "16482"])
            m_cleanup.assert_called_once()

        # --skip-chamber-deploy skips deploy_chamber and preserves reused chamber
        with (
            mock.patch.object(ChamberManager, "detect_live_fair_sharing_enabled", return_value=True),
            mock.patch.object(ChamberManager, "deploy_chamber") as m_deploy,
            mock.patch.object(ChamberManager, "ensure_kwok_connection", side_effect=RuntimeError("conn boom")),
            mock.patch.object(ChamberManager, "cleanup_chamber") as m_cleanup,
            mock.patch("sys.stdout", new_callable=io.StringIO),
        ):
            with self.assertRaisesRegex(RuntimeError, "conn boom"):
                demo.main(["--skip-chamber-deploy", "--namespace", "kueue-optimizer-chamber-reuse", "--port", "16483"])
            m_deploy.assert_not_called()
            m_cleanup.assert_not_called()


class ScenarioReadmePackageBoundaryTests(unittest.TestCase):
    def test_analyze_scenario_and_render_match_committed_demo_readme(self) -> None:
        demo_dir = REPO_ROOT / "testscenarios/demo"
        analysis = analyze_scenario(demo_dir)
        self.assertIsInstance(analysis, ScenarioAnalysis)
        self.assertEqual(analysis.scenario_title, "demo")
        self.assertEqual(len(analysis.all_nodes), 10)
        self.assertEqual(len(analysis.unadmitted_wfs), 20)
        self.assertEqual(sum(w.total_pods for w in analysis.unadmitted_wfs), 154)
        self.assertEqual(analysis.total_initial_pods, 0)

        rendered = render_scenario_readme(analysis)
        committed = (demo_dir / "README.md").read_text(encoding="utf-8")
        self.assertEqual(rendered, committed)


class MetricsSaturationHelperTests(unittest.TestCase):
    def test_aggregate_cluster_capacity_excludes_ephemeral_storage(self) -> None:
        nodes = [
            {
                "status": {
                    "allocatable": {
                        "cpu": "4",
                        "memory": "16Gi",
                        "pods": "110",
                        "google.com/tpu": "4",
                        "ephemeral-storage": "100Gi",
                    }
                }
            },
            {
                "status": {
                    "capacity": {
                        "cpu": "8",
                        "memory": "32Gi",
                        "google.com/tpu": "4",
                    }
                }
            },
        ]
        cap = _aggregate_cluster_capacity(nodes)
        self.assertAlmostEqual(cap["cpu"], 12.0)
        self.assertAlmostEqual(cap["memory"], 48.0)
        self.assertAlmostEqual(cap["google.com/tpu"], 8.0)
        self.assertNotIn("ephemeral-storage", cap)

    def test_index_pods_by_owner_skips_system_namespaces_and_indexes_labels(self) -> None:
        pods = [
            {
                "metadata": {
                    "name": "sys-pod",
                    "namespace": "infra-pods",
                    "labels": {"job-name": "ignored"},
                }
            },
            {
                "metadata": {
                    "name": "worker-0",
                    "namespace": "ai-lab",
                    "labels": {"jobset.sigs.k8s.io/jobset-name": "train-js", "job-name": "train-js-w-0"},
                    "annotations": {"kueue.x-k8s.io/workload": "jobset-train-js-12345"},
                }
            },
        ]
        indexed = _index_pods_by_owner(pods)
        self.assertNotIn(("infra-pods", "ignored"), indexed)
        self.assertEqual(len(indexed[("ai-lab", "train-js")]), 1)
        self.assertEqual(len(indexed[("ai-lab", "jobset-train-js-12345")]), 1)

    def test_completed_effective_duration_respects_window_and_cfg_cap(self) -> None:
        d_eff, d_full = completed_effective_duration(
            t_start=100.0,
            t_finish=200.0,
            start_epoch=100.0,
            end_epoch=200.0,
            cfg_dur=300.0,
        )
        self.assertAlmostEqual(d_eff, 100.0)
        self.assertAlmostEqual(d_full, 300.0)


class RunnerValidationHelperTests(unittest.TestCase):
    def test_prepare_node_manifest_and_cq_override(self) -> None:
        node = _prepare_node_manifest(
            {
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {"name": "n-0", "uid": "server-uid", "resourceVersion": "99"},
                "status": {"allocatable": {"cpu": "8", "memory": "32Gi", "pods": "110", "google.com/tpu": "4"}},
            },
            node_idx=0,
        )
        self.assertNotIn("uid", node["metadata"])
        self.assertEqual(node["metadata"]["labels"]["type"], "kwok")
        self.assertEqual(node["metadata"]["annotations"]["kueue-optimizer.gke.io/node-accel-resource"], "google.com/tpu")
        self.assertEqual(node["spec"]["podCIDR"], "10.100.0.0/24")

        cq = _prepare_kueue_or_ns_doc(
            {
                "apiVersion": "kueue.x-k8s.io/v1beta2",
                "kind": "ClusterQueue",
                "metadata": {"name": "cq-a"},
                "spec": {"preemption": {"borrowWithinCohort": {"policy": "LowerPriority"}}},
            },
            override_queueing_strategy="StrictFIFO",
            override_cq_fair_sharing_preemption=True,
        )
        self.assertEqual(cq["spec"]["queueingStrategy"], "StrictFIFO")
        self.assertEqual(cq["spec"]["fairSharing"], {"weight": 1})
        self.assertNotIn("borrowWithinCohort", cq["spec"]["preemption"])

    def test_max_workflow_name_len_and_validate_workflow_events(self) -> None:
        js = {
            "spec": {
                "replicatedJobs": [
                    {"name": "workers", "replicas": 2, "template": {"spec": {"completions": 4}}}
                ]
            }
        }
        # 62 - len("-workers-1-3-abcde"): the longest generated pod-name suffix must still fit
        self.assertEqual(_max_workflow_name_len(js, "JobSet"), 44)
        self.assertEqual(_max_workflow_name_len({"spec": {}}, "Job"), 52)
        self.assertEqual(_max_workflow_name_len({}, "LeaderWorkerSet"), 45)
        self.assertEqual(_max_workflow_name_len({}, "Pod"), 62)

        # Valid Deleted event without embedded object succeeds
        validate_workflow_events(
            [
                {
                    "timestamp": "2026-10-30T12:00:00Z",
                    "eventType": "Deleted",
                    "payload": {"objectKind": "JobSet", "name": "demo-js", "namespace": "ai-lab"},
                }
            ],
            Path("workflows.json"),
        )

        # Any other event type must embed the object
        with self.assertRaisesRegex(ValueError, "missing or non-object 'payload.object'"):
            validate_workflow_events(
                [
                    {
                        "timestamp": "2026-10-30T12:00:00Z",
                        "eventType": "New",
                        "payload": {"objectKind": "JobSet", "name": "demo-js", "namespace": "ai-lab"},
                    }
                ],
                Path("workflows.json"),
            )


class MetricsQuotaIndexTests(unittest.TestCase):
    @staticmethod
    def _series(values: list, **labels: str) -> dict:
        return {"metric": labels, "values": values}

    def test_index_quota_series_by_resource_modes(self) -> None:
        series = [
            # ts=5 is before the window start, NaN is dropped
            self._series([[5, "99"], [10, "4"], [20, "8"], [30, "NaN"]], cluster_queue="cq-a", cohort="org", flavor="f1", resource="cpu"),
            # same (entity, flavor) and timestamp from a second replica: the larger sample wins
            self._series([[20, "2"]], cluster_queue="cq-a", cohort="org", flavor="f1", resource="cpu"),
            self._series([[20, "1"]], cluster_queue="cq-b", flavor="f2", resource="cpu"),
            self._series([[20, "1"]], cluster_queue="", flavor="f1", resource="cpu"),
            self._series([[20, "1"]], cluster_queue="cq-a", flavor="f1", resource=""),
        ]

        cohorts = {"cq-b": "old"}
        by_res, entities = _index_quota_series_by_resource(
            series, 10.0, "cluster_queue", record_cq_cohort=cohorts, overwrite_cq_cohort=True
        )
        self.assertEqual(
            by_res,
            {"cpu": {10.0: {("cq-a", "f1"): 4.0}, 20.0: {("cq-a", "f1"): 8.0, ("cq-b", "f2"): 1.0}}},
        )
        self.assertEqual(entities, {"cpu": {"cq-a", "cq-b"}})
        self.assertEqual(cohorts, {"cq-a": "org", "cq-b": "old"})

        # Usage series must not replace a cohort already learned from the quota series
        cohorts_from_quota = {"cq-a": "from-quota"}
        _index_quota_series_by_resource(
            series, 10.0, "cluster_queue", record_cq_cohort=cohorts_from_quota, overwrite_cq_cohort=False
        )
        self.assertEqual(cohorts_from_quota, {"cq-a": "from-quota"})

        # Fallback when no cohort_subtree series exist: cohort totals keyed by "<cluster_queue>:<flavor>"
        by_cohort, cohort_names = _index_quota_series_by_resource(
            series, 10.0, "cohort", secondary_from_cq_flavor=True
        )
        self.assertEqual(
            by_cohort,
            {"cpu": {10.0: {("org", "cq-a:f1"): 4.0}, 20.0: {("org", "cq-a:f1"): 8.0}}},
        )
        self.assertEqual(cohort_names, {"cpu": {"org"}})


if __name__ == "__main__":
    unittest.main()
