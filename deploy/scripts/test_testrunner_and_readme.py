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

"""Unit tests for testrunner.py, generate_scenario_readme.py, and shared chamber_common.py helpers."""

from __future__ import annotations

import hashlib
import hmac
import io
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from chamber_common import (
    ChamberManager,
    ClusterStatusSnapshot,
    build_cluster_queue_manifest,
    build_topology_assignment,
    extract_pod_spec_resources,
    extract_workflow_pods_and_requests,
    format_pod_execution_seconds,
    index_execution_times,
    multiply_k8s_quantity_exact,
    parse_k8s_quantity,
    parse_positive_duration_annotation,
    to_clean_yaml,
)
import generate_scenario_readme
import kueue_metrics_report
import testrunner


class SharedHelpersAndFormattingTests(unittest.TestCase):
    def test_pod_execution_seconds_is_plain_numeric_and_roundtrips(self) -> None:
        self.assertEqual(format_pod_execution_seconds(10.0), "10")
        self.assertEqual(format_pod_execution_seconds(5.5), "5.5")
        self.assertEqual(format_pod_execution_seconds(0.02, min_seconds=0.1), "0.1")
        for val_str, expected in (
            ("10", 10.0),
            ("5.5", 5.5),
            ("0.1", 0.1),
            ("10s", 10.0),
            ("500ms", 0.5),
            ("2m", 120.0),
        ):
            anns = {"kueue-optimizer.gke.io/pod-execution-time-seconds": val_str}
            self.assertAlmostEqual(
                parse_positive_duration_annotation(anns, "kueue-optimizer.gke.io/pod-execution-time-seconds") or 0.0,
                expected,
            )
            self.assertAlmostEqual(
                kueue_metrics_report._parse_pos_float_ann(anns, "kueue-optimizer.gke.io/pod-execution-time-seconds")
                or 0.0,
                expected,
            )
        self.assertIsNone(
            kueue_metrics_report._parse_pos_float_ann(
                {"kueue-optimizer.gke.io/pod-execution-time-seconds": "0"},
                "kueue-optimizer.gke.io/pod-execution-time-seconds",
            )
        )

    def test_parse_k8s_quantity_handles_hugepages_and_binary_si(self) -> None:
        self.assertAlmostEqual(parse_k8s_quantity("500m", "cpu"), 0.5)
        self.assertAlmostEqual(parse_k8s_quantity("16Gi", "memory"), 16.0)
        self.assertAlmostEqual(parse_k8s_quantity("2Gi", "hugepages-2Mi"), float(2 * 1024**3))
        self.assertAlmostEqual(parse_k8s_quantity("8", "google.com/tpu"), 8.0)

    def test_to_clean_yaml_handles_empty_collections_in_lists(self) -> None:
        rendered = to_clean_yaml(
            [
                {"emptyDir": {}, "args": []},
                {"name": "worker", "env": []},
            ]
        )
        self.assertIn("- emptyDir: {}", rendered)
        self.assertIn("  args: []", rendered)
        self.assertNotIn('"{}"', rendered)
        self.assertNotIn('"[]"', rendered)

    def test_extract_workflow_pods_and_requests_covers_limits_init_overhead_and_lws(self) -> None:
        lws_obj = {
            "kind": "LeaderWorkerSet",
            "spec": {
                "replicas": 2,
                "leaderWorkerTemplate": {
                    "size": 3,
                    "leaderTemplate": {
                        "spec": {
                            "overhead": {"cpu": "500m", "memory": "1Gi"},
                            "initContainers": [{"resources": {"requests": {"cpu": "4", "memory": "4Gi"}}}],
                            "containers": [
                                {
                                    "resources": {
                                        "requests": {"cpu": "2", "memory": "8Gi"},
                                        "limits": {"google.com/tpu": "4"},
                                    }
                                }
                            ],
                        }
                    },
                    "workerTemplate": {
                        "spec": {
                            "containers": [
                                {
                                    "resources": {
                                        "requests": {"cpu": "2", "memory": "8Gi"},
                                        "limits": {"google.com/tpu": "4"},
                                    }
                                }
                            ]
                        }
                    },
                },
            },
        }
        pods, total_reqs, _ = extract_workflow_pods_and_requests(lws_obj, "LeaderWorkerSet")
        self.assertEqual(pods, 6)
        # 2 leaders * (max(4, 2) + 0.5 = 4.5 CPU) + 4 workers * 2 CPU = 17.0 CPU
        self.assertAlmostEqual(total_reqs["cpu"], 17.0)
        # 2 leaders * (8 + 1 = 9 GiB) + 4 workers * 8 GiB = 50.0 GiB
        self.assertAlmostEqual(total_reqs["memory"], 50.0)
        # 6 pods * 4 TPUs (from limits) = 24 TPUs
        self.assertAlmostEqual(total_reqs["google.com/tpu"], 24.0)

    def test_extract_pod_spec_resources_uses_pod_level_request_defaults(self) -> None:
        def ctr(**reqs: str) -> dict:
            return {"resources": {"requests": reqs}}

        cases = [
            (
                "a pod-level limit sets the request when no container requests it",
                {"resources": {"limits": {"cpu": "4"}}, "containers": [ctr(memory="1Gi")]},
                {"cpu": 4.0, "memory": 1.0},
            ),
            (
                "a pod-level cpu limit does not replace the container cpu request",
                {"resources": {"limits": {"cpu": "4"}}, "containers": [ctr(cpu="1")]},
                {"cpu": 1.0},
            ),
            (
                "a pod-level memory limit does not replace the container memory request",
                {"resources": {"limits": {"memory": "8Gi"}}, "containers": [ctr(cpu="1", memory="1Gi")]},
                {"cpu": 1.0, "memory": 1.0},
            ),
            (
                "a container limit counts as its request, so the pod-level limit is not used",
                {"resources": {"limits": {"cpu": "4"}}, "containers": [{"resources": {"limits": {"cpu": "2"}}}]},
                {"cpu": 2.0},
            ),
            (
                "a pod-level limit does not replace a cpu request made only by an init container",
                {"resources": {"limits": {"cpu": "4"}}, "initContainers": [ctr(cpu="2")], "containers": [ctr(memory="1Gi")]},
                {"cpu": 2.0, "memory": 1.0},
            ),
            (
                "a zero container request takes precedence over the pod-level limit",
                {"resources": {"limits": {"cpu": "4"}}, "containers": [ctr(cpu="0")]},
                {"cpu": 0.0},
            ),
            (
                "explicit pod-level requests win over their limits",
                {
                    "resources": {"requests": {"cpu": "2", "memory": "2Gi"}, "limits": {"cpu": "4", "memory": "4Gi"}},
                    "containers": [ctr(memory="1Gi")],
                },
                {"cpu": 2.0, "memory": 2.0},
            ),
            (
                "a pod-level hugepages limit sets the request even when containers request hugepages",
                {
                    "resources": {"limits": {"cpu": "1", "hugepages-2Mi": "4Mi"}},
                    "containers": [
                        {
                            "resources": {
                                "requests": {"cpu": "1", "hugepages-2Mi": "2Mi"},
                                "limits": {"cpu": "1", "hugepages-2Mi": "2Mi"},
                            }
                        }
                    ],
                },
                {"cpu": 1.0, "hugepages-2Mi": 4.0 * 1024**2},
            ),
            (
                "overhead is added to pod-level requests",
                {"resources": {"requests": {"cpu": "4"}}, "overhead": {"cpu": "500m"}, "containers": [ctr(cpu="1")]},
                {"cpu": 4.5},
            ),
            (
                "pod-level resources other than cpu, memory and hugepages are ignored",
                {
                    "resources": {"requests": {"example.com/gpu": "8"}},
                    "containers": [{"resources": {"limits": {"example.com/gpu": "2"}}}],
                },
                {"example.com/gpu": 2.0},
            ),
        ]
        for name, spec, want in cases:
            with self.subTest(name):
                self.assertEqual(extract_pod_spec_resources(spec), want)

    def test_extract_pod_spec_resources_ignores_non_mapping_pod_resources(self) -> None:
        # Not a valid PodSpec, but hand-written traces are not validated.
        for resources in ([], "invalid"):
            with self.subTest(resources=resources):
                spec = {"containers": [{"resources": {"requests": {"cpu": "1"}}}], "resources": resources}
                self.assertEqual(extract_pod_spec_resources(spec), {"cpu": 1.0})

    def test_execution_time_index_preserves_namespace_and_annotation_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "execution-time.json"
            events = [
                {
                    "timestamp": "2026-10-30T12:00:00Z",
                    "eventType": "FullExecution",
                    "payload": {
                        "namespace": "team-a",
                        "ownerKind": "JobSet",
                        "ownerName": "shared-name",
                        "workloadName": "jobset-shared-name-111",
                        "executionDurationSeconds": 42.0,
                    },
                },
                {
                    "timestamp": "2026-10-30T12:00:00Z",
                    "eventType": "Admitted",
                    "payload": {
                        "namespace": "team-b",
                        "ownerKind": "JobSet",
                        "ownerName": "admitted-only",
                        "workloadName": "jobset-admitted-only-222",
                    },
                },
            ]
            p.write_text("\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8")
            idx = index_execution_times(p)

            # Different namespace must NOT collide with team-a's shared-name
            dur_other_ns, found_other = idx.resolve_duration_sec(
                namespace="team-b", object_kind="JobSet", name="shared-name", fallback_sec=99.0
            )
            self.assertFalse(found_other)
            self.assertAlmostEqual(dur_other_ns, 99.0)

            # Admitted-only entry without execution duration must fall back to caller's annotation duration (not 10s)
            dur_adm, found_adm = idx.resolve_duration_sec(
                namespace="team-b", object_kind="JobSet", name="admitted-only", fallback_sec=0.5
            )
            self.assertFalse(found_adm)
            self.assertAlmostEqual(dur_adm, 0.5)


class ChamberLifecycleAndManifestSafetyTests(unittest.TestCase):
    def test_base_manifests_include_fair_sharing_and_chamber_label(self) -> None:
        cfg = (REPO_ROOT / "deploy/base/config/controller_manager_config.yaml").read_text(encoding="utf-8")
        self.assertIn("fairSharing:", cfg)
        self.assertIn("LessThanOrEqualToFinalShare", cfg)
        self.assertIn("LessThanInitialShare", cfg)

        ns_yaml = (REPO_ROOT / "deploy/base/namespace.yaml").read_text(encoding="utf-8")
        self.assertIn('kueue-optimizer.gke.io/chamber: "true"', ns_yaml)

        sample_q = (REPO_ROOT / "deploy/overlays/with-sample-queues/sample-queues.yaml").read_text(encoding="utf-8")
        self.assertIn("fairSharing:\n    weight: 1", sample_q)
        self.assertIn("withinClusterQueue: LowerPriority", sample_q)
        self.assertIn("reclaimWithinCohort: Any", sample_q)
        self.assertNotIn("borrowWithinCohort", sample_q)

    def test_preemption_based_fair_sharing_defaults_and_cli_toggles(self) -> None:
        # 1. build_cluster_queue_manifest defaults to Preemption-Based Fair Sharing
        cq_default = build_cluster_queue_manifest(
            name="cq-fs",
            flavor_name="default-flavor",
            resources=[{"name": "cpu", "nominalQuota": "40"}],
        )
        self.assertEqual(cq_default["spec"]["fairSharing"], {"weight": 1})
        self.assertEqual(
            cq_default["spec"]["preemption"],
            {"withinClusterQueue": "LowerPriority", "reclaimWithinCohort": "Any"},
        )

        # Classical Preemption mode (--no-fair-sharing) includes borrowWithinCohort and omits fairSharing unless weighted
        cq_classical = build_cluster_queue_manifest(
            name="cq-classic",
            flavor_name="default-flavor",
            resources=[{"name": "cpu", "nominalQuota": "40"}],
            enable_fair_sharing=False,
        )
        self.assertNotIn("fairSharing", cq_classical["spec"])
        self.assertEqual(
            cq_classical["spec"]["preemption"]["borrowWithinCohort"],
            {"policy": "LowerPriority"},
        )

        # 2. ChamberManager toggles controller_manager_config.yaml fairSharing on (default) vs off (--no-fair-sharing)
        mgr_on = ChamberManager(chamber_namespace="kueue-optimizer-chamber-fs-on", apiserver_port=16470)
        mgr_off = ChamberManager(
            chamber_namespace="kueue-optimizer-chamber-fs-off",
            apiserver_port=16471,
            enable_fair_sharing=False,
        )
        try:
            dst_on = mgr_on.prepare_tmp_deploy_dir(prefer_cluster_pki=False, check_cluster_ports=False)
            cfg_on = (dst_on / "config" / "controller_manager_config.yaml").read_text(encoding="utf-8")
            self.assertIn("fairSharing:", cfg_on)
            self.assertIn("LessThanOrEqualToFinalShare", cfg_on)

            dst_off = mgr_off.prepare_tmp_deploy_dir(prefer_cluster_pki=False, check_cluster_ports=False)
            cfg_off = (dst_off / "config" / "controller_manager_config.yaml").read_text(encoding="utf-8")
            self.assertNotIn("fairSharing:", cfg_off)
            self.assertNotIn("LessThanOrEqualToFinalShare", cfg_off)
        finally:
            mgr_on.close()
            mgr_off.close()

        # 3. testrunner preserves scraped infra.json ClusterQueue specs by default, and overrides when requested
        demo_dir = REPO_ROOT / "testscenarios/demo"
        bundle_verbatim = testrunner.load_scenario_bundle(scenario_dir=demo_dir)
        cq_verbatim = next(
            d for d in bundle_verbatim.prereq_and_queue_docs if d.get("kind") == "ClusterQueue"
        )
        self.assertNotIn("fairSharing", cq_verbatim["spec"])
        self.assertEqual(
            cq_verbatim["spec"].get("preemption", {}).get("reclaimWithinCohort", "Never"),
            "Never",
        )

        bundle_override = testrunner.load_scenario_bundle(
            scenario_dir=demo_dir,
            override_cq_fair_sharing_preemption=True,
        )
        cq_override = next(
            d for d in bundle_override.prereq_and_queue_docs if d.get("kind") == "ClusterQueue"
        )
        self.assertEqual(cq_override["spec"]["fairSharing"], {"weight": 1})
        self.assertEqual(cq_override["spec"]["preemption"]["withinClusterQueue"], "LowerPriority")
        self.assertEqual(cq_override["spec"]["preemption"]["reclaimWithinCohort"], "Any")
        self.assertNotIn("borrowWithinCohort", cq_override["spec"]["preemption"])

    def test_patch_registry_manifest_handles_arbitrary_memory_and_clamps_request(self) -> None:
        mgr = ChamberManager(
            chamber_namespace="kueue-optimizer-chamber-mem",
            apiserver_port=16460,
            registry_port=30517,
            registry_memory_limit="512Mi",
        )
        try:
            with tempfile.TemporaryDirectory() as tmp:
                reg_path = Path(tmp) / "registry.yaml"
                reg_path.write_text(
                    "\n".join(
                        [
                            "          resources:",
                            "            requests:",
                            "              cpu: 50m  # baseline request",
                            '              memory: "2Gi"',
                            "            limits:",
                            '              cpu: "2"',
                            "              nvidia.com/gpu: 0",
                            '              memory: "11059Mi"  # non-4Gi limit',
                            "      volumes:",
                            "        - name: registry-ram",
                            "          emptyDir:",
                            "            medium: Memory",
                            "            sizeLimit: 11059Mi",
                            "    - name: registry",
                            "      port: 5000",
                            "      nodePort: 30500",
                        ]
                    )
                    + "\n",
                    encoding="utf-8",
                )
                mgr._patch_registry_manifest(Path(tmp), 30517, "512Mi")
                patched = reg_path.read_text(encoding="utf-8")
                self.assertIn("sizeLimit: 512Mi", patched)
                self.assertIn("memory: 512Mi  # non-4Gi limit", patched)
                self.assertEqual(patched.count("memory: 512Mi"), 2)
                self.assertNotIn("11059Mi", patched)
                self.assertNotIn("2Gi", patched)
                self.assertIn("nodePort: 30517", patched)

                # Also verify requests without memory does NOT leak across into limits.memory
                reg_path.write_text(
                    "\n".join(
                        [
                            "          resources:",
                            "            requests:",
                            "              cpu: 50m",
                            "            limits:",
                            "              cpu: 500m",
                            "              memory: 16Gi",
                            "          emptyDir:",
                            "            medium: Memory",
                            "            sizeLimit: 16Gi",
                        ]
                    )
                    + "\n",
                    encoding="utf-8",
                )
                mgr._patch_registry_manifest(Path(tmp), 30517, "6Gi")
                patched2 = reg_path.read_text(encoding="utf-8")
                self.assertIn("sizeLimit: 6Gi", patched2)
                self.assertIn("memory: 6Gi", patched2)
                self.assertEqual(patched2.count("memory: 6Gi"), 1)
        finally:
            mgr.close()

    def test_pkill_chamber_port_forward_anchors_namespace_and_port(self) -> None:
        mgr = ChamberManager(chamber_namespace="kueue-optimizer-chamber-1", apiserver_port=16443)
        try:
            captured_patterns: list[str] = []

            def fake_run(cmd: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
                captured_patterns.append(cmd[-1])
                return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")

            with mock.patch("chamber.k8s_client.subprocess.run", side_effect=fake_run):
                mgr._pkill_chamber_port_forward("kwok-kube-apiserver", "16443:6443")

            self.assertEqual(len(captured_patterns), 1)
            pat = re.compile(captured_patterns[0])
            self.assertIsNotNone(
                pat.search("kubectl -n kueue-optimizer-chamber-1 port-forward svc/kwok-kube-apiserver 16443:6443")
            )
            self.assertIsNone(
                pat.search("kubectl -n kueue-optimizer-chamber-10 port-forward svc/kwok-kube-apiserver 16443:6443")
            )
            self.assertIsNone(
                pat.search("kubectl -n kueue-optimizer-chamber-1 port-forward svc/kwok-kube-apiserver 164430:6443")
            )
        finally:
            mgr.close()

    def test_cleanup_chamber_verifies_ownership_label_and_delete_exit_code(self) -> None:
        mgr = ChamberManager(chamber_namespace="default", apiserver_port=16443)
        try:
            unowned_ns = {"metadata": {"name": "default", "labels": {"kubernetes.io/metadata.name": "default"}}}
            get_ok = subprocess.CompletedProcess(
                args=["kubectl"], returncode=0, stdout=json.dumps(unowned_ns), stderr=""
            )
            with (
                mock.patch.object(mgr, "_pkill_chamber_port_forward"),
                mock.patch.object(mgr, "host_kubectl", return_value=get_ok) as m_kubectl,
            ):
                with self.assertRaisesRegex(RuntimeError, "Refusing to delete namespace 'default'"):
                    mgr.cleanup_chamber()
                # Must never issue `delete namespace` on an unowned namespace
                self.assertEqual(m_kubectl.call_count, 1)
                self.assertEqual(m_kubectl.call_args[0][0], ["get", "namespace", "-o", "json", "--", "default"])
        finally:
            mgr.close()

        # app.kubernetes.io/part-of alone must NOT grant chamber ownership
        mgr_part_of_only = ChamberManager(chamber_namespace="shared-tools", apiserver_port=16443)
        try:
            part_of_ns = {
                "metadata": {
                    "name": "shared-tools",
                    "labels": {"app.kubernetes.io/part-of": "kueue-optimizer"},
                }
            }
            get_part_of = subprocess.CompletedProcess(
                args=["kubectl"], returncode=0, stdout=json.dumps(part_of_ns), stderr=""
            )
            with (
                mock.patch.object(mgr_part_of_only, "_pkill_chamber_port_forward"),
                mock.patch.object(mgr_part_of_only, "host_kubectl", return_value=get_part_of) as m_kubectl,
            ):
                with self.assertRaisesRegex(RuntimeError, "Refusing to delete namespace 'shared-tools'"):
                    mgr_part_of_only.cleanup_chamber()
                self.assertEqual(m_kubectl.call_count, 1)
        finally:
            mgr_part_of_only.close()

        # Pre-apply check in deploy_chamber and stage_registry_only refuses unowned existing namespaces BEFORE apply
        mgr_prod = ChamberManager(chamber_namespace="my-team-prod", apiserver_port=16445)
        try:
            prod_ns = {"metadata": {"name": "my-team-prod", "labels": {"env": "prod"}}}
            get_prod = subprocess.CompletedProcess(
                args=["kubectl"], returncode=0, stdout=json.dumps(prod_ns), stderr=""
            )
            with mock.patch.object(mgr_prod, "host_kubectl", return_value=get_prod) as m_kubectl:
                with self.assertRaisesRegex(RuntimeError, "Refusing to deploy chamber into existing namespace"):
                    mgr_prod.deploy_chamber()
                with self.assertRaisesRegex(RuntimeError, "Refusing to deploy chamber into existing namespace"):
                    mgr_prod.stage_registry_only()
                for call in m_kubectl.call_args_list:
                    self.assertNotIn("apply", call.args[0])
                self.assertFalse(mgr_prod.created_namespace_in_this_run)
        finally:
            mgr_prod.close()

        mgr_owned = ChamberManager(chamber_namespace="kueue-optimizer-chamber-del", apiserver_port=16444)
        try:
            owned_ns = {
                "metadata": {
                    "name": "kueue-optimizer-chamber-del",
                    "labels": {"kueue-optimizer.gke.io/chamber": "true"},
                }
            }
            get_owned = subprocess.CompletedProcess(
                args=["kubectl"], returncode=0, stdout=json.dumps(owned_ns), stderr=""
            )
            del_fail = subprocess.CompletedProcess(
                args=["kubectl"], returncode=1, stdout="", stderr="Forbidden: RBAC denied"
            )
            with (
                mock.patch.object(mgr_owned, "_pkill_chamber_port_forward"),
                mock.patch.object(mgr_owned, "host_kubectl", side_effect=[get_owned, del_fail]) as m_kubectl,
            ):
                with self.assertRaisesRegex(RuntimeError, "Failed to delete chamber namespace"):
                    mgr_owned.cleanup_chamber()
                self.assertEqual(
                    m_kubectl.call_args_list[1].args[0],
                    ["delete", "namespace", "--ignore-not-found", "--", "kueue-optimizer-chamber-del"],
                )
        finally:
            mgr_owned.close()

    def test_testrunner_cli_dry_run_precedes_cleanup_and_reuse_preserves_chamber(self) -> None:
        # 1. --cleanup --dry-run must NOT call cleanup_chamber
        with (
            mock.patch.object(ChamberManager, "cleanup_chamber") as m_cleanup,
            mock.patch.object(ChamberManager, "deploy_chamber") as m_deploy,
            mock.patch("sys.stdout", new_callable=io.StringIO),
        ):
            rc = testrunner.main(
                [str(REPO_ROOT / "testscenarios/demo"), "--cleanup", "--dry-run", "--namespace", "default"]
            )
            self.assertEqual(rc, 0)
            m_cleanup.assert_not_called()
            m_deploy.assert_not_called()

        # 2a. Failure BEFORE creating namespace (e.g., pre-existing chamber or pre-apply error) must NOT delete it
        with (
            mock.patch.object(ChamberManager, "deploy_chamber", side_effect=RuntimeError("pre-apply boom")),
            mock.patch.object(ChamberManager, "cleanup_chamber") as m_cleanup,
            mock.patch("sys.stdout", new_callable=io.StringIO),
        ):
            with self.assertRaisesRegex(RuntimeError, "pre-apply boom"):
                testrunner.main(
                    [
                        str(REPO_ROOT / "testscenarios/demo"),
                        "--namespace",
                        "kueue-optimizer-chamber-preexisting",
                        "--port",
                        "16490",
                    ]
                )
            m_cleanup.assert_not_called()

        # 2b. Failure AFTER this run created the namespace DOES trigger finally cleanup_chamber
        def _create_then_fail(self_mgr: ChamberManager, initial_nodes: object = None) -> None:
            self_mgr._created_namespace_in_this_run = True
            raise RuntimeError("rollout boom")

        with (
            mock.patch.object(ChamberManager, "deploy_chamber", autospec=True, side_effect=_create_then_fail),
            mock.patch.object(ChamberManager, "cleanup_chamber") as m_cleanup,
            mock.patch("sys.stdout", new_callable=io.StringIO),
        ):
            with self.assertRaisesRegex(RuntimeError, "rollout boom"):
                testrunner.main(
                    [
                        str(REPO_ROOT / "testscenarios/demo"),
                        "--namespace",
                        "kueue-optimizer-chamber-fail",
                        "--port",
                        "16490",
                    ]
                )
            m_cleanup.assert_called_once()

        # 3. --skip-chamber-deploy preserves the reused chamber even if setup raises
        with (
            mock.patch.object(ChamberManager, "detect_live_fair_sharing_enabled", return_value=True),
            mock.patch.object(ChamberManager, "ensure_kwok_connection", side_effect=RuntimeError("conn fail")),
            mock.patch.object(ChamberManager, "cleanup_chamber") as m_cleanup,
            mock.patch("sys.stdout", new_callable=io.StringIO) as out_buf,
        ):
            with self.assertRaisesRegex(RuntimeError, "conn fail"):
                testrunner.main(
                    [
                        str(REPO_ROOT / "testscenarios/demo"),
                        "--skip-chamber-deploy",
                        "--namespace",
                        "kueue-optimizer-chamber-reused",
                        "--port",
                        "16491",
                    ]
                )
            m_cleanup.assert_not_called()
            self.assertIn("[detected from live chamber]", out_buf.getvalue())

    def test_fair_sharing_flag_rejected_with_skip_chamber_deploy_and_live_detection(self) -> None:
        # 1. Explicit --fair-sharing or --no-fair-sharing with --skip-chamber-deploy must be rejected
        for flag in ("--fair-sharing", "--no-fair-sharing"):
            with mock.patch("sys.stderr", new_callable=io.StringIO) as err_buf:
                with self.assertRaises(SystemExit) as ctx:
                    testrunner.main(
                        [
                            str(REPO_ROOT / "testscenarios/demo"),
                            "--skip-chamber-deploy",
                            flag,
                            "--dry-run",
                        ]
                    )
                self.assertEqual(ctx.exception.code, 2)
                self.assertIn("cannot be combined with --skip-chamber-deploy", err_buf.getvalue())

        # 2. --skip-chamber-deploy --dry-run (without explicit flag) prints inherited notice
        with mock.patch("sys.stdout", new_callable=io.StringIO) as out_buf:
            rc = testrunner.main([str(REPO_ROOT / "testscenarios/demo"), "--skip-chamber-deploy", "--dry-run"])
            self.assertEqual(rc, 0)
            self.assertIn("inherited from running chamber (--skip-chamber-deploy)", out_buf.getvalue())

        # 3. detect_live_fair_sharing_enabled parses ConfigMap/kueue-manager-config accurately
        mgr = ChamberManager(chamber_namespace="kueue-optimizer-chamber-live", apiserver_port=16492)
        try:
            cm_on = {"data": {"controller_manager_config.yaml": "kind: Configuration\nfairSharing:\n  preemptionStrategies: [LessThanInitialShare]\n"}}
            cm_off = {"data": {"controller_manager_config.yaml": "kind: Configuration\nwaitForPodsReady:\n  enable: false\n"}}
            with mock.patch.object(
                mgr,
                "host_kubectl",
                return_value=subprocess.CompletedProcess(args=["kubectl"], returncode=0, stdout=json.dumps(cm_on), stderr=""),
            ):
                self.assertIs(mgr.detect_live_fair_sharing_enabled(), True)
            with mock.patch.object(
                mgr,
                "host_kubectl",
                return_value=subprocess.CompletedProcess(args=["kubectl"], returncode=0, stdout=json.dumps(cm_off), stderr=""),
            ):
                self.assertIs(mgr.detect_live_fair_sharing_enabled(), False)
        finally:
            mgr.close()

    def test_trace_gvk_allowlist_dns_labels_strict_timestamps_and_delete_dash_dash(self) -> None:
        demo_dir = REPO_ROOT / "testscenarios/demo"

        def _make_scenario_copy(root: Path) -> Path:
            dst = root / "scen"
            dst.mkdir(parents=True, exist_ok=True)
            for fn in ("infra.json", "workflows.json", "execution-time.json"):
                (dst / fn).write_text((demo_dir / fn).read_text(encoding="utf-8"), encoding="utf-8")
            return dst

        with tempfile.TemporaryDirectory() as tmp:
            tmp_p = Path(tmp)

            # 1. Disallowed embedded object kind (e.g. ClusterExec / CustomResourceDefinition) in workflows.json
            s_bad_wf_gvk = _make_scenario_copy(tmp_p / "bad_wf_gvk")
            bad_wf_ev = {
                "timestamp": "2026-10-30T12:06:05Z",
                "eventType": "New",
                "workloadState": "NotAdmitted",
                "payload": {
                    "objectKind": "Job",
                    "name": "evil-job",
                    "namespace": "ai-lab",
                    "localQueueName": "ai-lab-lq",
                    "object": {
                        "apiVersion": "kwok.x-k8s.io/v1alpha1",
                        "kind": "ClusterExec",
                        "metadata": {"name": "evil-job", "namespace": "ai-lab"},
                    },
                },
            }
            with (s_bad_wf_gvk / "workflows.json").open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(bad_wf_ev) + "\n")
            with self.assertRaisesRegex(ValueError, "does not match embedded object kind"):
                testrunner.load_scenario_bundle(scenario_dir=s_bad_wf_gvk)

            # 2. Matching objectKind == kind, but disallowed GVK
            s_bad_crd = _make_scenario_copy(tmp_p / "bad_crd")
            bad_crd_ev = {
                "timestamp": "2026-10-30T12:06:05Z",
                "eventType": "New",
                "workloadState": "NotAdmitted",
                "payload": {
                    "objectKind": "CustomResourceDefinition",
                    "name": "evil-crd",
                    "namespace": "ai-lab",
                    "localQueueName": "",
                    "object": {
                        "apiVersion": "apiextensions.k8s.io/v1",
                        "kind": "CustomResourceDefinition",
                        "metadata": {"name": "evil-crd"},
                    },
                },
            }
            with (s_bad_crd / "workflows.json").open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(bad_crd_ev) + "\n")
            with self.assertRaisesRegex(ValueError, "disallowed workflow objectKind"):
                testrunner.load_scenario_bundle(scenario_dir=s_bad_crd)

            # 3. Disallowed GVK in infra.json
            s_bad_inf = _make_scenario_copy(tmp_p / "bad_inf")
            bad_inf_ev = {
                "timestamp": "2026-10-30T12:05:53Z",
                "eventType": "New",
                "payload": {
                    "apiVersion": "apiregistration.k8s.io/v1",
                    "kind": "APIService",
                    "metadata": {"name": "v1-evil"},
                },
            }
            with (s_bad_inf / "infra.json").open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(bad_inf_ev) + "\n")
            with self.assertRaisesRegex(ValueError, "disallowed infra"):
                testrunner.load_scenario_bundle(scenario_dir=s_bad_inf)

            # 4. Non-DNS-label namespace / name (e.g. "--all" flag injection attempt)
            s_bad_dns = _make_scenario_copy(tmp_p / "bad_dns")
            bad_ns_ev = {
                "timestamp": "2026-10-30T12:05:53Z",
                "eventType": "New",
                "payload": {
                    "apiVersion": "v1",
                    "kind": "Namespace",
                    "metadata": {"name": "--all"},
                },
            }
            with (s_bad_dns / "infra.json").open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(bad_ns_ev) + "\n")
            with self.assertRaisesRegex(ValueError, "RFC 1123 DNS label"):
                testrunner.load_scenario_bundle(scenario_dir=s_bad_dns)

            # 5. Malformed timestamp in workflows.json or infra.json raises ValueError instead of falling back to t0
            s_bad_ts = _make_scenario_copy(tmp_p / "bad_ts")
            bad_ts_ev = {
                "timestamp": "not-an-rfc3339-timestamp",
                "eventType": "New",
                "payload": {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "extra-ns"}},
            }
            with (s_bad_ts / "infra.json").open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(bad_ts_ev) + "\n")
            with self.assertRaisesRegex(ValueError, "invalid RFC3339 timestamp"):
                testrunner.load_scenario_bundle(scenario_dir=s_bad_ts)

        # 6. cleanup_scenario_workloads passes `--` before dynamic namespace names
        bundle = testrunner.load_scenario_bundle(scenario_dir=demo_dir)
        mgr = ChamberManager(chamber_namespace="kueue-optimizer-chamber-clean", apiserver_port=16493)
        try:
            with (
                mock.patch.object(mgr, "ensure_kwok_connection"),
                mock.patch.object(mgr, "kwok_kubectl") as m_kwok,
                mock.patch("sys.stdout", new_callable=io.StringIO),
            ):
                testrunner.cleanup_scenario_workloads(mgr, bundle)
                ns_del_calls = [
                    c.args[0] for c in m_kwok.call_args_list if c.args[0][:2] == ["delete", "namespace"]
                ]
                self.assertTrue(ns_del_calls)
                for cmd_args in ns_del_calls:
                    self.assertEqual(cmd_args[:4], ["delete", "namespace", "--ignore-not-found", "--"])
        finally:
            mgr.close()


class ScenarioParsingAndReadmeConsistencyTests(unittest.TestCase):
    def test_demo_scenario_readme_matches_testrunner_and_committed_file(self) -> None:
        demo_dir = REPO_ROOT / "testscenarios/demo"
        bundle = testrunner.load_scenario_bundle(
            scenario_dir=demo_dir,
            speedup=1.0,
            skip_initial_offset=True,
            default_exec_sec=10.0,
            min_pod_duration_sec=0.1,
            initial_window_sec=1.0,
            infra_pods_namespace="infra-pods",
            override_queueing_strategy=None,
        )
        self.assertEqual(len(bundle.initial_pods), 0)
        self.assertEqual(len(bundle.scheduled_workflows), 20)
        self.assertEqual(sum(w.pod_count for w in bundle.scheduled_workflows), 154)

        # Verify injected pod-execution-time-seconds annotations are plain numeric strings without unit suffixes
        for wf in bundle.scheduled_workflows:
            spec_str = json.dumps(wf.manifest)
            for match in re.finditer(r'"kueue-optimizer\.gke\.io/pod-execution-time-seconds":\s*"([^"]+)"', spec_str):
                ann_val = match.group(1)
                float(ann_val)  # Must parse directly as float without unit suffix

        generated_md = generate_scenario_readme.generate_readme_for_scenario(demo_dir)
        self.assertIn("**0** pods across **0** / 10 nodes", generated_md)
        self.assertIn("**20** `NotAdmitted` arrivals (154 total pods;", generated_md)
        committed_md = (demo_dir / "README.md").read_text(encoding="utf-8")
        self.assertEqual(generated_md, committed_md)

    def test_hash_names_anonymized_scenario_filters_workflow_pods_and_keeps_true_t0_pods(self) -> None:
        """Simulate cluster-scraper --hash-names (independent HMAC-SHA256 h-<32hex> names) + 1s floor truncation."""
        demo_dir = REPO_ROOT / "testscenarios/demo"
        secret = b"unit-test-hmac-key"

        def hname(raw: str) -> str:
            if not raw:
                return raw
            return "h-" + hmac.new(secret, raw.encode("utf-8"), hashlib.sha256).hexdigest()[:32]

        with tempfile.TemporaryDirectory() as tmp:
            anon_dir = Path(tmp) / "anon_demo"
            anon_dir.mkdir(parents=True, exist_ok=True)

            # Anonymize infra.json + inject 1 genuine t0 running pod at t0=12:05:52.657680Z
            infra_lines: list[str] = []
            for raw_line in (demo_dir / "infra.json").read_text(encoding="utf-8").splitlines():
                if not raw_line.strip():
                    continue
                ev = json.loads(raw_line)
                pl = ev.get("payload") or {}
                kind = pl.get("kind") or pl.get("objectKind")
                if kind == "Pod":
                    pl["name"] = hname(str(pl.get("name") or ""))
                    pl["namespace"] = hname(str(pl.get("namespace") or "default"))
                elif kind == "Namespace":
                    pl["metadata"]["name"] = hname(pl["metadata"]["name"])
                elif kind == "LocalQueue":
                    pl["metadata"]["name"] = hname(pl["metadata"]["name"])
                    pl["metadata"]["namespace"] = hname(pl["metadata"]["namespace"])
                    pl["spec"]["clusterQueue"] = hname(pl["spec"]["clusterQueue"])
                elif kind == "ClusterQueue":
                    pl["metadata"]["name"] = hname(pl["metadata"]["name"])
                infra_lines.append(json.dumps(ev, separators=(",", ":")))

            # Add one genuine background pod already running at t0 (before any workflow admission)
            true_t0_pod = {
                "timestamp": "2026-10-30T12:05:52.657680000Z",
                "eventType": "New",
                "payload": {
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "name": hname("preexisting-daemon-pod-0"),
                    "namespace": hname("ai-lab"),
                    "uid": "00000000-0000-0000-0000-000000000001",
                    "firstNoticeTime": "2026-10-30T12:05:52Z",
                    "schedulingTime": "2026-10-30T12:05:50Z",
                    "placement": "kwok-node-0",
                    "resourceRequest": {"cpu": "2", "memory": "4Gi"},
                },
            }
            infra_lines.insert(0, json.dumps(true_t0_pod, separators=(",", ":")))
            (anon_dir / "infra.json").write_text("\n".join(infra_lines) + "\n", encoding="utf-8")

            # Anonymize workflows.json (independent hashes for JobSet names and child Job names)
            wf_lines: list[str] = []
            for raw_line in (demo_dir / "workflows.json").read_text(encoding="utf-8").splitlines():
                if not raw_line.strip():
                    continue
                ev = json.loads(raw_line)
                pl = ev.get("payload") or {}
                obj = pl.get("object") or {}
                meta = obj.get("metadata") or {}
                pl["name"] = hname(str(pl.get("name") or ""))
                pl["namespace"] = hname(str(pl.get("namespace") or "default"))
                if pl.get("localQueueName"):
                    pl["localQueueName"] = hname(str(pl["localQueueName"]))
                meta["name"] = pl["name"]
                meta["namespace"] = pl["namespace"]
                if isinstance(meta.get("labels"), dict) and "kueue.x-k8s.io/queue-name" in meta["labels"]:
                    meta["labels"]["kueue.x-k8s.io/queue-name"] = hname(meta["labels"]["kueue.x-k8s.io/queue-name"])
                if isinstance(meta.get("labels"), dict) and "jobset.sigs.k8s.io/jobset-name" in meta["labels"]:
                    meta["labels"]["jobset.sigs.k8s.io/jobset-name"] = hname(
                        meta["labels"]["jobset.sigs.k8s.io/jobset-name"]
                    )
                for oref in meta.get("ownerReferences") or []:
                    if isinstance(oref, dict) and oref.get("name"):
                        oref["name"] = hname(str(oref["name"]))
                wf_lines.append(json.dumps(ev, separators=(",", ":")))
            (anon_dir / "workflows.json").write_text("\n".join(wf_lines) + "\n", encoding="utf-8")

            # Anonymize execution-time.json
            exec_lines: list[str] = []
            for raw_line in (demo_dir / "execution-time.json").read_text(encoding="utf-8").splitlines():
                if not raw_line.strip():
                    continue
                ev = json.loads(raw_line)
                pl = ev.get("payload") or {}
                pl["namespace"] = hname(str(pl.get("namespace") or "default"))
                pl["ownerName"] = hname(str(pl.get("ownerName") or ""))
                pl["workloadName"] = hname(str(pl.get("workloadName") or ""))
                if pl.get("localQueueName"):
                    pl["localQueueName"] = hname(str(pl["localQueueName"]))
                if pl.get("clusterQueueName"):
                    pl["clusterQueueName"] = hname(str(pl["clusterQueueName"]))
                exec_lines.append(json.dumps(ev, separators=(",", ":")))
            (anon_dir / "execution-time.json").write_text("\n".join(exec_lines) + "\n", encoding="utf-8")

            bundle = testrunner.load_scenario_bundle(
                scenario_dir=anon_dir,
                speedup=1.0,
                skip_initial_offset=True,
                default_exec_sec=10.0,
                min_pod_duration_sec=0.1,
                initial_window_sec=1.0,
                infra_pods_namespace="infra-pods",
                override_queueing_strategy=None,
            )
            # Only the 1 genuine t0 pod should remain; all 154 workflow-spawned hashed pods must be excluded!
            self.assertEqual(len(bundle.initial_pods), 1)
            self.assertEqual(len(bundle.scheduled_workflows), 20)
            self.assertEqual(sum(w.pod_count for w in bundle.scheduled_workflows), 154)
            with mock.patch("sys.stdout", new_callable=io.StringIO) as outline_buf:
                testrunner.print_scenario_outline(bundle, speedup=1.0)
            self.assertIn("1 running pod(s)", outline_buf.getvalue())

            anon_md = generate_scenario_readme.generate_readme_for_scenario(anon_dir)
            self.assertIn("**1** pods across **1** / 10 nodes", anon_md)
            self.assertIn("**20** `NotAdmitted` arrivals (154 total pods;", anon_md)


class StartupPodGroupAndQuotaSyncTests(unittest.TestCase):
    def test_multiply_k8s_quantity_exact_and_build_topology_assignment(self) -> None:
        self.assertEqual(multiply_k8s_quantity_exact("250m", 4, "cpu"), "1")
        self.assertEqual(multiply_k8s_quantity_exact("250m", 3, "cpu"), "750m")
        self.assertEqual(multiply_k8s_quantity_exact("0.5", 3, "cpu"), "1500m")
        self.assertEqual(multiply_k8s_quantity_exact("16Gi", 4, "memory"), "64Gi")
        self.assertEqual(multiply_k8s_quantity_exact("1.5Gi", 2, "memory"), "3Gi")
        self.assertEqual(multiply_k8s_quantity_exact("512Mi", 3, "memory"), "1536Mi")
        self.assertEqual(multiply_k8s_quantity_exact("4", 8, "google.com/tpu"), "32")

        nodes = {
            "n-0": {"metadata": {"labels": {"kubernetes.io/hostname": "n-0", "rack": "r0", "sub": "s0"}}},
            "n-1": {"metadata": {"labels": {"kubernetes.io/hostname": "n-1", "rack": "r0", "sub": "s1"}}},
        }
        # Hostname lowest-level shortcut
        ta_host = build_topology_assignment(
            ["n-0", "n-0", "n-1", "n-1"],
            ["rack", "kubernetes.io/hostname"],
            nodes,
        )
        self.assertEqual(
            ta_host,
            {
                "levels": ["kubernetes.io/hostname"],
                "slices": [
                    {
                        "domainCount": 2,
                        "valuesPerLevel": [{"individual": {"roots": ["n-0", "n-1"]}}],
                        "podCounts": {"universal": 2},
                    }
                ],
            },
        )
        # Multi-level non-hostname topology
        ta_rack = build_topology_assignment(["n-0", "n-0", "n-1"], ["rack", "sub"], nodes)
        self.assertEqual(
            ta_rack,
            {
                "levels": ["rack", "sub"],
                "slices": [
                    {
                        "domainCount": 2,
                        "valuesPerLevel": [
                            {"universal": "r0"},
                            {"individual": {"roots": ["s0", "s1"]}},
                        ],
                        "podCounts": {"individual": [2, 1]},
                    }
                ],
            },
        )

    def test_t0_podgroup_bootstrap_separates_kueue_workloads_and_infra_pods(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            scen = Path(tmp) / "t0_sync"
            scen.mkdir(parents=True, exist_ok=True)
            t0 = "2026-11-20T10:00:00Z"
            t_arr = "2026-11-20T10:00:05Z"
            t_del = "2026-11-20T10:00:45Z"

            infra_events = [
                {
                    "timestamp": t0,
                    "eventType": "New",
                    "payload": {
                        "apiVersion": "v1",
                        "kind": "Node",
                        "metadata": {"name": "node-0", "labels": {"kubernetes.io/hostname": "node-0", "rack": "r0", "instance": "tpu"}},
                        "status": {"allocatable": {"cpu": "16", "memory": "64Gi", "google.com/tpu": "4", "pods": "110"}},
                    },
                },
                {
                    "timestamp": t0,
                    "eventType": "New",
                    "payload": {
                        "apiVersion": "v1",
                        "kind": "Node",
                        "metadata": {"name": "node-1", "labels": {"kubernetes.io/hostname": "node-1", "rack": "r0", "instance": "tpu"}},
                        "status": {"allocatable": {"cpu": "16", "memory": "64Gi", "google.com/tpu": "4", "pods": "110"}},
                    },
                },
                {
                    "timestamp": t0,
                    "eventType": "New",
                    "payload": {
                        "apiVersion": "kueue.x-k8s.io/v1beta2",
                        "kind": "Topology",
                        "metadata": {"name": "tpu-topo"},
                        "spec": {"levels": [{"nodeLabel": "rack"}, {"nodeLabel": "kubernetes.io/hostname"}]},
                    },
                },
                {
                    "timestamp": t0,
                    "eventType": "New",
                    "payload": {
                        "apiVersion": "kueue.x-k8s.io/v1beta2",
                        "kind": "ResourceFlavor",
                        "metadata": {"name": "tpu-rf"},
                        "spec": {"nodeLabels": {"instance": "tpu"}, "topologyName": "tpu-topo"},
                    },
                },
                {
                    "timestamp": t0,
                    "eventType": "New",
                    "payload": {
                        "apiVersion": "kueue.x-k8s.io/v1beta2",
                        "kind": "ClusterQueue",
                        "metadata": {"name": "tpu-cq"},
                        "spec": {
                            "queueingStrategy": "BestEffortFIFO",
                            "resourceGroups": [
                                {
                                    "coveredResources": ["google.com/tpu", "cpu", "memory"],
                                    "flavors": [
                                        {
                                            "name": "tpu-rf",
                                            "resources": [
                                                {"name": "google.com/tpu", "nominalQuota": "8"},
                                                {"name": "cpu", "nominalQuota": "32"},
                                                {"name": "memory", "nominalQuota": "128Gi"},
                                            ],
                                        }
                                    ],
                                }
                            ],
                        },
                    },
                },
                {
                    "timestamp": t0,
                    "eventType": "New",
                    "payload": {
                        "apiVersion": "kueue.x-k8s.io/v1beta2",
                        "kind": "LocalQueue",
                        "metadata": {"name": "tpu-lq", "namespace": "ai-lab"},
                        "spec": {"clusterQueue": "tpu-cq"},
                    },
                },
                # Class 1 pod #0 (Kueue-admitted JobSet at t0)
                {
                    "timestamp": t0,
                    "eventType": "New",
                    "payload": {
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "name": "running-js-w-0",
                        "namespace": "ai-lab",
                        "placement": "node-0",
                        "resourceRequest": {"cpu": "4", "memory": "16Gi", "google.com/tpu": "4"},
                        "firstNoticeTime": t0,
                        "schedulingTime": t0,
                        "workloadName": "jobset-running-js-111",
                        "ownerKind": "JobSet",
                        "ownerName": "running-js",
                        "podSetName": "worker",
                        "isTAS": True,
                    },
                },
                # Class 1 pod #1
                {
                    "timestamp": t0,
                    "eventType": "New",
                    "payload": {
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "name": "running-js-w-1",
                        "namespace": "ai-lab",
                        "placement": "node-1",
                        "resourceRequest": {"cpu": "4", "memory": "16Gi", "google.com/tpu": "2"},
                        "firstNoticeTime": t0,
                        "schedulingTime": t0,
                        "workloadName": "jobset-running-js-111",
                        "ownerKind": "JobSet",
                        "ownerName": "running-js",
                        "podSetName": "head",
                        "isTAS": True,
                    },
                },
                {
                    "timestamp": t_del,
                    "eventType": "Deleted",
                    "payload": {
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "name": "running-js-w-0",
                        "namespace": "ai-lab",
                        "placement": "node-0",
                        "resourceRequest": {"cpu": "4", "memory": "16Gi", "google.com/tpu": "4"},
                        "firstNoticeTime": t0,
                        "deletionTime": t_del,
                    },
                },
                # Class 2 non-Kueue DaemonSet pod at t0 (no workloadName / ownerName)
                {
                    "timestamp": t0,
                    "eventType": "New",
                    "payload": {
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "name": "node-exporter-0",
                        "namespace": "kube-system",
                        "placement": "node-0",
                        "resourceRequest": {"cpu": "500m", "memory": "1Gi"},
                        "firstNoticeTime": t0,
                        "schedulingTime": t0,
                    },
                },
            ]
            wf_events = [
                {
                    "timestamp": t0,
                    "eventType": "New",
                    "workloadState": "Admitted",
                    "payload": {
                        "objectKind": "JobSet",
                        "name": "running-js",
                        "namespace": "ai-lab",
                        "creationTime": t0,
                        "localQueueName": "tpu-lq",
                        "object": {
                            "apiVersion": "jobset.x-k8s.io/v1alpha2",
                            "kind": "JobSet",
                            "metadata": {
                                "name": "running-js",
                                "namespace": "ai-lab",
                                "labels": {
                                    "kueue.x-k8s.io/queue-name": "tpu-lq",
                                    "kueue.x-k8s.io/priority-class": "low-prio",
                                },
                            },
                            "spec": {
                                "suspend": False,
                                "replicatedJobs": [
                                    {
                                        "name": "worker",
                                        "replicas": 2,
                                        "template": {
                                            "spec": {
                                                "parallelism": 1,
                                                "completions": 1,
                                                "template": {
                                                    "spec": {
                                                        "containers": [
                                                            {
                                                                "name": "c",
                                                                "image": "registry.k8s.io/pause:3.10",
                                                                "resources": {"requests": {"cpu": "4", "memory": "16Gi", "google.com/tpu": "4"}},
                                                            }
                                                        ]
                                                    }
                                                },
                                            }
                                        },
                                    }
                                ],
                            },
                        },
                    },
                },
                {
                    "timestamp": t_arr,
                    "eventType": "New",
                    "workloadState": "NotAdmitted",
                    "payload": {
                        "objectKind": "JobSet",
                        "name": "arrival-js",
                        "namespace": "ai-lab",
                        "creationTime": t_arr,
                        "localQueueName": "tpu-lq",
                        "object": {
                            "apiVersion": "jobset.x-k8s.io/v1alpha2",
                            "kind": "JobSet",
                            "metadata": {
                                "name": "arrival-js",
                                "namespace": "ai-lab",
                                "labels": {"kueue.x-k8s.io/queue-name": "tpu-lq"},
                            },
                            "spec": {
                                "suspend": True,
                                "replicatedJobs": [
                                    {
                                        "name": "worker",
                                        "replicas": 1,
                                        "template": {
                                            "spec": {
                                                "parallelism": 1,
                                                "completions": 1,
                                                "template": {
                                                    "spec": {
                                                        "containers": [
                                                            {
                                                                "name": "c",
                                                                "image": "registry.k8s.io/pause:3.10",
                                                                "resources": {"requests": {"cpu": "4", "memory": "16Gi", "google.com/tpu": "2"}},
                                                            }
                                                        ]
                                                    }
                                                },
                                            }
                                        },
                                    }
                                ],
                            },
                        },
                    },
                },
            ]
            exec_events = [
                {
                    "timestamp": t0,
                    "eventType": "Admitted",
                    "workloadState": "Admitted",
                    "payload": {
                        "workloadName": "jobset-running-js-111",
                        "namespace": "ai-lab",
                        "ownerKind": "JobSet",
                        "ownerName": "running-js",
                        "localQueueName": "tpu-lq",
                        "clusterQueueName": "tpu-cq",
                        "priorityClassName": "low-prio",
                        "priority": 100,
                        "creationTime": t0,
                        "admittedTime": t0,
                        "preempted": False,
                    },
                },
                {
                    "timestamp": t_del,
                    "eventType": "FullExecution",
                    "workloadState": "Admitted",
                    "payload": {
                        "executionType": "Full",
                        "workloadName": "jobset-running-js-111",
                        "namespace": "ai-lab",
                        "ownerKind": "JobSet",
                        "ownerName": "running-js",
                        "localQueueName": "tpu-lq",
                        "clusterQueueName": "tpu-cq",
                        "creationTime": t0,
                        "admittedTime": t0,
                        "endTime": t_del,
                        "executionDurationSeconds": 45.0,
                        "preempted": False,
                    },
                },
            ]
            (scen / "infra.json").write_text("\n".join(json.dumps(e) for e in infra_events) + "\n", encoding="utf-8")
            (scen / "workflows.json").write_text("\n".join(json.dumps(e) for e in wf_events) + "\n", encoding="utf-8")
            (scen / "execution-time.json").write_text("\n".join(json.dumps(e) for e in exec_events) + "\n", encoding="utf-8")

            bundle = testrunner.load_scenario_bundle(scenario_dir=scen, speedup=1.0)
            self.assertEqual(len(bundle.initial_workloads), 1)
            self.assertEqual(bundle.initial_workload_pods_count, 2)
            self.assertEqual(len(bundle.initial_pods), 1)
            self.assertEqual(bundle.initial_pods[0].namespace, "infra-pods")
            self.assertNotIn(
                "kueue.x-k8s.io/podset-unconstrained-topology",
                bundle.initial_pods[0].manifest["metadata"]["annotations"],
            )

            iwl = bundle.initial_workloads[0]
            self.assertEqual(iwl.namespace, "ai-lab")
            self.assertEqual(iwl.cluster_queue_name, "tpu-cq")
            self.assertEqual(iwl.local_queue_name, "tpu-lq")
            self.assertTrue(iwl.is_tas)
            self.assertEqual(iwl.priority_class_name, "low-prio")
            self.assertEqual(iwl.priority, 100)
            self.assertIsNotNone(iwl.resubmit_item)
            self.assertTrue(iwl.resubmit_item.manifest["spec"]["suspend"])

            # Check PodGroup & TAS invariants on Workload and member Pods
            self.assertEqual(
                iwl.workload_manifest["metadata"]["annotations"]["kueue.x-k8s.io/is-group-workload"],
                "true",
            )
            self.assertEqual(len(iwl.workload_manifest["spec"]["podSets"]), 2)
            psas = iwl.workload_status["admission"]["podSetAssignments"]
            self.assertEqual(len(psas), 2)
            for psa in psas:
                self.assertIn("topologyAssignment", psa)
            for p in iwl.pods:
                self.assertEqual(p.namespace, "ai-lab")
                p_lbl = p.manifest["metadata"]["labels"]
                p_ann = p.manifest["metadata"]["annotations"]
                self.assertEqual(p_lbl["kueue.x-k8s.io/managed"], "true")
                self.assertEqual(p_lbl["kueue.x-k8s.io/pod-group-name"], iwl.workload_name)
                self.assertEqual(p_lbl["kueue.x-k8s.io/queue-name"], "tpu-lq")
                self.assertEqual(p_lbl["kueue-optimizer.gke.io/initial-pod"], "true")
                self.assertEqual(p_ann["kueue.x-k8s.io/pod-group-total-count"], "2")
                self.assertEqual(p_ann["kueue.x-k8s.io/retriable-in-group"], "false")
                self.assertEqual(p_ann["kueue.x-k8s.io/workload"], iwl.workload_name)
                self.assertEqual(p_ann["kueue.x-k8s.io/podset-unconstrained-topology"], "true")

            # WorkloadPriorityClass `low-prio` referenced by initial_workloads must be synthesized
            wpc_docs = [d for d in bundle.prereq_and_queue_docs if d.get("kind") == "WorkloadPriorityClass"]
            self.assertEqual([d["metadata"]["name"] for d in wpc_docs], ["low-prio"])
            self.assertEqual(wpc_docs[0]["value"], 100)

    def test_webhook_mpod_object_selector_and_replay_bootstrap_order_and_eviction_resubmit(self) -> None:
        # 1. Verify committed kueue-bootstrap.yaml sets objectSelector on mpod.kb.io only
        bootstrap_yaml = (REPO_ROOT / "deploy/base/manifests/kueue-bootstrap.yaml").read_text(encoding="utf-8")
        self.assertIn("key: kueue-optimizer.gke.io/initial-pod", bootstrap_yaml)
        self.assertEqual(bootstrap_yaml.count("key: kueue-optimizer.gke.io/initial-pod"), 1)

        # 2. Verify ensure_kueue_webhook_namespace_exclusions dynamically patches mpod.kb.io objectSelector
        mgr = ChamberManager(chamber_namespace="kueue-optimizer-chamber-wh", apiserver_port=16495)
        try:
            mut_wh = {
                "metadata": {"name": "kueue-mutating-webhook-configuration"},
                "webhooks": [
                    {
                        "name": "mpod.kb.io",
                        "namespaceSelector": {"matchExpressions": [{"key": "kubernetes.io/metadata.name", "operator": "NotIn", "values": ["kube-system"]}]},
                    },
                    {
                        "name": "mworkload.kb.io",
                        "namespaceSelector": {"matchExpressions": [{"key": "kubernetes.io/metadata.name", "operator": "NotIn", "values": ["kube-system"]}]},
                    },
                ],
            }
            val_wh = {
                "metadata": {"name": "kueue-validating-webhook-configuration"},
                "webhooks": [
                    {
                        "name": "vpod.kb.io",
                        "namespaceSelector": {"matchExpressions": [{"key": "kubernetes.io/metadata.name", "operator": "NotIn", "values": ["kube-system"]}]},
                    }
                ],
            }

            def fake_api_req(method: str, path: str, **kwargs: object) -> tuple[int, bytes]:
                if "mutatingwebhookconfigurations" in path:
                    return 200, json.dumps(mut_wh).encode("utf-8")
                return 200, json.dumps(val_wh).encode("utf-8")

            applied_wh_docs: list[dict] = []
            with (
                mock.patch.object(mgr, "api_request", side_effect=fake_api_req),
                mock.patch.object(mgr, "ssa_apply_doc", side_effect=applied_wh_docs.append),
            ):
                mgr.ensure_kueue_webhook_namespace_exclusions(excluded_namespaces=("infra-pods",))

            self.assertEqual(len(applied_wh_docs), 2)
            mut_patched = next(d for d in applied_wh_docs if d["kind"] == "MutatingWebhookConfiguration")
            mpod = next(w for w in mut_patched["webhooks"] if w["name"] == "mpod.kb.io")
            mwl = next(w for w in mut_patched["webhooks"] if w["name"] == "mworkload.kb.io")
            self.assertIn("objectSelector", mpod)
            self.assertNotIn("objectSelector", mwl)
            val_patched = next(d for d in applied_wh_docs if d["kind"] == "ValidatingWebhookConfiguration")
            vpod = val_patched["webhooks"][0]
            self.assertNotIn("objectSelector", vpod)

            # 3. Verify apply_scenario_queues_and_wait bootstrap sequence AND run_scenario_replay dynamic resubmit + completion check
            call_order: list[str] = []
            iwl_doc = {"apiVersion": "kueue.x-k8s.io/v1beta2", "kind": "Workload", "metadata": {"name": "pg-init-0", "namespace": "ai-lab"}}
            iwl_st = {"admission": {"clusterQueue": "cq-a", "podSetAssignments": []}, "conditions": []}
            ipod = testrunner.InitialPodSpec(
                name="pg-init-0-p0",
                namespace="ai-lab",
                placement="node-0",
                resource_request={"cpu": "2"},
                remaining_duration_sec=5.0,
                is_infinite=False,
                manifest={"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "pg-init-0-p0", "namespace": "ai-lab"}},
            )
            resub_sw = testrunner.ScheduledWorkflowItem(
                target_wall_offset_sec=0.0,
                seq=100,
                raw_offset_sec=0.0,
                raw_offset_from_t0_sec=0.0,
                event_timestamp="2026-11-20T10:00:00Z",
                object_kind="JobSet",
                name="init-js",
                namespace="ai-lab",
                local_queue_name="lq-a",
                workload_state="NotAdmitted",
                trigger_reason="preempted_after_initial_admission",
                execution_duration_sec=10.0,
                compressed_duration_sec=10.0,
                indexed_from_execution_time=True,
                pod_count=2,
                manifest={"apiVersion": "jobset.x-k8s.io/v1alpha2", "kind": "JobSet", "metadata": {"name": "init-js", "namespace": "ai-lab"}},
            )
            iwl_obj = testrunner.InitialWorkloadPodGroup(
                workload_name="pg-init-0",
                namespace="ai-lab",
                local_queue_name="lq-a",
                cluster_queue_name="cq-a",
                owner_kind="JobSet",
                owner_name="init-js",
                priority_class_name="",
                priority=0,
                is_tas=False,
                pods=[ipod],
                workload_manifest=iwl_doc,
                workload_status=iwl_st,
                resubmit_item=resub_sw,
            )
            prereq_docs = [
                {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "ai-lab"}},
                {"apiVersion": "kueue.x-k8s.io/v1beta2", "kind": "ResourceFlavor", "metadata": {"name": "rf-a"}},
                {"apiVersion": "kueue.x-k8s.io/v1beta2", "kind": "ClusterQueue", "metadata": {"name": "cq-a"}},
                {"apiVersion": "kueue.x-k8s.io/v1beta2", "kind": "LocalQueue", "metadata": {"name": "lq-a", "namespace": "ai-lab"}},
            ]

            def record_apply_parallel(docs: list[dict], **kw: object) -> None:
                if docs:
                    call_order.append("apply:" + ",".join(sorted({d["kind"] for d in docs})))

            def fake_q_api(method: str, path: str, **kw: object) -> tuple[int, bytes]:
                if path.endswith("/clusterqueues"):
                    return 200, json.dumps({"items": [{"metadata": {"name": "cq-a"}, "status": {"conditions": [{"type": "Active", "status": "True"}]}}]}).encode()
                return 200, json.dumps({"items": [{"metadata": {"name": "lq-a", "namespace": "ai-lab"}, "status": {"conditions": [{"type": "Active", "status": "True"}]}}]}).encode()

            with (
                mock.patch.object(mgr, "apply_manifests_parallel", side_effect=record_apply_parallel),
                mock.patch.object(mgr, "apply_pre_admitted_workloads_parallel", side_effect=lambda _: call_order.append("pre_admit_workloads")),
                mock.patch.object(mgr, "wait_for_pods_running", side_effect=lambda _: call_order.append("wait_pods_running")),
                mock.patch.object(mgr, "api_request", side_effect=fake_q_api),
                mock.patch("sys.stdout", new_callable=io.StringIO),
            ):
                testrunner.apply_scenario_queues_and_wait(
                    mgr,
                    prereq_docs,
                    initial_workloads=[iwl_obj],
                    initial_pods=[],
                )

            # ClusterQueue & LocalQueue MUST be applied strictly AFTER pre-admitted Workloads & Pods are running!
            self.assertEqual(
                call_order,
                [
                    "apply:Namespace",
                    "apply:ResourceFlavor",
                    "pre_admit_workloads",
                    "apply:Pod",
                    "wait_pods_running",
                    "apply:ClusterQueue,LocalQueue",
                ],
            )

            # Now test run_scenario_replay:
            # Tick 1: initial workload finishes (must NOT finish replay early while arrival-js hasn't finished!),
            #         WAIT - test eviction instead:
            # Tick 1: arrival-js submitted; pg-init-0 is Evicted -> triggers dynamic resubmit of init-js (submitted becomes 2!)
            # Tick 2: only arrival-js finished (done=1 < 2 -> must NOT exit early!)
            # Tick 3: both arrival-js and resubmitted init-js finished (done=2 >= 2 -> exits cleanly!)
            arrival_sw = testrunner.ScheduledWorkflowItem(
                target_wall_offset_sec=0.0,
                seq=0,
                raw_offset_sec=0.0,
                raw_offset_from_t0_sec=0.0,
                event_timestamp="2026-11-20T10:00:00Z",
                object_kind="JobSet",
                name="arrival-js",
                namespace="ai-lab",
                local_queue_name="lq-a",
                workload_state="NotAdmitted",
                trigger_reason="initial_not_admitted",
                execution_duration_sec=5.0,
                compressed_duration_sec=5.0,
                indexed_from_execution_time=True,
                pod_count=1,
                manifest={"apiVersion": "jobset.x-k8s.io/v1alpha2", "kind": "JobSet", "metadata": {"name": "arrival-js", "namespace": "ai-lab"}},
            )
            demo_bundle = testrunner.load_scenario_bundle(scenario_dir=REPO_ROOT / "testscenarios/demo")
            demo_bundle.initial_workloads = [iwl_obj]
            demo_bundle.initial_pods = []
            demo_bundle.scheduled_workflows = [arrival_sw]

            snaps = [
                # Status #1: pg-init-0 pods deleted by preemption & GC removed pg-init-0 before poll -> still triggers dynamic resubmit!
                ClusterStatusSnapshot(
                    workloads_admitted=1,
                    workloads_pending=0,
                    workloads_finished=0,
                    workloads_query_ok=True,
                    present_workload_keys={("ai-lab", "jobset-arrival-js-1")},
                    evicted_workload_keys=set(),
                    finished_workload_keys=set(),
                    jobsets_completed=0,
                ),
                # Status #2: arrival-js finished, resubmitted init-js still running (done=1 < 2 -> must NOT exit early!)
                ClusterStatusSnapshot(
                    workloads_admitted=1,
                    workloads_pending=0,
                    workloads_finished=1,
                    workloads_query_ok=True,
                    present_workload_keys={("ai-lab", "jobset-arrival-js-1"), ("ai-lab", "jobset-init-js-2")},
                    evicted_workload_keys=set(),
                    finished_workload_keys={("ai-lab", "jobset-arrival-js-1")},
                    jobsets_completed=1,
                ),
                # Status #3: resubmitted init-js also finished!
                ClusterStatusSnapshot(
                    workloads_admitted=0,
                    workloads_pending=0,
                    workloads_finished=2,
                    workloads_query_ok=True,
                    present_workload_keys={("ai-lab", "jobset-arrival-js-1"), ("ai-lab", "jobset-init-js-2")},
                    evicted_workload_keys=set(),
                    finished_workload_keys={
                        ("ai-lab", "jobset-arrival-js-1"),
                        ("ai-lab", "jobset-init-js-2"),
                    },
                    jobsets_completed=2,
                ),
            ]
            resubmitted_docs: list[dict] = []
            with (
                mock.patch.object(mgr, "apply_manifests_parallel"),
                mock.patch.object(mgr, "ssa_apply_doc", side_effect=resubmitted_docs.append),
                mock.patch("runner.replay.query_cluster_status", side_effect=snaps) as m_qcs,
                mock.patch("sys.stdout", new_callable=io.StringIO),
            ):
                _s, _e, sub_cnt, tot_pods = testrunner.run_scenario_replay(
                    mgr=mgr,
                    bundle=demo_bundle,
                    speedup=1.0,
                    max_run_seconds=10.0,
                    status_interval_sec=0.02,
                    wait_for_completion=True,
                    initial_state_already_bootstrapped=True,
                )

            self.assertEqual(len(resubmitted_docs), 1)
            self.assertEqual(resubmitted_docs[0]["metadata"]["name"], "init-js")
            self.assertEqual(sub_cnt, 2)
            self.assertEqual(tot_pods, 1 + 1 + 2)
            self.assertEqual(m_qcs.call_count, 3)
        finally:
            mgr.close()

    def test_finite_vs_infinite_initial_t0_workload_completion_gating_and_self_test_sync(self) -> None:
        """Finite t0 PodGroup workloads hold replay open until finished, while infinite t0 PodGroups do not block."""
        import subprocess
        import sys
        from chamber.lifecycle import ChamberManager, ClusterStatusSnapshot

        repo_root = REPO_ROOT
        chk = subprocess.run(
            [sys.executable, str(repo_root / "testscenarios/self-tests/generate_self_test_suite.py"), "--check"],
            cwd=str(repo_root),
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(chk.returncode, 0, msg=f"Self-test suite out of sync:\n{chk.stdout}\n{chk.stderr}")

        # Verify st37 loader output uses t0pg- prefix and injects preempted-t0-resubmit annotation on resubmit_item
        st37_bundle = testrunner.load_scenario_bundle(
            scenario_dir=repo_root / "testscenarios/self-tests/st37-t0-preempt-within-cq-lower-priority-resubmit",
            skip_initial_offset=False,
        )
        self.assertEqual(len(st37_bundle.initial_workloads), 8)
        for iwl in st37_bundle.initial_workloads:
            self.assertTrue(iwl.workload_name.startswith("t0pg-"), iwl.workload_name)
            self.assertIsNotNone(iwl.resubmit_item)
            self.assertEqual(
                iwl.resubmit_item.manifest["metadata"]["annotations"].get("kueue-optimizer.gke.io/preempted-t0-resubmit"),
                "true",
            )

        mgr = ChamberManager(chamber_namespace="kueue-optimizer-chamber-t0-gate", apiserver_port=16498)
        try:
            finite_pod = testrunner.InitialPodSpec(
                name="fin-p0",
                namespace="ai-lab",
                placement="node-0",
                resource_request={"cpu": "4"},
                remaining_duration_sec=10.0,
                is_infinite=False,
                manifest={},
            )
            inf_pod = testrunner.InitialPodSpec(
                name="inf-p0",
                namespace="ai-lab",
                placement="node-1",
                resource_request={"cpu": "4"},
                remaining_duration_sec=864000.0,
                is_infinite=True,
                manifest={},
            )
            iwl_fin = testrunner.InitialWorkloadPodGroup(
                workload_name="t0pg-fin-0",
                namespace="ai-lab",
                local_queue_name="lq-a",
                cluster_queue_name="cq-a",
                owner_kind="JobSet",
                owner_name="fin-0",
                priority_class_name="",
                priority=0,
                is_tas=False,
                pods=[finite_pod],
                workload_manifest={},
                workload_status={},
            )
            iwl_inf = testrunner.InitialWorkloadPodGroup(
                workload_name="t0pg-inf-0",
                namespace="ai-lab",
                local_queue_name="lq-a",
                cluster_queue_name="cq-a",
                owner_kind="Deployment",
                owner_name="inf-0",
                priority_class_name="",
                priority=0,
                is_tas=False,
                pods=[inf_pod],
                workload_manifest={},
                workload_status={},
            )
            bundle = testrunner.load_scenario_bundle(scenario_dir=repo_root / "testscenarios/demo")
            bundle.initial_workloads = [iwl_fin, iwl_inf]
            bundle.initial_pods = []
            bundle.scheduled_workflows = []
            snaps = [
                # Poll #1: finite t0pg-fin-0 still running -> must NOT exit yet!
                ClusterStatusSnapshot(
                    workloads_admitted=2,
                    workloads_finished=0,
                    workloads_query_ok=True,
                    present_workload_keys={("ai-lab", "t0pg-fin-0"), ("ai-lab", "t0pg-inf-0")},
                    finished_workload_keys=set(),
                    pods_running=2,
                ),
                # Poll #2: t0pg-fin-0 finished while infinite t0pg-inf-0 stays running -> MUST exit cleanly now!
                ClusterStatusSnapshot(
                    workloads_admitted=1,
                    workloads_finished=1,
                    workloads_query_ok=True,
                    present_workload_keys={("ai-lab", "t0pg-fin-0"), ("ai-lab", "t0pg-inf-0")},
                    finished_workload_keys={("ai-lab", "t0pg-fin-0")},
                    pods_running=1,
                ),
            ]
            with (
                mock.patch("runner.replay.query_cluster_status", side_effect=snaps) as m_qcs,
                mock.patch("sys.stdout", new_callable=io.StringIO),
            ):
                testrunner.run_scenario_replay(
                    mgr=mgr,
                    bundle=bundle,
                    speedup=1.0,
                    max_run_seconds=10.0,
                    status_interval_sec=0.02,
                    wait_for_completion=True,
                    initial_state_already_bootstrapped=True,
                )
            self.assertEqual(m_qcs.call_count, 2)
        finally:
            mgr.close()

    def test_podgroup_roundtrip_coalescing_partial_tas_synthesis_and_latched_preempted_finish(self) -> None:
        """Verify PodGroup round-trip coalescing (no phantom Tier 4 pods), partial vanilla-K8s t0 TAS synthesis,
        query_cluster_status latched Preempted=True handling, and 60-scenario scale invariants."""
        from chamber.lifecycle import ChamberManager, query_cluster_status
        from scenario_readme import analyze_scenario

        repo_root = REPO_ROOT

        # 1. Verify st50 (vanilla PodGroups + multi-API + infra-pods) coalesces 16 2-pod PodGroups without doubling
        st50_dir = repo_root / "testscenarios/self-tests/st50-scale-quota-mixed-apis-infra-128n-1064w"
        st50_bundle = testrunner.load_scenario_bundle(scenario_dir=st50_dir, skip_initial_offset=False)
        self.assertEqual(len(st50_bundle.nodes), 128)
        self.assertEqual(len(st50_bundle.initial_workloads), 48)
        self.assertEqual(len(st50_bundle.initial_deployments), 16)
        self.assertEqual(len(st50_bundle.initial_workloads) + len(st50_bundle.initial_deployments), 64)
        self.assertEqual(st50_bundle.initial_workload_pods_count, 80)
        self.assertEqual(len(st50_bundle.initial_pods), 16)
        self.assertEqual(len(st50_bundle.scheduled_workflows), 1000)
        st50_analysis = analyze_scenario(st50_dir)
        self.assertEqual(len(st50_analysis.admitted_at_start_wfs), 64)
        self.assertEqual(len(st50_analysis.unadmitted_wfs), 1000)

        # 2. Verify st54 partial vanilla-K8s t0 TAS binding synthesizes exact 24 missing pods onto TAS node pairs
        st54_dir = repo_root / "testscenarios/self-tests/st54-scale-tas-preferred-and-partial-t0-144n-1104w"
        st54_bundle = testrunner.load_scenario_bundle(scenario_dir=st54_dir, skip_initial_offset=False)
        self.assertEqual(len(st54_bundle.nodes), 144)
        self.assertEqual(len(st54_bundle.initial_workloads), 48)
        self.assertEqual(st54_bundle.initial_workload_pods_count, 96)
        for idx, iwl in enumerate(st54_bundle.initial_workloads):
            self.assertEqual(len(iwl.pods), 2)
            self.assertTrue(iwl.is_tas)
            expected_pair = {f"st-node-{2 * idx:03d}", f"st-node-{2 * idx + 1:03d}"}
            self.assertEqual({p.placement for p in iwl.pods}, expected_pair)

        # 3. Verify scale scenarios st46..st60 counts (5 quota-only without TAS, 10 with TAS, all >100 nodes & >1000 wls)
        suite_dir = repo_root / "testscenarios/self-tests"
        scale_dirs = sorted(
            p
            for p in suite_dir.iterdir()
            if p.is_dir() and p.name.startswith("st") and 46 <= int(p.name[2:4]) <= 60
        )
        self.assertEqual(len(scale_dirs), 15)
        no_tas = 0
        with_tas = 0
        for sd in scale_dirs:
            sb = testrunner.load_scenario_bundle(scenario_dir=sd, skip_initial_offset=False)
            self.assertGreater(len(sb.nodes), 100, sd.name)
            self.assertGreater(len(sb.initial_workloads) + len(sb.scheduled_workflows), 1000, sd.name)
            self.assertGreater(len(sb.initial_workloads), 0, sd.name)
            if any(doc.get("kind") == "Topology" for doc in sb.prereq_and_queue_docs):
                with_tas += 1
            else:
                no_tas += 1
        self.assertEqual((no_tas, with_tas), (5, 10))

        # 4. Verify query_cluster_status handles latched Preempted=True on finished regular Jobs vs PodGroups
        fake_wl_list = {
            "items": [
                # Regular Job workload that was preempted on attempt 1, re-admitted, and finished cleanly:
                {
                    "metadata": {"name": "job-arr-0-abcde", "namespace": "st-workloads"},
                    "status": {
                        "conditions": [
                            {"type": "Preempted", "status": "True", "reason": "InClusterQueue"},
                            {"type": "Evicted", "status": "False", "reason": "InactiveWorkload"},
                            {"type": "Finished", "status": "True", "reason": "Succeeded"},
                        ]
                    },
                },
                # t0 PodGroup workload whose pod succeeded in a race against preemption:
                {
                    "metadata": {
                        "name": "t0pg-race-winner",
                        "namespace": "st-workloads",
                        "annotations": {"kueue.x-k8s.io/is-group-workload": "true"},
                    },
                    "status": {
                        "conditions": [
                            {"type": "Preempted", "status": "True", "reason": "InClusterQueue"},
                            {"type": "Evicted", "status": "True", "reason": "Preempted"},
                            {"type": "Finished", "status": "True", "reason": "Succeeded"},
                        ]
                    },
                },
                # t0 PodGroup workload genuinely killed by preemption (Finished=True with PodsFailed):
                {
                    "metadata": {
                        "name": "t0pg-evicted-victim",
                        "namespace": "st-workloads",
                        "annotations": {"kueue.x-k8s.io/is-group-workload": "true"},
                    },
                    "status": {
                        "conditions": [
                            {"type": "Preempted", "status": "True", "reason": "InClusterQueue"},
                            {"type": "Evicted", "status": "True", "reason": "Preempted"},
                            {"type": "Finished", "status": "True", "reason": "PodsFailed"},
                        ]
                    },
                },
            ]
        }

        def fake_api_request(method: str, path: str, **_kwargs: object) -> tuple[int, bytes]:
            if path == "/apis/kueue.x-k8s.io/v1beta2/workloads":
                return 200, json.dumps(fake_wl_list).encode("utf-8")
            return 200, b'{"items":[]}'

        mgr = ChamberManager(chamber_namespace="kueue-optimizer-chamber-qcs-test", apiserver_port=16499)
        try:
            with mock.patch.object(mgr, "api_request", side_effect=fake_api_request):
                snap = query_cluster_status(mgr, workload_namespaces=["st-workloads"], include_kueue_workloads=True)
            self.assertIn(("st-workloads", "job-arr-0-abcde"), snap.finished_workload_keys)
            self.assertIn(("st-workloads", "t0pg-race-winner"), snap.finished_workload_keys)
            self.assertNotIn(("st-workloads", "t0pg-race-winner"), snap.evicted_workload_keys)
            self.assertIn(("st-workloads", "t0pg-evicted-victim"), snap.evicted_workload_keys)
            self.assertNotIn(("st-workloads", "t0pg-evicted-victim"), snap.finished_workload_keys)
        finally:
            mgr.close()

    def test_wait_for_pods_ready_scaling_clamping_and_config_patching(self) -> None:
        """Verify waitForPodsReady scales with --speedup, clamps to --duration, and renders valid Kueue config."""
        # 1. Speedup scaling (40s / 5 = 8s, backoff 10s / 5 = 2s)
        cfg_5x = testrunner.resolve_wait_for_pods_ready_config(
            raw_timeout="40s",
            raw_recovery_timeout="20s",
            block_admission=True,
            backoff_limit_count=1,
            raw_backoff_base="10s",
            raw_backoff_max="60s",
            speedup=5.0,
        )
        self.assertTrue(cfg_5x.enabled)
        self.assertAlmostEqual(cfg_5x.effective_timeout_sec, 8.0)
        self.assertAlmostEqual(cfg_5x.effective_recovery_timeout_sec or 0.0, 4.0)
        self.assertEqual(cfg_5x.effective_backoff_base_sec, 2)
        self.assertEqual(cfg_5x.effective_backoff_max_sec, 12)
        self.assertFalse(cfg_5x.clamped_timeout_to_duration)

        # 2. Duration clamping (10m = 600s clamped to 50% of 24s = 12s)
        cfg_clamped = testrunner.resolve_wait_for_pods_ready_config(
            raw_timeout="10m",
            speedup=1.0,
            max_run_seconds=24.0,
            cap_to_duration=True,
        )
        self.assertAlmostEqual(cfg_clamped.effective_timeout_sec, 12.0)
        self.assertTrue(cfg_clamped.clamped_timeout_to_duration)

        # 3. Minimum floor at extreme speedup (30s / 100 = 0.3s -> floored to 2.0s; 0s recoveryTimeout preserved)
        cfg_floored = testrunner.resolve_wait_for_pods_ready_config(
            raw_timeout="30s",
            raw_recovery_timeout="0s",
            speedup=100.0,
            min_wall_timeout="2s",
        )
        self.assertAlmostEqual(cfg_floored.effective_timeout_sec, 2.0)
        self.assertTrue(cfg_floored.floored_timeout_to_min)
        self.assertEqual(cfg_floored.effective_recovery_timeout_sec, 0.0)

        # 4. Patching controller_manager_config.yaml for waitForPodsReady & featureGates
        with tempfile.TemporaryDirectory() as td:
            base_dst = Path(td)
            (base_dst / "config").mkdir()
            (base_dst / "config" / "controller_manager_config.yaml").write_text(
                "apiVersion: config.kueue.x-k8s.io/v1beta2\nkind: Configuration\ninternalCertManagement:\n  enable: false\n",
                encoding="utf-8",
            )
            from chamber.manifests import (
                parse_feature_gates_arg,
                patch_kueue_feature_gates_config,
                patch_kueue_wait_for_pods_ready_config,
            )

            patch_kueue_wait_for_pods_ready_config(base_dst, cfg_5x)
            patch_kueue_feature_gates_config(base_dst, parse_feature_gates_arg("TASReplaceMultipleFailedNodes=true"))
            rendered = (base_dst / "config" / "controller_manager_config.yaml").read_text(encoding="utf-8")
            self.assertIn("waitForPodsReady:\n  timeout: 8s\n  recoveryTimeout: 4s\n  blockAdmission: true\n", rendered)
            self.assertIn("backoffBaseSeconds: 2\n    backoffMaxSeconds: 12", rendered)
            self.assertIn("featureGates:\n  TASReplaceMultipleFailedNodes: true\n", rendered)

    def test_interrupted_workload_multiplier_and_true_length_override_precedence(self) -> None:
        """Verify PartialExecution-only workloads always replay strictly longer than interrupted recordings."""
        with tempfile.TemporaryDirectory() as td:
            sc_dir = Path(td)
            t0 = "2026-06-01T00:00:00Z"
            t8 = "2026-06-01T00:00:08Z"
            t14 = "2026-06-01T00:00:14Z"
            (sc_dir / "infra.json").write_text(
                "\n".join(
                    json.dumps(x, separators=(",", ":"))
                    for x in [
                        {
                            "timestamp": t0,
                            "eventType": "New",
                            "payload": {
                                "apiVersion": "v1",
                                "kind": "Node",
                                "metadata": {"name": "n-0", "labels": {"type": "kwok"}},
                                "spec": {},
                                "status": {"capacity": {"cpu": "16", "memory": "64Gi"}, "allocatable": {"cpu": "16", "memory": "64Gi"}},
                            },
                        },
                        {
                            "timestamp": t0,
                            "eventType": "New",
                            "payload": {
                                "apiVersion": "kueue.x-k8s.io/v1beta2",
                                "kind": "ResourceFlavor",
                                "metadata": {"name": "rf-main"},
                                "spec": {"nodeLabels": {"type": "kwok"}},
                            },
                        },
                        {
                            "timestamp": t0,
                            "eventType": "New",
                            "payload": {
                                "apiVersion": "kueue.x-k8s.io/v1beta2",
                                "kind": "ClusterQueue",
                                "metadata": {"name": "cq-main"},
                                "spec": {
                                    "namespaceSelector": {},
                                    "resourceGroups": [
                                        {
                                            "coveredResources": ["cpu", "memory"],
                                            "flavors": [
                                                {
                                                    "name": "rf-main",
                                                    "resources": [
                                                        {"name": "cpu", "nominalQuota": "16"},
                                                        {"name": "memory", "nominalQuota": "64Gi"},
                                                    ],
                                                }
                                            ],
                                        }
                                    ],
                                },
                            },
                        },
                        {
                            "timestamp": t0,
                            "eventType": "New",
                            "payload": {
                                "apiVersion": "kueue.x-k8s.io/v1beta2",
                                "kind": "LocalQueue",
                                "metadata": {"name": "lq-main", "namespace": "default"},
                                "spec": {"clusterQueue": "cq-main"},
                            },
                        },
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            def _job_ev(name: str) -> dict[str, object]:
                return {
                    "timestamp": t0,
                    "eventType": "New",
                    "workloadState": "NotAdmitted",
                    "payload": {
                        "objectKind": "Job",
                        "name": name,
                        "namespace": "default",
                        "creationTime": t0,
                        "localQueueName": "lq-main",
                        "object": {
                            "apiVersion": "batch/v1",
                            "kind": "Job",
                            "metadata": {"name": name, "namespace": "default", "labels": {"kueue.x-k8s.io/queue-name": "lq-main"}},
                            "spec": {
                                "suspend": True,
                                "parallelism": 1,
                                "completions": 1,
                                "template": {
                                    "metadata": {},
                                    "spec": {
                                        "restartPolicy": "Never",
                                        "containers": [
                                            {
                                                "name": "w",
                                                "image": "registry.k8s.io/pause:3.10",
                                                "resources": {"requests": {"cpu": "2", "memory": "4Gi"}},
                                            }
                                        ],
                                    },
                                },
                            },
                        },
                    },
                }

            (sc_dir / "workflows.json").write_text(
                "\n".join(json.dumps(_job_ev(n), separators=(",", ":")) for n in ("job-part-plain", "job-part-true", "job-full")) + "\n",
                encoding="utf-8",
            )
            (sc_dir / "execution-time.json").write_text(
                "\n".join(
                    json.dumps(x, separators=(",", ":"))
                    for x in [
                        {
                            "timestamp": t8,
                            "eventType": "PartialExecution",
                            "workloadState": "NotAdmitted",
                            "payload": {
                                "executionType": "Partial",
                                "workloadName": "wl-1",
                                "namespace": "default",
                                "ownerKind": "Job",
                                "ownerName": "job-part-plain",
                                "creationTime": t0,
                                "admittedTime": t0,
                                "endTime": t8,
                                "executionDurationSeconds": 8.0,
                                "preempted": True,
                                "interruptionReason": "Preempted",
                            },
                        },
                        {
                            "timestamp": t8,
                            "eventType": "PartialExecution",
                            "workloadState": "NotAdmitted",
                            "payload": {
                                "executionType": "Partial",
                                "workloadName": "wl-2",
                                "namespace": "default",
                                "ownerKind": "Job",
                                "ownerName": "job-part-true",
                                "creationTime": t0,
                                "admittedTime": t0,
                                "endTime": t8,
                                "executionDurationSeconds": 8.0,
                                "preempted": False,
                                "interruptionReason": "NodeFailure",
                                "trueDurationSeconds": 21.0,
                            },
                        },
                        {
                            "timestamp": t14,
                            "eventType": "FullExecution",
                            "workloadState": "Admitted",
                            "payload": {
                                "executionType": "Full",
                                "workloadName": "wl-3",
                                "namespace": "default",
                                "ownerKind": "Job",
                                "ownerName": "job-full",
                                "creationTime": t0,
                                "admittedTime": t0,
                                "endTime": t14,
                                "executionDurationSeconds": 14.0,
                                "preempted": False,
                            },
                        },
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            # Default fallback (1.5x -> 12.0s on job-part-plain, 21.0s schema trueDuration on job-part-true, 14.0s FullExecution)
            b_def = testrunner.load_scenario_bundle(sc_dir)
            durs_def = {w.name: w.execution_duration_sec for w in b_def.scheduled_workflows}
            self.assertAlmostEqual(durs_def["job-part-plain"], 12.0)
            self.assertAlmostEqual(durs_def["job-part-true"], 21.0)
            self.assertAlmostEqual(durs_def["job-full"], 14.0)

            # Explicit CLI multiplier 2.5x -> 20.0s on job-part-plain, 21.0s schema trueDuration preserved, 14.0s FullExecution untouched
            b_mult = testrunner.load_scenario_bundle(sc_dir, interrupted_duration_multiplier=2.5)
            durs_mult = {w.name: w.execution_duration_sec for w in b_mult.scheduled_workflows}
            self.assertAlmostEqual(durs_mult["job-part-plain"], 20.0)
            self.assertAlmostEqual(durs_mult["job-part-true"], 21.0)
            self.assertAlmostEqual(durs_mult["job-full"], 14.0)

            # Explicit CLI override via JSON file -> overrides interrupted jobs, leaves FullExecution untouched
            ov_path = sc_dir / "overrides.json"
            ov_path.write_text(json.dumps({"job-part-plain": "30s", "job-part-true": 28}), encoding="utf-8")
            b_ov = testrunner.load_scenario_bundle(sc_dir, interrupted_duration_override=f"@{ov_path}")
            durs_ov = {w.name: w.execution_duration_sec for w in b_ov.scheduled_workflows}
            self.assertAlmostEqual(durs_ov["job-part-plain"], 30.0)
            self.assertAlmostEqual(durs_ov["job-part-true"], 28.0)
            self.assertAlmostEqual(durs_ov["job-full"], 14.0)

            # Reject multiplier <= 1.0 or override <= observed_partial (8.0s)
            with self.assertRaises(ValueError):
                testrunner.load_scenario_bundle(sc_dir, interrupted_duration_multiplier=1.0)
            with self.assertRaises(ValueError):
                testrunner.load_scenario_bundle(sc_dir, interrupted_duration_override="job-part-plain=6s")

    def test_dynamic_node_events_and_deactivated_workload_completion(self) -> None:
        """Verify post-t0 Node failure/recovery/deletion replay and deactivated Workload terminal accounting."""
        sc61 = REPO_ROOT / "testscenarios" / "self-tests" / "st61-tas-single-node-failure-hotswap-same-rack"
        b61 = testrunner.load_scenario_bundle(sc61)
        self.assertEqual(len(b61.nodes), 8)
        self.assertEqual(len(b61.scheduled_infra_events), 1)
        ie = b61.scheduled_infra_events[0]
        self.assertEqual(ie.object_kind, "Node")
        self.assertEqual(ie.name, "st-node-00")
        self.assertTrue(ie.is_not_ready)
        self.assertAlmostEqual(ie.raw_offset_sec, 6.0)

        # Verify _execute_scheduled_infra_event patches node + fails bound active pod + query_cluster_status counts deactivated workloads
        calls: list[tuple[str, str]] = []

        def fake_api(method: str, path: str, **_kw: object) -> tuple[int, bytes]:
            calls.append((method, path))
            if method == "GET" and path.startswith("/api/v1/pods?fieldSelector="):
                return (
                    200,
                    json.dumps(
                        {
                            "items": [
                                {
                                    "metadata": {"name": "pod-0", "namespace": "st-workloads"},
                                    "spec": {"containers": [{"name": "worker", "image": "registry.k8s.io/pause:3.10"}]},
                                    "status": {"phase": "Running"},
                                }
                            ]
                        }
                    ).encode("utf-8"),
                )
            if method == "GET" and path == "/apis/kueue.x-k8s.io/v1beta2/workloads":
                return (
                    200,
                    json.dumps(
                        {
                            "items": [
                                {
                                    "metadata": {"name": "wl-deactivated", "namespace": "st-workloads"},
                                    "spec": {"active": False},
                                    "status": {
                                        "conditions": [
                                            {"type": "Admitted", "status": "False"},
                                            {"type": "Evicted", "status": "True", "reason": "Deactivated"},
                                        ]
                                    },
                                }
                            ]
                        }
                    ).encode("utf-8"),
                )
            return 200, b'{"items":[]}'

        from chamber_common import ChamberManager, query_cluster_status
        from runner.replay import _execute_scheduled_infra_event

        mgr = ChamberManager(chamber_namespace="kueue-optimizer-chamber-nf-test", apiserver_port=16498)
        try:
            with mock.patch.object(mgr, "api_request", side_effect=fake_api):
                _execute_scheduled_infra_event(mgr, b61, ie, 6.0)
                snap = query_cluster_status(mgr, workload_namespaces=["st-workloads"], include_kueue_workloads=True)
            self.assertIn(("PATCH", "/api/v1/nodes/st-node-00"), calls)
            self.assertIn(("PATCH", "/api/v1/nodes/st-node-00/status"), calls)
            self.assertIn(("PATCH", "/api/v1/namespaces/st-workloads/pods/pod-0/status"), calls)
            self.assertEqual(snap.workloads_finished, 1)
            self.assertIn(("st-workloads", "wl-deactivated"), snap.finished_workload_keys)
        finally:
            mgr.close()

    def test_declarative_kueue_configuration_loading_patching_and_scenarios(self) -> None:
        """Verify declarative Kueue Configuration parsing, controller_manager_config.yaml patching, CLI precedence, and st76-st80 scenarios."""
        import contextlib
        import shutil
        from chamber.manifests import (
            patch_kueue_admission_fair_sharing_config,
            patch_kueue_fair_sharing_config,
            parse_kueue_configuration_payload,
        )
        from chamber_common import resolve_admission_fair_sharing_config, resolve_fair_sharing_arg

        # 1. Validate st76-st80 scenario bundles and README rendering
        expected = {
            "st76-config-fair-sharing-enabled-default": (
                True,
                ("LessThanOrEqualToFinalShare", "LessThanInitialShare"),
                False,
            ),
            "st77-config-fair-sharing-disabled-globally": (
                False,
                ("LessThanOrEqualToFinalShare", "LessThanInitialShare"),
                False,
            ),
            "st78-config-fair-sharing-final-share-rule-only": (
                True,
                ("LessThanOrEqualToFinalShare",),
                False,
            ),
            "st79-config-admission-fair-sharing-enabled": (
                True,
                ("LessThanOrEqualToFinalShare", "LessThanInitialShare"),
                True,
            ),
            "st80-config-admission-fair-sharing-disabled": (
                True,
                ("LessThanOrEqualToFinalShare", "LessThanInitialShare"),
                False,
            ),
        }
        for slug, (want_fs, want_strats, want_afs) in expected.items():
            sc_dir = REPO_ROOT / "testscenarios" / "self-tests" / slug
            bundle = testrunner.load_scenario_bundle(sc_dir)
            self.assertIsNotNone(bundle.kueue_configuration, slug)
            assert bundle.kueue_configuration is not None
            self.assertEqual(bundle.kueue_configuration.fair_sharing_enabled, want_fs, slug)
            self.assertEqual(bundle.kueue_configuration.fair_sharing_strategies, want_strats, slug)
            self.assertEqual(bundle.kueue_configuration.afs_raw is not None, want_afs, slug)

            analysis = generate_scenario_readme.analyze_scenario(sc_dir)
            self.assertIsNotNone(analysis.kueue_configuration, slug)
            md = generate_scenario_readme.generate_readme_for_scenario(sc_dir)
            self.assertIn("Kueue Controller `Configuration`", md)
            self.assertIn("### 5.0 Kueue Controller `Configuration` (`infra.json`)", md)

        # 2. Validation rejects invalid preemptionStrategies and negative AFS weights
        with self.assertRaises(ValueError):
            parse_kueue_configuration_payload(
                {
                    "apiVersion": "config.kueue.x-k8s.io/v1beta2",
                    "kind": "Configuration",
                    "fairSharing": {"preemptionStrategies": []},
                }
            )
        with self.assertRaises(ValueError):
            parse_kueue_configuration_payload(
                {
                    "apiVersion": "config.kueue.x-k8s.io/v1beta2",
                    "kind": "Configuration",
                    "fairSharing": {"preemptionStrategies": ["LessThanInitialShare", "LessThanOrEqualToFinalShare"]},
                }
            )
        with self.assertRaises(ValueError):
            parse_kueue_configuration_payload(
                {
                    "apiVersion": "config.kueue.x-k8s.io/v1beta2",
                    "kind": "Configuration",
                    "admissionFairSharing": {"resourceWeights": {"cpu": -1.0}},
                }
            )

        # 3. Legacy v1beta1 fairSharing.enable: false normalizes cleanly to fair_sharing_enabled=False
        legacy = parse_kueue_configuration_payload(
            {
                "apiVersion": "config.kueue.x-k8s.io/v1beta1",
                "kind": "Configuration",
                "fairSharing": {"enable": False, "preemptionStrategies": ["LessThanOrEqualToFinalShare"]},
            }
        )
        self.assertFalse(legacy.fair_sharing_enabled)

        # 4. Speedup scaling & CLI precedence over scenario Configuration
        b79 = testrunner.load_scenario_bundle(
            REPO_ROOT / "testscenarios" / "self-tests" / "st79-config-admission-fair-sharing-enabled"
        )
        assert b79.kueue_configuration is not None
        scaled_afs = resolve_admission_fair_sharing_config(
            enabled=None,
            raw_usage_half_life=None,
            raw_usage_sampling_interval=None,
            resource_weights=None,
            scenario_afs=b79.kueue_configuration.afs_raw,
            speedup=4.0,
        )
        self.assertTrue(scaled_afs.enabled)
        self.assertAlmostEqual(scaled_afs.effective_usage_half_life_sec, 5.0)
        # 1.0s / 4.0 = 0.25s is clamped at DEFAULT_AFS_MIN_SAMPLING_WALL_SEC (0.5s)
        self.assertAlmostEqual(scaled_afs.effective_usage_sampling_interval_sec, 0.5)

        # Explicit --no-admission-fair-sharing overrides scenario AFS
        disabled_afs = resolve_admission_fair_sharing_config(
            enabled=False,
            raw_usage_half_life=None,
            raw_usage_sampling_interval=None,
            resource_weights=None,
            scenario_afs=b79.kueue_configuration.afs_raw,
            speedup=1.0,
        )
        self.assertFalse(disabled_afs.enabled)

        # Explicit --fair-sharing overrides st77 scenario FairSharing=False
        dummy_parser = testrunner.build_arg_parser()
        self.assertFalse(resolve_fair_sharing_arg(None, False, dummy_parser, scenario_default=False))
        self.assertTrue(resolve_fair_sharing_arg(True, False, dummy_parser, scenario_default=False))

        # 5. Verify controller_manager_config.yaml patching for disabled FS, single-rule FS, and AFS
        with tempfile.TemporaryDirectory() as td:
            base_copy = Path(td) / "base"
            shutil.copytree(REPO_ROOT / "deploy" / "base", base_copy)
            cfg_file = base_copy / "config" / "controller_manager_config.yaml"

            patch_kueue_fair_sharing_config(base_copy, False)
            self.assertNotIn("fairSharing:", cfg_file.read_text(encoding="utf-8"))

            patch_kueue_fair_sharing_config(base_copy, True, strategies=("LessThanOrEqualToFinalShare",))
            text_rule = cfg_file.read_text(encoding="utf-8")
            self.assertIn("  - LessThanOrEqualToFinalShare\n", text_rule)
            self.assertNotIn("LessThanInitialShare", text_rule)

            patch_kueue_admission_fair_sharing_config(base_copy, scaled_afs)
            text_afs = cfg_file.read_text(encoding="utf-8")
            self.assertIn("admissionFairSharing:", text_afs)
            self.assertIn("usageHalfLifeTime: 5s", text_afs)
            self.assertIn("google.com/tpu: 1", text_afs)

        # 6. Reject --skip-chamber-deploy when scenario specifies declarative Configuration
        sc77 = str(REPO_ROOT / "testscenarios" / "self-tests" / "st77-config-fair-sharing-disabled-globally")
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
            with self.assertRaises(SystemExit) as cm:
                testrunner.main([sc77, "--skip-chamber-deploy"])
        self.assertEqual(cm.exception.code, 2)
        self.assertIn("cannot be combined with --skip-chamber-deploy", buf.getvalue())


class FidelityReviewRegressionTests(unittest.TestCase):
    """Regression suite covering Findings #1-#13 (Experiments A-L) from the scraper/chamber fidelity review."""

    @staticmethod
    def _base_infra_events(t0: str = "2026-11-22T10:00:00Z") -> list[dict]:
        return [
            {
                "timestamp": t0,
                "eventType": "New",
                "payload": {
                    "apiVersion": "v1",
                    "kind": "Node",
                    "metadata": {
                        "name": "node-0",
                        "labels": {"cloud.google.com/gke-tpu-accelerator": "tpu-v7x"},
                    },
                    "status": {
                        "allocatable": {
                            "cpu": "64",
                            "memory": "128Gi",
                            "pods": "110",
                            "hugepages-1Gi": "0",
                            "hugepages-2Mi": "0",
                            "nvidia.com/gpu": "8",
                            "google.com/tpu": "4",
                        },
                        "capacity": {
                            "cpu": "64",
                            "memory": "128Gi",
                            "pods": "110",
                            "hugepages-1Gi": "0",
                            "hugepages-2Mi": "0",
                            "nvidia.com/gpu": "8",
                            "google.com/tpu": "4",
                        },
                        "conditions": [{"type": "Ready", "status": "True"}],
                    },
                },
            },
            {
                "timestamp": t0,
                "eventType": "New",
                "payload": {
                    "apiVersion": "kueue.x-k8s.io/v1beta2",
                    "kind": "ResourceFlavor",
                    "metadata": {"name": "rf-main"},
                    "spec": {},
                },
            },
            {
                "timestamp": t0,
                "eventType": "New",
                "payload": {
                    "apiVersion": "kueue.x-k8s.io/v1beta2",
                    "kind": "ClusterQueue",
                    "metadata": {"name": "cq-main"},
                    "spec": {
                        "admissionChecks": ["prov-check-1"],
                        "admissionCheckStrategy": {"admissionChecks": [{"name": "prov-check-2"}]},
                        "resourceGroups": [
                            {
                                "coveredResources": ["cpu", "memory", "google.com/tpu", "nvidia.com/gpu"],
                                "flavors": [
                                    {
                                        "name": "rf-main",
                                        "resources": [
                                            {"name": "cpu", "nominalQuota": "64"},
                                            {"name": "memory", "nominalQuota": "128Gi"},
                                            {"name": "google.com/tpu", "nominalQuota": "4"},
                                            {"name": "nvidia.com/gpu", "nominalQuota": "8"},
                                        ],
                                    }
                                ],
                            }
                        ],
                    },
                },
            },
            {
                "timestamp": t0,
                "eventType": "New",
                "payload": {
                    "apiVersion": "kueue.x-k8s.io/v1beta2",
                    "kind": "LocalQueue",
                    "metadata": {"name": "lq-main", "namespace": "ai-lab"},
                    "spec": {"clusterQueue": "cq-main"},
                },
            },
        ]

    @staticmethod
    def _mk_job_event(
        ts: str,
        ev_type: str,
        state: str,
        name: str,
        *,
        ns: str = "ai-lab",
        lq: str = "lq-main",
        creation_time: str | None = None,
        parallelism: int = 1,
        completions: int = 1,
        priority_class_k8s: str = "",
        scheduler_name: str = "",
    ) -> dict:
        p_spec: dict = {
            "restartPolicy": "Never",
            "containers": [
                {
                    "name": "c",
                    "image": "registry.k8s.io/pause:3.10",
                    "resources": {"requests": {"cpu": "2", "memory": "4Gi", "google.com/tpu": "1"}},
                }
            ],
        }
        if priority_class_k8s:
            p_spec["priorityClassName"] = priority_class_k8s
            p_spec["priority"] = 99999
        if scheduler_name:
            p_spec["schedulerName"] = scheduler_name
        lbls = {"kueue.x-k8s.io/queue-name": lq} if lq else {}
        return {
            "timestamp": ts,
            "eventType": ev_type,
            "workloadState": state,
            "payload": {
                "objectKind": "Job",
                "name": name,
                "namespace": ns,
                "creationTime": creation_time or ts,
                "localQueueName": lq,
                "object": {
                    "apiVersion": "batch/v1",
                    "kind": "Job",
                    "metadata": {"name": name, "namespace": ns, "labels": lbls},
                    "spec": {
                        "suspend": state == "NotAdmitted",
                        "parallelism": parallelism,
                        "completions": completions,
                        "template": {"spec": p_spec},
                    },
                },
            },
        }

    def test_experiments_A_B_D_workflow_cancellation_running_deletion_and_same_name_recreate(self) -> None:
        t0 = "2026-11-22T10:00:00Z"
        with tempfile.TemporaryDirectory() as td:
            sc = Path(td)
            infra = self._base_infra_events(t0)
            wf = [
                # Exp A: cancelled while pending at +15s
                self._mk_job_event("2026-11-22T10:00:05Z", "New", "NotAdmitted", "job-cancel"),
                self._mk_job_event("2026-11-22T10:00:15Z", "Deleted", "NotAdmitted", "job-cancel"),
                # Exp B: running job deleted at +25s + terminal Failed job at +125s (must NOT multiply by 1.5x)
                self._mk_job_event("2026-11-22T10:00:05Z", "New", "NotAdmitted", "job-kill"),
                self._mk_job_event("2026-11-22T10:00:25Z", "Deleted", "Admitted", "job-kill"),
                self._mk_job_event("2026-11-22T10:00:05Z", "New", "NotAdmitted", "job-crash"),
                # Exp D: same-name delete & re-create ("train": 30s first run, deleted at +50s, re-created at +60s for 100s)
                self._mk_job_event("2026-11-22T10:00:10Z", "New", "NotAdmitted", "train", creation_time="2026-11-22T10:00:10Z"),
                self._mk_job_event("2026-11-22T10:00:50Z", "Deleted", "Admitted", "train", creation_time="2026-11-22T10:00:10Z"),
                self._mk_job_event("2026-11-22T10:01:00Z", "New", "NotAdmitted", "train", creation_time="2026-11-22T10:01:00Z"),
            ]
            ex = [
                {
                    "timestamp": "2026-11-22T10:00:25Z",
                    "eventType": "PartialExecution",
                    "workloadState": "NotAdmitted",
                    "payload": {
                        "executionType": "Partial",
                        "workloadName": "job-job-kill-111",
                        "namespace": "ai-lab",
                        "ownerKind": "Job",
                        "ownerName": "job-kill",
                        "localQueueName": "lq-main",
                        "creationTime": "2026-11-22T10:00:05Z",
                        "admittedTime": "2026-11-22T10:00:05Z",
                        "endTime": "2026-11-22T10:00:25Z",
                        "executionDurationSeconds": 20.0,
                        "interruptionReason": "Deleted",
                        "preempted": False,
                    },
                },
                {
                    "timestamp": "2026-11-22T10:02:05Z",
                    "eventType": "PartialExecution",
                    "workloadState": "NotAdmitted",
                    "payload": {
                        "executionType": "Partial",
                        "workloadName": "job-job-crash-222",
                        "namespace": "ai-lab",
                        "ownerKind": "Job",
                        "ownerName": "job-crash",
                        "localQueueName": "lq-main",
                        "creationTime": "2026-11-22T10:00:05Z",
                        "admittedTime": "2026-11-22T10:00:05Z",
                        "endTime": "2026-11-22T10:02:05Z",
                        "executionDurationSeconds": 120.0,
                        "interruptionReason": "Failed",
                        "preempted": False,
                    },
                },
                {
                    "timestamp": "2026-11-22T10:00:40Z",
                    "eventType": "FullExecution",
                    "workloadState": "Admitted",
                    "payload": {
                        "executionType": "Full",
                        "workloadName": "job-train-inc1",
                        "namespace": "ai-lab",
                        "ownerKind": "Job",
                        "ownerName": "train",
                        "localQueueName": "lq-main",
                        "creationTime": "2026-11-22T10:00:10Z",
                        "admittedTime": "2026-11-22T10:00:10Z",
                        "endTime": "2026-11-22T10:00:40Z",
                        "executionDurationSeconds": 30.0,
                        "preempted": False,
                    },
                },
                {
                    "timestamp": "2026-11-22T10:02:40Z",
                    "eventType": "FullExecution",
                    "workloadState": "Admitted",
                    "payload": {
                        "executionType": "Full",
                        "workloadName": "job-train-inc2",
                        "namespace": "ai-lab",
                        "ownerKind": "Job",
                        "ownerName": "train",
                        "localQueueName": "lq-main",
                        "creationTime": "2026-11-22T10:01:00Z",
                        "admittedTime": "2026-11-22T10:01:00Z",
                        "endTime": "2026-11-22T10:02:40Z",
                        "executionDurationSeconds": 100.0,
                        "preempted": False,
                    },
                },
            ]
            (sc / "infra.json").write_text("\n".join(json.dumps(x) for x in infra) + "\n", encoding="utf-8")
            (sc / "workflows.json").write_text("\n".join(json.dumps(x) for x in wf) + "\n", encoding="utf-8")
            (sc / "execution-time.json").write_text("\n".join(json.dumps(x) for x in ex) + "\n", encoding="utf-8")

            bundle = testrunner.load_scenario_bundle(sc, skip_initial_offset=False)
            by_name_inc = {(w.name, w.incarnation_index): w for w in bundle.scheduled_workflows}
            # Failed and Deleted PartialExecution stay at 1.0x (120.0s and 20.0s), NOT 1.5x!
            self.assertEqual(by_name_inc[("job-kill", 0)].execution_duration_sec, 20.0)
            self.assertEqual(by_name_inc[("job-crash", 0)].execution_duration_sec, 120.0)
            # Both incarnations of 'train' are scheduled separately with 30.0s and 100.0s!
            self.assertEqual(by_name_inc[("train", 0)].execution_duration_sec, 30.0)
            self.assertEqual(by_name_inc[("train", 0)].raw_offset_from_t0_sec, 10.0)
            self.assertEqual(by_name_inc[("train", 1)].execution_duration_sec, 100.0)
            self.assertEqual(by_name_inc[("train", 1)].raw_offset_from_t0_sec, 60.0)

            del_reasons = {(d.name, round(d.raw_offset_from_t0_sec, 1)): d.delete_reason for d in bundle.scheduled_workflow_deletions}
            self.assertEqual(del_reasons[("job-cancel", 15.0)], "cancelled")
            self.assertEqual(del_reasons[("job-kill", 25.0)], "cancelled")
            self.assertEqual(del_reasons[("train", 50.0)], "before_recreate")

    def test_experiments_C_I_J_post_t0_fast_admitted_non_kueue_jobs_and_dynamic_cq_quota(self) -> None:
        t0 = "2026-11-22T10:00:00Z"
        with tempfile.TemporaryDirectory() as td:
            sc = Path(td)
            infra = self._base_infra_events(t0)
            # Post-t0 ClusterQueue quota expansion at +30s & post-t0 non-Kueue Pod at +20s
            cq_upd = json.loads(json.dumps(infra[2]))
            cq_upd["timestamp"] = "2026-11-22T10:00:30Z"
            cq_upd["eventType"] = "Update"
            cq_upd["payload"]["spec"]["resourceGroups"][0]["flavors"][0]["resources"][2]["nominalQuota"] = "16"
            infra.append(cq_upd)
            infra.append(
                {
                    "timestamp": "2026-11-22T10:00:20Z",
                    "eventType": "New",
                    "payload": {
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "name": "daemon-agent-xyz",
                        "namespace": "kube-system",
                        "placement": "node-0",
                        "resourceRequest": {"cpu": "2", "memory": "4Gi"},
                        "deletionTime": "2026-11-22T10:00:50Z",
                    },
                }
            )
            wf = [
                # Non-Kueue system job and unqueued user job -> must NOT become t0 workloads or invent <cq>-init-lq!
                self._mk_job_event(t0, "New", "Admitted", "cert-rotator", ns="kube-system", lq=""),
                self._mk_job_event(t0, "New", "Admitted", "cron-backup", ns="batch-ops", lq=""),
                # Fast-admitting Kueue job created at +45s (first seen Admitted) -> scheduled at +45s, NOT t0!
                self._mk_job_event("2026-11-22T10:00:45Z", "New", "Admitted", "late-fast-job"),
            ]
            ex = [
                {
                    "timestamp": "2026-11-22T10:01:05Z",
                    "eventType": "FullExecution",
                    "workloadState": "Admitted",
                    "payload": {
                        "executionType": "Full",
                        "workloadName": "job-late-fast-1",
                        "namespace": "ai-lab",
                        "ownerKind": "Job",
                        "ownerName": "late-fast-job",
                        "localQueueName": "lq-main",
                        "creationTime": "2026-11-22T10:00:45Z",
                        "admittedTime": "2026-11-22T10:00:45Z",
                        "endTime": "2026-11-22T10:01:05Z",
                        "executionDurationSeconds": 20.0,
                        "preempted": False,
                    },
                }
            ]
            (sc / "infra.json").write_text("\n".join(json.dumps(x) for x in infra) + "\n", encoding="utf-8")
            (sc / "workflows.json").write_text("\n".join(json.dumps(x) for x in wf) + "\n", encoding="utf-8")
            (sc / "execution-time.json").write_text("\n".join(json.dumps(x) for x in ex) + "\n", encoding="utf-8")

            bundle = testrunner.load_scenario_bundle(sc, skip_initial_offset=False)
            self.assertEqual(len(bundle.initial_workloads), 0)
            lq_names = [d["metadata"]["name"] for d in bundle.prereq_and_queue_docs if d.get("kind") == "LocalQueue"]
            self.assertEqual(lq_names, ["lq-main"])
            self.assertEqual(len(bundle.scheduled_workflows), 1)
            self.assertEqual(bundle.scheduled_workflows[0].name, "late-fast-job")
            self.assertEqual(bundle.scheduled_workflows[0].raw_offset_from_t0_sec, 45.0)
            self.assertEqual(bundle.scheduled_workflows[0].trigger_reason, "post_t0_fast_admitted")

            infra_kinds = [(ie.object_kind, ie.event_type, round(ie.raw_offset_from_t0_sec, 1)) for ie in bundle.scheduled_infra_events]
            self.assertIn(("Pod", "New", 20.0), infra_kinds)
            self.assertIn(("ClusterQueue", "Update", 30.0), infra_kinds)

    def test_experiments_E_F_G_H_fifo_backlog_t0_preempt_full_waves_and_imputation(self) -> None:
        t0 = "2026-11-22T10:00:00Z"
        with tempfile.TemporaryDirectory() as td:
            sc = Path(td)
            infra = self._base_infra_events(t0)
            # Exp F: t0 pod preempted at +100s in trace, then re-admitted and finished FullExecution=800s at +1000s.
            infra.append(
                {
                    "timestamp": t0,
                    "eventType": "New",
                    "payload": {
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "name": "t0-preempt-full-0",
                        "namespace": "ai-lab",
                        "placement": "node-0",
                        "workloadName": "wl-t0-preempt-full",
                        "ownerKind": "Job",
                        "ownerName": "t0-preempt-full",
                        "resourceRequest": {"cpu": "2", "memory": "4Gi", "google.com/tpu": "1"},
                        "deletionTime": "2026-11-22T10:01:40Z",  # +100s preemption
                    },
                }
            )
            infra.append(
                {
                    "timestamp": t0,
                    "eventType": "New",
                    "payload": {
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "name": "t0-scrape-end-0",
                        "namespace": "ai-lab",
                        "placement": "node-0",
                        "workloadName": "wl-t0-scrape-end",
                        "ownerKind": "Job",
                        "ownerName": "t0-scrape-end",
                        "resourceRequest": {"cpu": "2", "memory": "4Gi", "google.com/tpu": "1"},
                    },
                }
            )
            wf = [
                self._mk_job_event(t0, "New", "Admitted", "t0-preempt-full", creation_time="2026-11-22T09:50:00Z"),
                self._mk_job_event(t0, "New", "Admitted", "t0-scrape-end", creation_time="2026-11-22T09:58:00Z"),
                # Exp E: listed alphabetically (alpha, mid, zeta), created zeta (-300s), mid (-200s), alpha (-100s)
                self._mk_job_event(t0, "New", "NotAdmitted", "alpha", creation_time="2026-11-22T09:58:20Z"),
                self._mk_job_event(t0, "New", "NotAdmitted", "mid", creation_time="2026-11-22T09:56:40Z"),
                self._mk_job_event(t0, "New", "NotAdmitted", "zeta", creation_time="2026-11-22T09:55:00Z"),
                # Exp G: completions=10, parallelism=2 (5 waves) with 100s FullExecution -> 20s per pod!
                self._mk_job_event(
                    "2026-11-22T10:00:10Z",
                    "New",
                    "NotAdmitted",
                    "wave-job",
                    parallelism=2,
                    completions=10,
                ),
                # Exp H: starved job with no execution-time.json entry -> imputed from 80s peer median!
                self._mk_job_event("2026-11-22T10:00:20Z", "New", "NotAdmitted", "starved-job"),
            ]
            ex = [
                {
                    "timestamp": t0,
                    "eventType": "Admitted",
                    "workloadState": "Admitted",
                    "payload": {
                        "workloadName": "wl-t0-preempt-full",
                        "namespace": "ai-lab",
                        "ownerKind": "Job",
                        "ownerName": "t0-preempt-full",
                        "localQueueName": "lq-main",
                        "clusterQueueName": "cq-main",
                        "creationTime": "2026-11-22T09:50:00Z",
                        "admittedTime": t0,
                    },
                },
                {
                    "timestamp": "2026-11-22T10:01:40Z",
                    "eventType": "PartialExecution",
                    "workloadState": "NotAdmitted",
                    "payload": {
                        "executionType": "Partial",
                        "workloadName": "wl-t0-preempt-full",
                        "namespace": "ai-lab",
                        "ownerKind": "Job",
                        "ownerName": "t0-preempt-full",
                        "localQueueName": "lq-main",
                        "admittedTime": t0,
                        "endTime": "2026-11-22T10:01:40Z",
                        "executionDurationSeconds": 100.0,
                        "interruptionReason": "Preempted",
                        "preempted": True,
                    },
                },
                {
                    "timestamp": "2026-11-22T10:16:40Z",
                    "eventType": "FullExecution",
                    "workloadState": "Admitted",
                    "payload": {
                        "executionType": "Full",
                        "workloadName": "wl-t0-preempt-full",
                        "namespace": "ai-lab",
                        "ownerKind": "Job",
                        "ownerName": "t0-preempt-full",
                        "localQueueName": "lq-main",
                        "admittedTime": "2026-11-22T10:03:20Z",
                        "endTime": "2026-11-22T10:16:40Z",
                        "executionDurationSeconds": 800.0,
                        "preempted": False,
                    },
                },
                {
                    "timestamp": "2026-11-22T10:00:40Z",
                    "eventType": "PartialExecution",
                    "workloadState": "Admitted",
                    "payload": {
                        "executionType": "Partial",
                        "workloadName": "wl-t0-scrape-end",
                        "namespace": "ai-lab",
                        "ownerKind": "Job",
                        "ownerName": "t0-scrape-end",
                        "localQueueName": "lq-main",
                        "clusterQueueName": "cq-main",
                        "creationTime": "2026-11-22T09:58:00Z",
                        "admittedTime": t0,
                        "endTime": "2026-11-22T10:00:40Z",
                        "executionDurationSeconds": 40.0,
                        "interruptionReason": "ScrapeEnded",
                        "preempted": False,
                    },
                },
                *[
                    {
                        "timestamp": "2026-11-22T10:01:20Z",
                        "eventType": "FullExecution",
                        "workloadState": "Admitted",
                        "payload": {
                            "executionType": "Full",
                            "workloadName": f"wl-{nm}",
                            "namespace": "ai-lab",
                            "ownerKind": "Job",
                            "ownerName": nm,
                            "localQueueName": "lq-main",
                            "admittedTime": t0,
                            "endTime": "2026-11-22T10:01:20Z",
                            "executionDurationSeconds": 80.0,
                            "preempted": False,
                        },
                    }
                    for nm in ("alpha", "mid", "zeta")
                ],
                {
                    "timestamp": "2026-11-22T10:01:50Z",
                    "eventType": "FullExecution",
                    "workloadState": "Admitted",
                    "payload": {
                        "executionType": "Full",
                        "workloadName": "wl-wave-job",
                        "namespace": "ai-lab",
                        "ownerKind": "Job",
                        "ownerName": "wave-job",
                        "localQueueName": "lq-main",
                        "admittedTime": "2026-11-22T10:00:10Z",
                        "endTime": "2026-11-22T10:01:50Z",
                        "executionDurationSeconds": 100.0,
                        "preempted": False,
                    },
                },
            ]
            (sc / "infra.json").write_text("\n".join(json.dumps(x) for x in infra) + "\n", encoding="utf-8")
            (sc / "workflows.json").write_text("\n".join(json.dumps(x) for x in wf) + "\n", encoding="utf-8")
            (sc / "execution-time.json").write_text("\n".join(json.dumps(x) for x in ex) + "\n", encoding="utf-8")

            with self.assertRaises(ValueError):
                testrunner.load_scenario_bundle(sc, skip_initial_offset=False, missing_duration_policy="error")

            bundle = testrunner.load_scenario_bundle(sc, skip_initial_offset=False, missing_duration_policy="impute")
            t0_backlog_names = [w.name for w in bundle.scheduled_workflows if w.target_wall_offset_sec == 0.0]
            self.assertEqual(t0_backlog_names, ["zeta", "mid", "alpha"])

            iwl_by_owner = {w.owner_name: w for w in bundle.initial_workloads}
            # Exp F: uninterrupted 800s (NOT truncated 100s), and ScrapeEnded gets 1.5x * 40s = 60s (NOT 10 days)!
            self.assertAlmostEqual(iwl_by_owner["t0-preempt-full"].pods[0].remaining_duration_sec, 800.0)
            self.assertFalse(iwl_by_owner["t0-scrape-end"].pods[0].is_infinite)
            self.assertAlmostEqual(iwl_by_owner["t0-scrape-end"].pods[0].remaining_duration_sec, 60.0)
            # Seniority order: t0-preempt-full (09:50) comes before t0-scrape-end (09:58)
            self.assertEqual([w.owner_name for w in bundle.initial_workloads], ["t0-preempt-full", "t0-scrape-end"])

            sw_by_name = {w.name: w for w in bundle.scheduled_workflows}
            # Exp G: 100s / ceil(10 / 2) = 20s per pod!
            wave_pod_ann = sw_by_name["wave-job"].manifest["spec"]["template"]["metadata"]["annotations"][
                "kueue-optimizer.gke.io/pod-execution-time-seconds"
            ]
            self.assertEqual(wave_pod_ann, "20")
            # Exp H: starved-job imputed from 1-pod lq-main peers (80.0s), NOT 10.0s fallback!
            self.assertEqual(sw_by_name["starved-job"].execution_duration_sec, 80.0)
            self.assertEqual(bundle.imputed_duration_count, 1)
            self.assertTrue(bundle.missing_duration_warnings)

    def test_experiments_K_L_and_compat_lws_ray_priorityclass_scheduler_admissionchecks_noexecute(self) -> None:
        from runner.replay import _fail_pods_bound_to_node

        t0 = "2026-11-22T10:00:00Z"
        with tempfile.TemporaryDirectory() as td:
            sc = Path(td)
            infra = self._base_infra_events(t0)
            # Add a NotReady node at t0 (Experiment K)
            infra.append(
                {
                    "timestamp": t0,
                    "eventType": "New",
                    "payload": {
                        "apiVersion": "v1",
                        "kind": "Node",
                        "metadata": {"name": "node-down-t0"},
                        "status": {
                            "allocatable": {"cpu": "64", "memory": "128Gi", "pods": "110"},
                            "capacity": {"cpu": "64", "memory": "128Gi", "pods": "110"},
                            "conditions": [{"type": "Ready", "status": "False"}],
                        },
                    },
                }
            )
            wf = [
                self._mk_job_event(
                    t0,
                    "New",
                    "NotAdmitted",
                    "job-custom",
                    priority_class_k8s="prod-tier-1",
                    scheduler_name="volcano-custom-sched",
                ),
                {
                    "timestamp": t0,
                    "eventType": "New",
                    "workloadState": "NotAdmitted",
                    "payload": {
                        "objectKind": "LeaderWorkerSet",
                        "name": "lws-infer",
                        "namespace": "ai-lab",
                        "localQueueName": "lq-main",
                        "object": {
                            "apiVersion": "leaderworkerset.x-k8s.io/v1",
                            "kind": "LeaderWorkerSet",
                            "metadata": {
                                "name": "lws-infer",
                                "namespace": "ai-lab",
                                "labels": {"kueue.x-k8s.io/queue-name": "lq-main"},
                            },
                            "spec": {
                                "replicas": 1,
                                "leaderWorkerTemplate": {
                                    "size": 4,
                                    "leaderTemplate": {
                                        "spec": {
                                            "containers": [
                                                {
                                                    "name": "leader",
                                                    "resources": {"requests": {"cpu": "2", "memory": "4Gi"}},
                                                }
                                            ]
                                        }
                                    },
                                    "workerTemplate": {
                                        "spec": {
                                            "containers": [
                                                {
                                                    "name": "worker",
                                                    "resources": {"requests": {"cpu": "2", "memory": "4Gi"}},
                                                }
                                            ]
                                        }
                                    },
                                },
                            },
                        },
                    },
                },
                {
                    "timestamp": t0,
                    "eventType": "New",
                    "workloadState": "NotAdmitted",
                    "payload": {
                        "objectKind": "RayJob",
                        "name": "ray-train",
                        "namespace": "ai-lab",
                        "localQueueName": "lq-main",
                        "object": {
                            "apiVersion": "ray.io/v1",
                            "kind": "RayJob",
                            "metadata": {
                                "name": "ray-train",
                                "namespace": "ai-lab",
                                "labels": {"kueue.x-k8s.io/queue-name": "lq-main"},
                            },
                            "spec": {
                                "suspend": True,
                                "rayClusterSpec": {
                                    "headGroupSpec": {
                                        "template": {
                                            "spec": {
                                                "containers": [
                                                    {
                                                        "name": "head",
                                                        "resources": {"requests": {"cpu": "2", "memory": "4Gi"}},
                                                    }
                                                ]
                                            }
                                        }
                                    },
                                    "workerGroupSpecs": [
                                        {
                                            "groupName": "gpu-workers",
                                            "replicas": 2,
                                            "template": {
                                                "spec": {
                                                    "containers": [
                                                        {
                                                            "name": "worker",
                                                            "resources": {"requests": {"cpu": "4", "memory": "8Gi"}},
                                                        }
                                                    ]
                                                }
                                            },
                                        }
                                    ],
                                },
                            },
                        },
                    },
                },
            ]
            (sc / "infra.json").write_text("\n".join(json.dumps(x) for x in infra) + "\n", encoding="utf-8")
            (sc / "workflows.json").write_text("\n".join(json.dumps(x) for x in wf) + "\n", encoding="utf-8")
            (sc / "execution-time.json").write_text("", encoding="utf-8")

            bundle = testrunner.load_scenario_bundle(sc, skip_initial_offset=False)

            # Exp L: hugepages-1Gi=0 skipped -> real accelerator selected!
            node0 = next(n for n in bundle.nodes if n["metadata"]["name"] == "node-0")
            self.assertIn(
                node0["metadata"]["annotations"]["kueue-optimizer.gke.io/node-accel-resource"],
                ("google.com/tpu", "nvidia.com/gpu"),
            )

            # ClusterQueue admissionChecks stripped & recorded in annotation
            cq_doc = next(d for d in bundle.prereq_and_queue_docs if d.get("kind") == "ClusterQueue")
            self.assertNotIn("admissionChecks", cq_doc["spec"])
            self.assertNotIn("admissionCheckStrategy", cq_doc["spec"])
            self.assertEqual(
                cq_doc["metadata"]["annotations"]["kueue-optimizer.gke.io/stripped-admission-checks"],
                "prov-check-1,prov-check-2",
            )

            # PodSpec priorityClassName synthesized as scheduling.k8s.io/v1 PriorityClass, custom schedulerName stripped
            k8s_pcs = [d for d in bundle.prereq_and_queue_docs if d.get("kind") == "PriorityClass"]
            self.assertEqual([d["metadata"]["name"] for d in k8s_pcs], ["prod-tier-1"])
            sw_by_name = {w.name: w for w in bundle.scheduled_workflows}
            jc_pspec = sw_by_name["job-custom"].manifest["spec"]["template"]["spec"]
            self.assertNotIn("schedulerName", jc_pspec)
            self.assertNotIn("priority", jc_pspec)
            self.assertEqual(jc_pspec["containers"][0]["resources"]["limits"]["google.com/tpu"], "1")

            # LWS & RayJob translated to runnable JobSets with exact pod counts (4 and 3)
            self.assertEqual(sw_by_name["lws-infer"].manifest["kind"], "JobSet")
            self.assertEqual(sw_by_name["lws-infer"].pod_count, 4)
            self.assertEqual(sw_by_name["ray-train"].manifest["kind"], "JobSet")
            self.assertEqual(sw_by_name["ray-train"].pod_count, 3)

            # Exp K: sync_kwok_nodes succeeds immediately when node-down-t0 is NotReady at t0
            mgr = ChamberManager(chamber_namespace="kueue-optimizer-chamber-fidk", apiserver_port=16499)
            try:
                patched_statuses: list[tuple[str, dict]] = []

                def fake_node_api(method: str, path: str, **kw: object) -> tuple[int, bytes]:
                    if method == "PATCH" and path.endswith("/status"):
                        patched_statuses.append((path, json.loads(kw["body"].decode("utf-8"))))
                        return 200, b"{}"
                    if method == "GET" and path == "/api/v1/nodes":
                        return 200, json.dumps(
                            {
                                "items": [
                                    {
                                        "metadata": {"name": "node-0", "labels": {"type": "kwok"}},
                                        "status": {
                                            "conditions": [{"type": "Ready", "status": "True"}],
                                            "allocatable": node0["status"]["allocatable"],
                                        },
                                    },
                                    {
                                        "metadata": {"name": "node-down-t0", "labels": {"type": "kwok"}},
                                        "status": {
                                            "conditions": [{"type": "Ready", "status": "False"}],
                                            "allocatable": {"cpu": "64", "memory": "128Gi", "pods": "110"},
                                        },
                                    },
                                ]
                            }
                        ).encode("utf-8")
                    if method == "GET" and path.startswith("/api/v1/pods?fieldSelector="):
                        return 200, json.dumps(
                            {
                                "items": [
                                    {
                                        "metadata": {"name": "tol-pod", "namespace": "ai-lab"},
                                        "spec": {
                                            "tolerations": [
                                                {"key": "maint", "operator": "Equal", "value": "true", "effect": "NoExecute"}
                                            ],
                                            "containers": [{"name": "c"}],
                                        },
                                        "status": {"phase": "Running"},
                                    },
                                    {
                                        "metadata": {"name": "untol-pod", "namespace": "ai-lab"},
                                        "spec": {"tolerations": [], "containers": [{"name": "c"}]},
                                        "status": {"phase": "Running"},
                                    },
                                ]
                            }
                        ).encode("utf-8")
                    return 200, b"{}"

                with (
                    mock.patch.object(mgr, "apply_manifests_parallel"),
                    mock.patch.object(mgr, "api_request", side_effect=fake_node_api),
                ):
                    mgr.sync_kwok_nodes(bundle.nodes, timeout_sec=2.0)
                    self.assertTrue(any("node-down-t0/status" in p for p, _ in patched_statuses))

                    failed_cnt = _fail_pods_bound_to_node(
                        mgr,
                        {"ai-lab"},
                        "node-0",
                        reason="TaintManagerEviction",
                        message="NoExecute taint",
                        no_execute_taints=[{"key": "maint", "value": "true", "effect": "NoExecute"}],
                    )
                    # Only untol-pod fails; tol-pod tolerates the NoExecute taint!
                    self.assertEqual(failed_cnt, 1)
            finally:
                mgr.close()

    def test_same_creation_epoch_batch_dispatches_in_parallel(self) -> None:
        from runner.loader import load_scenario_bundle
        from runner.replay import run_scenario_replay

        sc_dir = REPO_ROOT / "testscenarios" / "self-tests" / "st08-single-priority-deep-backlog-waves"
        bundle = load_scenario_bundle(
            scenario_dir=sc_dir,
            infra_path=None,
            workflows_path=None,
            execution_time_path=None,
            speedup=1.0,
            min_pod_duration_sec=0.1,
            infinite_pod_duration_sec=86400.0,
            default_exec_sec=35.0,
            initial_window_sec=1.0,
            skip_initial_offset=True,
            pause_image_override="",
            override_workload_namespace=None,
            override_queueing_strategy=None,
            override_cq_fair_sharing_preemption=None,
        )
        mgr = ChamberManager(chamber_namespace="kueue-optimizer-chamber-ut-st08", workload_namespace="st-workloads")
        calls: list[tuple[int, int]] = []
        try:
            def rec_parallel(docs, max_workers=24):
                calls.append((len(docs), max_workers))

            with (
                mock.patch.object(mgr, "apply_manifests_parallel", side_effect=rec_parallel),
                mock.patch("runner.replay.query_cluster_status") as q_mock,
            ):
                q_mock.return_value = ClusterStatusSnapshot(
                    workloads_admitted=0,
                    workloads_pending=0,
                    workloads_finished=16,
                    pods_running=0,
                    pods_pending=0,
                    pods_succeeded=32,
                    pods_failed=0,
                    jobsets_completed=16,
                    jobsets_failed=0,
                    cq_summary=[],
                    workloads_query_ok=True,
                )
                run_scenario_replay(
                    mgr=mgr,
                    bundle=bundle,
                    speedup=1.0,
                    max_run_seconds=None,
                    status_interval_sec=3.0,
                    wait_for_completion=False,
                    initial_state_already_bootstrapped=True,
                )
            self.assertEqual(calls, [(16, 24)])
        finally:
            mgr.close()

    def test_wfpr_disable_feature_gate_and_unscheduled_timeout(self) -> None:
        """Verify Kueue > v0.19 DisableWaitForPodsReady gate, default-enabled summary, and unscheduledTimeout."""
        from chamber.manifests import (
            parse_kueue_configuration_payload,
            reconcile_wait_for_pods_ready_feature_gates,
        )

        # 1. Default omitted state: enabled in Kueue (> v0.19, timeout=30m), configured=False, no DisableWaitForPodsReady
        cfg_default = testrunner.resolve_wait_for_pods_ready_config()
        self.assertTrue(cfg_default.enabled)
        self.assertFalse(cfg_default.configured)
        self.assertIn("default (enabled in Kueue, timeout=30m)", cfg_default.summary_str())
        eff_def, fg_def = reconcile_wait_for_pods_ready_feature_gates(cfg_default, {})
        self.assertEqual(fg_def, {})

        # 2. Explicit --no-wait-for-pods-ready: enabled=False -> injects DisableWaitForPodsReady=true in controller_manager_config.yaml
        cfg_disabled = testrunner.resolve_wait_for_pods_ready_config(enabled=False)
        self.assertFalse(cfg_disabled.enabled)
        self.assertIn("DisableWaitForPodsReady=true", cfg_disabled.summary_str())
        mgr_dis = ChamberManager(
            chamber_namespace="kueue-optimizer-chamber-ut-wfpr-dis",
            wait_for_pods_ready=cfg_disabled,
        )
        try:
            base_dst = mgr_dis.prepare_tmp_deploy_dir(prefer_cluster_pki=False)
            rendered_dis = (base_dst / "config" / "controller_manager_config.yaml").read_text(encoding="utf-8")
            self.assertNotIn("waitForPodsReady:", rendered_dis)
            self.assertIn("DisableWaitForPodsReady: true", rendered_dis)
        finally:
            mgr_dis.close()

        # 3. unscheduledTimeout scaling, auto-enabling WaitForPodsReadyUnscheduledTimeout=true, and <= timeout validation
        cfg_unsch = testrunner.resolve_wait_for_pods_ready_config(
            raw_timeout="60s",
            raw_unscheduled_timeout="20s",
            speedup=5.0,
        )
        self.assertTrue(cfg_unsch.enabled)
        self.assertTrue(cfg_unsch.configured)
        self.assertAlmostEqual(cfg_unsch.effective_timeout_sec, 12.0)
        self.assertAlmostEqual(cfg_unsch.effective_unscheduled_timeout_sec or 0.0, 4.0)
        mgr_unsch = ChamberManager(
            chamber_namespace="kueue-optimizer-chamber-ut-wfpr-unsch",
            wait_for_pods_ready=cfg_unsch,
        )
        try:
            base_dst2 = mgr_unsch.prepare_tmp_deploy_dir(prefer_cluster_pki=False)
            rendered_unsch = (base_dst2 / "config" / "controller_manager_config.yaml").read_text(encoding="utf-8")
            self.assertIn("unscheduledTimeout: 4s", rendered_unsch)
            self.assertIn("WaitForPodsReadyUnscheduledTimeout: true", rendered_unsch)
        finally:
            mgr_unsch.close()

        with self.assertRaises(ValueError):
            # unscheduledTimeout > timeout must be rejected
            testrunner.resolve_wait_for_pods_ready_config(raw_timeout="10s", raw_unscheduled_timeout="20s")
        with self.assertRaises(ValueError):
            # DisableWaitForPodsReady + WaitForPodsReadyUnscheduledTimeout must be rejected
            reconcile_wait_for_pods_ready_feature_gates(
                cfg_disabled,
                {"WaitForPodsReadyUnscheduledTimeout": True},
            )

        # Legacy v1beta1 waitForPodsReady.enable=false normalizes to DisableWaitForPodsReady=true
        parsed_legacy = parse_kueue_configuration_payload(
            {
                "apiVersion": "config.kueue.x-k8s.io/v1beta1",
                "kind": "Configuration",
                "waitForPodsReady": {"enable": False, "timeout": "10m"},
            }
        )
        self.assertIsNone(parsed_legacy.wfpr_raw)
        self.assertTrue(parsed_legacy.feature_gates.get("DisableWaitForPodsReady"))

    def test_deferred_queue_deletion_and_incarnation_cancellation_fidelity(self) -> None:
        """Verify CQ/LQ deletions defer while workloads remain active/queued, and incarnation-0 cancel spares incarnation-1."""
        from runner.loader import load_scenario_bundle
        from runner.replay import _execute_scheduled_infra_event, run_scenario_replay

        with tempfile.TemporaryDirectory() as td:
            sc_dir = Path(td)
            infra_lines = [
                {
                    "timestamp": "2026-06-01T00:00:00Z",
                    "eventType": "New",
                    "payload": {
                        "apiVersion": "v1",
                        "kind": "Node",
                        "metadata": {"name": "node-0", "labels": {"type": "kwok"}},
                        "status": {"capacity": {"cpu": "16", "memory": "64Gi", "pods": "110"}, "allocatable": {"cpu": "16", "memory": "64Gi", "pods": "110"}},
                    },
                },
                {
                    "timestamp": "2026-06-01T00:00:00Z",
                    "eventType": "New",
                    "payload": {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "ai-lab"}},
                },
                {
                    "timestamp": "2026-06-01T00:00:00Z",
                    "eventType": "New",
                    "payload": {
                        "apiVersion": "kueue.x-k8s.io/v1beta2",
                        "kind": "ResourceFlavor",
                        "metadata": {"name": "rf-main"},
                        "spec": {"nodeLabels": {"type": "kwok"}},
                    },
                },
                {
                    "timestamp": "2026-06-01T00:00:00Z",
                    "eventType": "New",
                    "payload": {
                        "apiVersion": "kueue.x-k8s.io/v1beta2",
                        "kind": "ClusterQueue",
                        "metadata": {"name": "cq-main"},
                        "spec": {
                            "namespaceSelector": {},
                            "resourceGroups": [
                                {
                                    "coveredResources": ["cpu", "memory"],
                                    "flavors": [
                                        {
                                            "name": "rf-main",
                                            "resources": [
                                                {"name": "cpu", "nominalQuota": "16"},
                                                {"name": "memory", "nominalQuota": "64Gi"},
                                            ],
                                        }
                                    ],
                                }
                            ],
                        },
                    },
                },
                {
                    "timestamp": "2026-06-01T00:00:00Z",
                    "eventType": "New",
                    "payload": {
                        "apiVersion": "kueue.x-k8s.io/v1beta2",
                        "kind": "LocalQueue",
                        "metadata": {"name": "lq-main", "namespace": "ai-lab"},
                        "spec": {"clusterQueue": "cq-main"},
                    },
                },
                # Premature CQ deletion at t=3s while workloads are still active/queued!
                {
                    "timestamp": "2026-06-01T00:00:03Z",
                    "eventType": "Deleted",
                    "payload": {
                        "apiVersion": "kueue.x-k8s.io/v1beta2",
                        "kind": "ClusterQueue",
                        "metadata": {"name": "cq-main"},
                    },
                },
            ]
            # Incarnation 0 of 'train' created at t=0s, cancelled while running at t=2s (PartialExecution Deleted);
            # Incarnation 1 of 'train' created at t=4s (still in event_heap when t=2s cancel fires!) and finishes!
            wf_lines = [
                {
                    "timestamp": "2026-06-01T00:00:00Z",
                    "eventType": "New",
                    "workloadState": "NotAdmitted",
                    "payload": {
                        "objectKind": "Job",
                        "name": "train",
                        "namespace": "ai-lab",
                        "creationTime": "2026-06-01T00:00:00Z",
                        "localQueueName": "lq-main",
                        "object": {
                            "apiVersion": "batch/v1",
                            "kind": "Job",
                            "metadata": {"name": "train", "namespace": "ai-lab", "labels": {"kueue.x-k8s.io/queue-name": "lq-main"}},
                            "spec": {"parallelism": 1, "completions": 1, "template": {"spec": {"containers": [{"name": "c", "image": "pause", "resources": {"requests": {"cpu": "2", "memory": "4Gi"}}}]}}},
                        },
                    },
                },
                {
                    "timestamp": "2026-06-01T00:00:02Z",
                    "eventType": "Deleted",
                    "workloadState": "Admitted",
                    "payload": {
                        "objectKind": "Job",
                        "name": "train",
                        "namespace": "ai-lab",
                        "creationTime": "2026-06-01T00:00:00Z",
                        "localQueueName": "lq-main",
                        "object": {"apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": "train", "namespace": "ai-lab"}},
                    },
                },
                {
                    "timestamp": "2026-06-01T00:00:04Z",
                    "eventType": "New",
                    "workloadState": "NotAdmitted",
                    "payload": {
                        "objectKind": "Job",
                        "name": "train",
                        "namespace": "ai-lab",
                        "creationTime": "2026-06-01T00:00:04Z",
                        "localQueueName": "lq-main",
                        "object": {
                            "apiVersion": "batch/v1",
                            "kind": "Job",
                            "metadata": {"name": "train", "namespace": "ai-lab", "labels": {"kueue.x-k8s.io/queue-name": "lq-main"}},
                            "spec": {"parallelism": 1, "completions": 1, "template": {"spec": {"containers": [{"name": "c", "image": "pause", "resources": {"requests": {"cpu": "2", "memory": "4Gi"}}}]}}},
                        },
                    },
                },
            ]
            exec_lines = [
                {
                    "timestamp": "2026-06-01T00:00:00Z",
                    "eventType": "Admitted",
                    "workloadState": "Admitted",
                    "payload": {
                        "workloadName": "job-train-inc0",
                        "namespace": "ai-lab",
                        "ownerKind": "Job",
                        "ownerName": "train",
                        "localQueueName": "lq-main",
                        "clusterQueueName": "cq-main",
                        "creationTime": "2026-06-01T00:00:00Z",
                        "admittedTime": "2026-06-01T00:00:00Z",
                    },
                },
                {
                    "timestamp": "2026-06-01T00:00:02Z",
                    "eventType": "PartialExecution",
                    "workloadState": "NotAdmitted",
                    "payload": {
                        "executionType": "Partial",
                        "workloadName": "job-train-inc0",
                        "namespace": "ai-lab",
                        "ownerKind": "Job",
                        "ownerName": "train",
                        "creationTime": "2026-06-01T00:00:00Z",
                        "admittedTime": "2026-06-01T00:00:00Z",
                        "endTime": "2026-06-01T00:00:02Z",
                        "executionDurationSeconds": 2,
                        "interruptionReason": "Deleted",
                    },
                },
                {
                    "timestamp": "2026-06-01T00:00:10Z",
                    "eventType": "FullExecution",
                    "workloadState": "Admitted",
                    "payload": {
                        "executionType": "Full",
                        "workloadName": "job-train-inc1",
                        "namespace": "ai-lab",
                        "ownerKind": "Job",
                        "ownerName": "train",
                        "creationTime": "2026-06-01T00:00:04Z",
                        "admittedTime": "2026-06-01T00:00:04Z",
                        "endTime": "2026-06-01T00:00:10Z",
                        "executionDurationSeconds": 6,
                    },
                },
            ]
            (sc_dir / "infra.json").write_text("\n".join(json.dumps(x) for x in infra_lines) + "\n", encoding="utf-8")
            (sc_dir / "workflows.json").write_text("\n".join(json.dumps(x) for x in wf_lines) + "\n", encoding="utf-8")
            (sc_dir / "execution-time.json").write_text("\n".join(json.dumps(x) for x in exec_lines) + "\n", encoding="utf-8")

            bundle = load_scenario_bundle(
                scenario_dir=sc_dir,
                infra_path=None,
                workflows_path=None,
                execution_time_path=None,
                speedup=20.0,
                min_pod_duration_sec=0.1,
                infinite_pod_duration_sec=86400.0,
                default_exec_sec=10.0,
                initial_window_sec=0.5,
                skip_initial_offset=True,
                pause_image_override="",
                override_workload_namespace=None,
                override_queueing_strategy=None,
                override_cq_fair_sharing_preemption=None,
            )
            self.assertEqual(len(bundle.scheduled_workflows), 2)
            self.assertEqual([w.incarnation_index for w in bundle.scheduled_workflows], [0, 1])
            self.assertEqual(len(bundle.scheduled_workflow_deletions), 1)
            self.assertEqual(bundle.scheduled_workflow_deletions[0].incarnation_index, 0)
            self.assertEqual(bundle.scheduled_workflow_deletions[0].delete_reason, "cancelled")

            mgr = ChamberManager(chamber_namespace="kueue-optimizer-chamber-ut-del", workload_namespace="ai-lab")
            submitted_docs: list[str] = []
            deleted_paths: list[str] = []
            cq_delete_calls: list[str] = []
            inc1_finished = False
            try:
                def fake_apply(docs, max_workers=24):
                    nonlocal inc1_finished
                    for d in docs:
                        nm = str((d.get("metadata") or {}).get("name") or "")
                        submitted_docs.append(nm)
                        if len(submitted_docs) == 2:
                            inc1_finished = True

                def fake_api_delete(path: str, ignore_not_found: bool = True) -> None:
                    cq_delete_calls.append(path)

                def fake_api_req(method: str, path: str, **_kw):
                    if method == "DELETE":
                        deleted_paths.append(path)
                        return 200, b"{}"
                    if method == "GET" and path.endswith("/pods"):
                        return 200, b'{"items":[]}'
                    if method == "GET" and "/jobs/" in path:
                        return 404, b""
                    if method == "GET" and path.endswith("/workloads"):
                        if inc1_finished:
                            return 200, json.dumps(
                                {
                                    "items": [
                                        {
                                            "metadata": {"name": "job-train-inc1", "namespace": "ai-lab"},
                                            "spec": {"queueName": "lq-main"},
                                            "status": {"conditions": [{"type": "Finished", "status": "True", "reason": "Succeeded"}]},
                                        }
                                    ]
                                }
                            ).encode("utf-8")
                        return 200, json.dumps(
                            {
                                "items": [
                                    {
                                        "metadata": {
                                            "name": "job-train-inc0",
                                            "namespace": "ai-lab",
                                            "ownerReferences": [{"name": "train"}],
                                        },
                                        "spec": {"queueName": "lq-main"},
                                        "status": {
                                            "admission": {"clusterQueue": "cq-main"},
                                            "conditions": [{"type": "Admitted", "status": "True"}],
                                        },
                                    }
                                ]
                            }
                        ).encode("utf-8")
                    return 200, b"{}"

                def fake_query_status(*_a, **_kw):
                    fin_keys = {("ai-lab", "job-train-inc1")} if inc1_finished else set()
                    return ClusterStatusSnapshot(
                        workloads_admitted=0 if inc1_finished else 1,
                        workloads_pending=0,
                        workloads_finished=len(fin_keys),
                        pods_running=0,
                        pods_pending=0,
                        pods_succeeded=1 if inc1_finished else 0,
                        pods_failed=0,
                        jobsets_completed=0,
                        jobsets_failed=0,
                        cq_summary=[],
                        finished_workload_keys=fin_keys,
                        present_workload_keys=fin_keys,
                        workloads_query_ok=True,
                    )

                with (
                    mock.patch.object(mgr, "apply_manifests_parallel", side_effect=fake_apply),
                    mock.patch.object(mgr, "api_delete", side_effect=fake_api_delete),
                    mock.patch.object(mgr, "api_request", side_effect=fake_api_req),
                    mock.patch("runner.replay.query_cluster_status", side_effect=fake_query_status),
                ):
                    _, _, sub_cnt, _ = run_scenario_replay(
                        mgr=mgr,
                        bundle=bundle,
                        speedup=20.0,
                        max_run_seconds=5.0,
                        status_interval_sec=0.2,
                        wait_for_completion=True,
                        initial_state_already_bootstrapped=True,
                    )

                # BOTH incarnation 0 AND incarnation 1 were submitted (incarnation 1 was NOT dropped!),
                # running incarnation 0 WAS deleted via DELETE /apis/batch/v1/namespaces/ai-lab/jobs/train,
                # AND ClusterQueue 'cq-main' was NOT prematurely deleted at t=3s while incarnation 1 was still queued!
                self.assertEqual(sub_cnt, 2)
                self.assertEqual(len(submitted_docs), 2)
                self.assertTrue(any("/apis/batch/v1/namespaces/ai-lab/jobs/" in p for p in deleted_paths))
                self.assertEqual(cq_delete_calls, [])

                # Also verify directly that once all dependent workloads finish, _execute_scheduled_infra_event deletes CQ cleanly
                ie_del_cq = bundle.scheduled_infra_events[0]
                with (
                    mock.patch.object(mgr, "api_delete", side_effect=fake_api_delete),
                    mock.patch.object(mgr, "api_request", side_effect=fake_api_req),
                ):
                    deferred_after, _ = _execute_scheduled_infra_event(
                        mgr,
                        bundle,
                        ie_del_cq,
                        1.0,
                        event_heap=[],
                        cancelled_unsubmitted_keys=set(),
                        active_initial_wls=[],
                        lq_to_cq={("ai-lab", "lq-main"): "cq-main"},
                        cq_to_cohort={"cq-main": ""},
                        cq_to_flavors={"cq-main": {"rf-main"}},
                        submitted_non_cancelled=[],
                    )
                self.assertFalse(deferred_after)
                self.assertEqual(cq_delete_calls, ["/apis/kueue.x-k8s.io/v1beta2/clusterqueues/cq-main"])
            finally:
                mgr.close()

    def test_kueue_feature_gates_sanitizes_graduated_locked_deprecated_and_dependencies(self) -> None:
        from chamber.manifests import (
            discover_kueue_feature_gates_from_source,
            parse_kueue_configuration_payload,
            patch_kueue_feature_gates_config,
            sanitize_kueue_feature_gates,
        )

        cleaned, warnings = sanitize_kueue_feature_gates(
            {
                "LendingLimit": True,  # graduated/removed in older Kueue
                "ConfigurableResourceTransformations": True,  # graduated/removed
                "TLSOptions": False,  # GA LockToDefault=True
                "MetricForWorkloadCreationLatency": True,  # GA LockToDefault=True
                "TASReplaceNodeDueToNotReadyOverFixedTime": False,  # Deprecated sunset gate
                "DeferRayServiceFinalizationForRedisCleanup": True,  # Deprecated sunset gate
                "TopologyAwareScheduling": False,
                "TASProfileMixed": True,  # conflicts with TopologyAwareScheduling=False
                "SchedulerLibraryDeepIntegration": True,  # depends on SchedulerLibraryIntegration -> TopologyAwareScheduling=False!
                "DisableWaitForPodsReady": True,
            }
        )
        self.assertEqual(
            cleaned,
            {
                "DisableWaitForPodsReady": True,
                "TopologyAwareScheduling": False,
            },
        )
        self.assertTrue(any("LendingLimit" in w for w in warnings))
        self.assertTrue(any("TLSOptions" in w for w in warnings))
        self.assertTrue(any("DeferRayServiceFinalizationForRedisCleanup" in w for w in warnings))
        self.assertTrue(any("TASProfileMixed" in w for w in warnings))

        # Auto-enables default-off parent gate when parent is not explicitly disabled:
        cleaned_deps, dep_warnings = sanitize_kueue_feature_gates({"SchedulerLibraryDeepIntegration": True})
        self.assertEqual(
            cleaned_deps,
            {
                "SchedulerLibraryDeepIntegration": True,
                "SchedulerLibraryIntegration": True,
            },
        )
        self.assertEqual(dep_warnings, [])

        # parse_kueue_configuration_payload and patch_kueue_feature_gates_config strip unsupported gates cleanly:
        parsed_cfg = parse_kueue_configuration_payload(
            {
                "apiVersion": "config.kueue.x-k8s.io/v1beta2",
                "kind": "Configuration",
                "featureGates": {
                    "LendingLimit": True,
                    "TLSOptions": True,
                    "WaitForPodsReadyUnscheduledTimeout": True,
                },
            }
        )
        self.assertEqual(parsed_cfg.feature_gates, {"WaitForPodsReadyUnscheduledTimeout": True})
        self.assertGreaterEqual(len(parsed_cfg.feature_gate_warnings), 2)

        with tempfile.TemporaryDirectory() as tmp:
            base_dst = Path(tmp)
            cfg_dir = base_dst / "config"
            cfg_dir.mkdir(parents=True)
            cfg_file = cfg_dir / "controller_manager_config.yaml"
            cfg_file.write_text(
                "apiVersion: config.kueue.x-k8s.io/v1beta2\nkind: Configuration\ninternalCertManagement:\n  enable: false\n",
                encoding="utf-8",
            )
            written_fg = patch_kueue_feature_gates_config(
                base_dst,
                {"OldRemovedGate": True, "TLSOptions": True, "DisableWaitForPodsReady": True},
                emit_warnings=False,
            )
            self.assertEqual(written_fg, {"DisableWaitForPodsReady": True})
            rendered = cfg_file.read_text(encoding="utf-8")
            self.assertIn("DisableWaitForPodsReady: true", rendered)
            self.assertNotIn("OldRemovedGate", rendered)
            self.assertNotIn("TLSOptions", rendered)

            # Custom Kueue source checkout discovery recognizes newly defined experimental gates:
            fake_kueue = base_dst / "custom-kueue"
            (fake_kueue / "pkg" / "features").mkdir(parents=True)
            (fake_kueue / "pkg" / "features" / "kube_features.go").write_text(
                'package features\nconst (\n\tMyBrandNewGate featuregate.Feature = "MyBrandNewGate"\n)\n'
                'var defaultVersionedFeatureGates = map[featuregate.Feature]featuregate.VersionedSpecs{\n'
                '\tMyBrandNewGate: {\n\t\t{Version: version.MustParse("0.22"), Default: false, PreRelease: featuregate.Alpha},\n\t},\n}\n',
                encoding="utf-8",
            )
            disc = discover_kueue_feature_gates_from_source(fake_kueue)
            self.assertIsNotNone(disc)
            cleaned_custom, _ = sanitize_kueue_feature_gates(
                {"MyBrandNewGate": True, "OldRemovedGate": True},
                kueue_build_dir=fake_kueue,
            )
            self.assertEqual(cleaned_custom, {"MyBrandNewGate": True})

    def test_t0_bootstrap_stamps_pods_ready_true_when_admitted_is_backdated(self) -> None:
        from runner.loader import load_scenario_bundle
        from runner.replay import _stamp_initial_workloads_for_bootstrap

        with tempfile.TemporaryDirectory() as tmp:
            sc_dir = Path(tmp)
            infra_events = [
                {
                    "timestamp": "2026-01-01T02:00:00Z",
                    "eventType": "New",
                    "payload": {
                        "apiVersion": "v1",
                        "kind": "Node",
                        "metadata": {"name": "node-0", "labels": {"kubernetes.io/hostname": "node-0"}},
                        "status": {"capacity": {"cpu": "8", "memory": "16Gi"}, "allocatable": {"cpu": "8", "memory": "16Gi"}},
                    },
                },
                {
                    "timestamp": "2026-01-01T02:00:00Z",
                    "eventType": "New",
                    "payload": {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "ai-lab"}},
                },
                {
                    "timestamp": "2026-01-01T02:00:00Z",
                    "eventType": "New",
                    "payload": {"apiVersion": "kueue.x-k8s.io/v1beta2", "kind": "ResourceFlavor", "metadata": {"name": "rf-main"}, "spec": {}},
                },
                {
                    "timestamp": "2026-01-01T02:00:00Z",
                    "eventType": "New",
                    "payload": {
                        "apiVersion": "kueue.x-k8s.io/v1beta2",
                        "kind": "ClusterQueue",
                        "metadata": {"name": "cq-main"},
                        "spec": {
                            "namespaceSelector": {},
                            "resourceGroups": [{"coveredResources": ["cpu", "memory"], "flavors": [{"name": "rf-main", "resources": [{"name": "cpu", "nominalQuota": "8"}, {"name": "memory", "nominalQuota": "16Gi"}]}]}],
                        },
                    },
                },
                {
                    "timestamp": "2026-01-01T02:00:00Z",
                    "eventType": "New",
                    "payload": {"apiVersion": "kueue.x-k8s.io/v1beta2", "kind": "LocalQueue", "metadata": {"name": "lq-main", "namespace": "ai-lab"}, "spec": {"clusterQueue": "cq-main"}},
                },
                {
                    "timestamp": "2026-01-01T02:00:00Z",
                    "eventType": "New",
                    "payload": {
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "name": "long-job-0",
                        "namespace": "ai-lab",
                        "placement": "node-0",
                        "resourceRequest": {"cpu": "2", "memory": "4Gi"},
                        "ownerKind": "Job",
                        "ownerName": "long-job",
                        "workloadName": "job-long-job-12345",
                    },
                },
            ]
            # Admitted 2 hours BEFORE t0 (00:00:00Z vs t0=02:00:00Z), far older than default waitForPodsReady timeout!
            wf_events = [
                {
                    "timestamp": "2026-01-01T02:00:00Z",
                    "eventType": "New",
                    "workloadState": "Admitted",
                    "payload": {
                        "objectKind": "Job",
                        "name": "long-job",
                        "namespace": "ai-lab",
                        "creationTime": "2026-01-01T00:00:00Z",
                        "localQueueName": "lq-main",
                        "object": {
                            "apiVersion": "batch/v1",
                            "kind": "Job",
                            "metadata": {"name": "long-job", "namespace": "ai-lab", "labels": {"kueue.x-k8s.io/queue-name": "lq-main"}},
                            "spec": {
                                "parallelism": 1,
                                "completions": 1,
                                "template": {"spec": {"restartPolicy": "Never", "containers": [{"name": "c", "image": "pause", "resources": {"requests": {"cpu": "2", "memory": "4Gi"}}}]}},
                            },
                        },
                    },
                }
            ]
            exec_events = [
                {
                    "timestamp": "2026-01-01T02:00:00Z",
                    "eventType": "Admitted",
                    "workloadState": "Admitted",
                    "payload": {
                        "workloadName": "job-long-job-12345",
                        "namespace": "ai-lab",
                        "ownerKind": "Job",
                        "ownerName": "long-job",
                        "localQueueName": "lq-main",
                        "clusterQueueName": "cq-main",
                        "creationTime": "2026-01-01T00:00:00Z",
                        "admittedTime": "2026-01-01T00:00:00Z",
                    },
                },
                {
                    "timestamp": "2026-01-01T02:10:00Z",
                    "eventType": "FullExecution",
                    "workloadState": "Admitted",
                    "payload": {
                        "executionType": "Full",
                        "workloadName": "job-long-job-12345",
                        "namespace": "ai-lab",
                        "ownerKind": "Job",
                        "ownerName": "long-job",
                        "localQueueName": "lq-main",
                        "clusterQueueName": "cq-main",
                        "creationTime": "2026-01-01T00:00:00Z",
                        "admittedTime": "2026-01-01T00:00:00Z",
                        "endTime": "2026-01-01T02:10:00Z",
                        "executionDurationSeconds": 7800.0,
                    },
                },
            ]
            (sc_dir / "infra.json").write_text("\n".join(json.dumps(x) for x in infra_events) + "\n", encoding="utf-8")
            (sc_dir / "workflows.json").write_text("\n".join(json.dumps(x) for x in wf_events) + "\n", encoding="utf-8")
            (sc_dir / "execution-time.json").write_text("\n".join(json.dumps(x) for x in exec_events) + "\n", encoding="utf-8")

            bundle = load_scenario_bundle(sc_dir, speedup=1.0)
            self.assertEqual(len(bundle.initial_workloads), 1)
            iwl = bundle.initial_workloads[0]
            self.assertAlmostEqual(iwl.original_age_sec, 7200.0, places=1)
            cond_by_type = {c["type"]: c for c in iwl.workload_status["conditions"]}
            self.assertIn("PodsReady", cond_by_type)
            self.assertEqual(cond_by_type["PodsReady"]["status"], "True")
            self.assertEqual(cond_by_type["PodsReady"]["reason"], "Started")

            # Also verify _stamp_initial_workloads_for_bootstrap injects PodsReady=True if missing:
            iwl.workload_status["conditions"] = [c for c in iwl.workload_status["conditions"] if c["type"] != "PodsReady"]
            stamped = _stamp_initial_workloads_for_bootstrap([iwl])
            stamped_conds = {c["type"]: c for c in stamped[0][1]["conditions"]}
            self.assertEqual(stamped_conds["PodsReady"]["status"], "True")
            self.assertEqual(stamped_conds["PodsReady"]["reason"], "Started")
            self.assertEqual(stamped_conds["PodsReady"]["lastTransitionTime"], stamped_conds["Admitted"]["lastTransitionTime"])

    def test_before_recreate_delete_on_running_incarnation_does_not_hang_completion(self) -> None:
        from runner.loader import ScheduledWorkflowDeletion, load_scenario_bundle
        from runner.replay import _pod_belongs_to_workflow, run_scenario_replay

        # 1. Direct unit test of strict owner-based pod matching (Comment 5: no bare prefix match!)
        del_jobset = ScheduledWorkflowDeletion(
            target_wall_offset_sec=1.0,
            seq=0,
            raw_offset_sec=1.0,
            raw_offset_from_t0_sec=1.0,
            event_timestamp="2026-01-01T00:00:01Z",
            object_kind="JobSet",
            manifest_kind="JobSet",
            manifest_api_version="jobset.x-k8s.io/v1alpha2",
            name="train",
            applied_name="train",
            namespace="ai-lab",
            delete_reason="cancelled",
        )
        pod_owned = {
            "metadata": {
                "name": "train-worker-0-0-abc",
                "labels": {"jobset.sigs.k8s.io/jobset-name": "train", "job-name": "train-worker-0"},
            }
        }
        pod_sibling_dash_2 = {
            "metadata": {
                "name": "train-2-worker-0-0-xyz",
                "labels": {"jobset.sigs.k8s.io/jobset-name": "train-2", "job-name": "train-2-worker-0"},
            }
        }
        pod_sibling_eval = {
            "metadata": {
                "name": "train-eval-0-def",
                "labels": {"job-name": "train-eval"},
                "ownerReferences": [{"kind": "Job", "name": "train-eval"}],
            }
        }
        self.assertTrue(_pod_belongs_to_workflow(pod_owned, del_jobset, owned_job_names={"train-worker-0"}))
        self.assertFalse(_pod_belongs_to_workflow(pod_sibling_dash_2, del_jobset, owned_job_names={"train-worker-0"}))
        self.assertFalse(_pod_belongs_to_workflow(pod_sibling_eval, del_jobset, owned_job_names={"train-worker-0"}))

        # 2. Replay loop test for Comment 4 (before_recreate firing while incarnation 0 is still running)
        #    AND Comment 5 (sibling `train-2` & `train-eval` pods in namespace are NOT deleted when `train` is deleted).
        with tempfile.TemporaryDirectory() as tmp:
            sc_dir = Path(tmp)
            infra_events = [
                {
                    "timestamp": "2026-01-01T00:00:00Z",
                    "eventType": "New",
                    "payload": {
                        "apiVersion": "v1",
                        "kind": "Node",
                        "metadata": {"name": "node-0", "labels": {"kubernetes.io/hostname": "node-0"}},
                        "status": {"capacity": {"cpu": "8", "memory": "16Gi"}, "allocatable": {"cpu": "8", "memory": "16Gi"}},
                    },
                },
                {
                    "timestamp": "2026-01-01T00:00:00Z",
                    "eventType": "New",
                    "payload": {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "ai-lab"}},
                },
                {
                    "timestamp": "2026-01-01T00:00:00Z",
                    "eventType": "New",
                    "payload": {"apiVersion": "kueue.x-k8s.io/v1beta2", "kind": "ResourceFlavor", "metadata": {"name": "rf-main"}, "spec": {}},
                },
                {
                    "timestamp": "2026-01-01T00:00:00Z",
                    "eventType": "New",
                    "payload": {
                        "apiVersion": "kueue.x-k8s.io/v1beta2",
                        "kind": "ClusterQueue",
                        "metadata": {"name": "cq-main"},
                        "spec": {
                            "namespaceSelector": {},
                            "resourceGroups": [{"coveredResources": ["cpu", "memory"], "flavors": [{"name": "rf-main", "resources": [{"name": "cpu", "nominalQuota": "8"}, {"name": "memory", "nominalQuota": "16Gi"}]}]}],
                        },
                    },
                },
                {
                    "timestamp": "2026-01-01T00:00:00Z",
                    "eventType": "New",
                    "payload": {"apiVersion": "kueue.x-k8s.io/v1beta2", "kind": "LocalQueue", "metadata": {"name": "lq-main", "namespace": "ai-lab"}, "spec": {"clusterQueue": "cq-main"}},
                },
            ]
            job_obj = {
                "apiVersion": "batch/v1",
                "kind": "Job",
                "metadata": {"name": "train", "namespace": "ai-lab", "labels": {"kueue.x-k8s.io/queue-name": "lq-main"}},
                "spec": {
                    "parallelism": 1,
                    "completions": 1,
                    "template": {"spec": {"restartPolicy": "Never", "containers": [{"name": "c", "image": "pause", "resources": {"requests": {"cpu": "2", "memory": "4Gi"}}}]}},
                },
            }
            wf_events = [
                {
                    "timestamp": "2026-01-01T00:00:01Z",
                    "eventType": "New",
                    "workloadState": "NotAdmitted",
                    "payload": {
                        "objectKind": "Job",
                        "name": "train",
                        "namespace": "ai-lab",
                        "creationTime": "2026-01-01T00:00:01Z",
                        "localQueueName": "lq-main",
                        "object": job_obj,
                    },
                },
                {
                    "timestamp": "2026-01-01T00:00:05Z",
                    "eventType": "Deleted",
                    "workloadState": "Admitted",
                    "payload": {
                        "objectKind": "Job",
                        "name": "train",
                        "namespace": "ai-lab",
                        "creationTime": "2026-01-01T00:00:01Z",
                        "localQueueName": "lq-main",
                        "object": job_obj,
                    },
                },
                {
                    "timestamp": "2026-01-01T00:00:06Z",
                    "eventType": "New",
                    "workloadState": "NotAdmitted",
                    "payload": {
                        "objectKind": "Job",
                        "name": "train",
                        "namespace": "ai-lab",
                        "creationTime": "2026-01-01T00:00:06Z",
                        "localQueueName": "lq-main",
                        "object": job_obj,
                    },
                },
            ]
            # Historical FullExecution for incarnation 0 causes loader.py to emit delete_reason="before_recreate"!
            exec_events = [
                {
                    "timestamp": "2026-01-01T00:00:04Z",
                    "eventType": "FullExecution",
                    "workloadState": "Admitted",
                    "payload": {
                        "executionType": "Full",
                        "workloadName": "job-train-inc0",
                        "namespace": "ai-lab",
                        "ownerKind": "Job",
                        "ownerName": "train",
                        "localQueueName": "lq-main",
                        "clusterQueueName": "cq-main",
                        "creationTime": "2026-01-01T00:00:01Z",
                        "admittedTime": "2026-01-01T00:00:01Z",
                        "endTime": "2026-01-01T00:00:04Z",
                        "executionDurationSeconds": 3.0,
                    },
                },
                {
                    "timestamp": "2026-01-01T00:00:10Z",
                    "eventType": "FullExecution",
                    "workloadState": "Admitted",
                    "payload": {
                        "executionType": "Full",
                        "workloadName": "job-train-inc1",
                        "namespace": "ai-lab",
                        "ownerKind": "Job",
                        "ownerName": "train",
                        "localQueueName": "lq-main",
                        "clusterQueueName": "cq-main",
                        "creationTime": "2026-01-01T00:00:06Z",
                        "admittedTime": "2026-01-01T00:00:06Z",
                        "endTime": "2026-01-01T00:00:10Z",
                        "executionDurationSeconds": 4.0,
                    },
                },
            ]
            (sc_dir / "infra.json").write_text("\n".join(json.dumps(x) for x in infra_events) + "\n", encoding="utf-8")
            (sc_dir / "workflows.json").write_text("\n".join(json.dumps(x) for x in wf_events) + "\n", encoding="utf-8")
            (sc_dir / "execution-time.json").write_text("\n".join(json.dumps(x) for x in exec_events) + "\n", encoding="utf-8")

            bundle = load_scenario_bundle(sc_dir, speedup=20.0, skip_initial_offset=False)
            self.assertEqual(len(bundle.scheduled_workflow_deletions), 1)
            self.assertEqual(bundle.scheduled_workflow_deletions[0].delete_reason, "before_recreate")

            mgr = ChamberManager(chamber_namespace="kueue-optimizer-chamber-ut-recreate", apiserver_port=16498)
            try:
                sub_calls = 0
                inc1_done = False
                deleted_pod_paths: list[str] = []

                def fake_apply(docs, max_workers=16):
                    nonlocal sub_calls, inc1_done
                    sub_calls += len(docs)
                    if sub_calls >= 2:
                        inc1_done = True

                def fake_api_req(method: str, path: str, body=None, headers=None, content_type="application/json"):
                    if method == "DELETE" and "/pods/" in path:
                        deleted_pod_paths.append(path)
                        return 200, b"{}"
                    if method == "GET" and path == "/apis/batch/v1/namespaces/ai-lab/jobs/train":
                        return 404, b""
                    if method == "GET" and path == "/api/v1/namespaces/ai-lab/pods":
                        return 200, json.dumps(
                            {
                                "items": [
                                    {
                                        "metadata": {
                                            "name": "train-0-abc",
                                            "labels": {"job-name": "train"},
                                            "ownerReferences": [{"kind": "Job", "name": "train"}],
                                        }
                                    },
                                    {
                                        "metadata": {
                                            "name": "train-2-worker-0",
                                            "labels": {"jobset.sigs.k8s.io/jobset-name": "train-2"},
                                        }
                                    },
                                    {
                                        "metadata": {
                                            "name": "train-eval-0",
                                            "labels": {"job-name": "train-eval"},
                                            "ownerReferences": [{"kind": "Job", "name": "train-eval"}],
                                        }
                                    },
                                ]
                            }
                        ).encode("utf-8")
                    if method == "GET" and path == "/apis/kueue.x-k8s.io/v1beta2/namespaces/ai-lab/workloads":
                        # Incarnation 0 is STILL running (Admitted=True, NOT Finished) when before_recreate fires!
                        return 200, json.dumps(
                            {
                                "items": [
                                    {
                                        "metadata": {
                                            "name": "job-train-inc0",
                                            "namespace": "ai-lab",
                                            "ownerReferences": [{"kind": "Job", "name": "train"}],
                                        },
                                        "status": {"conditions": [{"type": "Admitted", "status": "True"}]},
                                    }
                                ]
                            }
                        ).encode("utf-8")
                    return 200, b"{}"

                def fake_query_status(*_a, **_kw):
                    fin = {("ai-lab", "job-train-inc1")} if inc1_done else set()
                    return ClusterStatusSnapshot(
                        workloads_admitted=0 if inc1_done else 1,
                        workloads_pending=0,
                        workloads_finished=len(fin),
                        pods_running=0,
                        pods_pending=0,
                        pods_succeeded=len(fin),
                        pods_failed=0,
                        jobsets_completed=0,
                        jobsets_failed=0,
                        cq_summary=[],
                        finished_workload_keys=fin,
                        present_workload_keys=fin,
                        workloads_query_ok=True,
                    )

                with (
                    mock.patch.object(mgr, "apply_manifests_parallel", side_effect=fake_apply),
                    mock.patch.object(mgr, "api_request", side_effect=fake_api_req),
                    mock.patch("runner.replay.query_cluster_status", side_effect=fake_query_status),
                ):
                    start_ep, end_ep, sub_cnt, _ = run_scenario_replay(
                        mgr=mgr,
                        bundle=bundle,
                        speedup=20.0,
                        max_run_seconds=10.0,
                        status_interval_sec=0.1,
                        wait_for_completion=True,
                        initial_state_already_bootstrapped=True,
                    )

                # Must finish right after incarnation 1 (~0.3-1.0s), NOT hang until max_run_seconds=10.0s!
                self.assertEqual(sub_cnt, 2)
                self.assertLess(end_ep - start_ep, 3.0)
                # Only `train-0-abc` was deleted; `train-2-worker-0` and `train-eval-0` were NOT deleted!
                self.assertEqual(
                    deleted_pod_paths,
                    ["/api/v1/namespaces/ai-lab/pods/train-0-abc?gracePeriodSeconds=0"],
                )
            finally:
                mgr.close()

    def test_pod_group_annotations_take_precedence_over_labels(self) -> None:
        """Verify pod-group-name & pod-group-total-count are read from annotations before labels (Kueue v0.18+)."""
        from runner.loader import ScheduledWorkflowDeletion, _get_pod_meta_annotation_or_label, load_scenario_bundle
        from runner.replay import _pod_belongs_to_workflow

        self.assertEqual(
            _get_pod_meta_annotation_or_label(
                {
                    "annotations": {"kueue.x-k8s.io/pod-group-name": "pg-ann"},
                    "labels": {"kueue.x-k8s.io/pod-group-name": "pg-lbl"},
                },
                "kueue.x-k8s.io/pod-group-name",
            ),
            "pg-ann",
        )
        self.assertEqual(
            _get_pod_meta_annotation_or_label(
                {"labels": {"kueue.x-k8s.io/pod-group-total-count": "4"}},
                "kueue.x-k8s.io/pod-group-total-count",
            ),
            "4",
        )

        # Pod belonging to PodGroup via annotation only must match _pod_belongs_to_workflow.
        del_ev = ScheduledWorkflowDeletion(
            target_wall_offset_sec=1.0,
            seq=1,
            raw_offset_sec=1.0,
            raw_offset_from_t0_sec=1.0,
            event_timestamp="2026-01-01T00:00:01Z",
            object_kind="PodGroup",
            manifest_api_version="v1",
            manifest_kind="Pod",
            name="pg-ann",
            applied_name="pg-ann",
            namespace="ai-lab",
            delete_reason="cancelled",
        )
        pod_ann = {"metadata": {"name": "worker-0", "annotations": {"kueue.x-k8s.io/pod-group-name": "pg-ann"}}}
        pod_other = {"metadata": {"name": "worker-1", "annotations": {"kueue.x-k8s.io/pod-group-name": "pg-other"}}}
        self.assertTrue(_pod_belongs_to_workflow(pod_ann, del_ev))
        self.assertFalse(_pod_belongs_to_workflow(pod_other, del_ev))

        # Scenario loader coalesces annotation-based PodGroup pods and reads pod-group-total-count annotation.
        with tempfile.TemporaryDirectory() as tmp:
            sc_dir = Path(tmp)
            infra_events = [
                {
                    "timestamp": "2026-01-01T00:00:00Z",
                    "eventType": "New",
                    "payload": {
                        "apiVersion": "v1",
                        "kind": "Node",
                        "metadata": {"name": "n0", "labels": {"type": "kwok"}},
                        "status": {"capacity": {"cpu": "16", "memory": "64Gi", "pods": "110"}, "allocatable": {"cpu": "16", "memory": "64Gi", "pods": "110"}},
                    },
                },
                {"timestamp": "2026-01-01T00:00:00Z", "eventType": "New", "payload": {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "ai-lab"}}},
                {"timestamp": "2026-01-01T00:00:00Z", "eventType": "New", "payload": {"apiVersion": "kueue.x-k8s.io/v1beta2", "kind": "ResourceFlavor", "metadata": {"name": "rf"}, "spec": {"nodeLabels": {"type": "kwok"}}}},
                {
                    "timestamp": "2026-01-01T00:00:00Z",
                    "eventType": "New",
                    "payload": {
                        "apiVersion": "kueue.x-k8s.io/v1beta2",
                        "kind": "ClusterQueue",
                        "metadata": {"name": "cq"},
                        "spec": {
                            "namespaceSelector": {},
                            "resourceGroups": [{"coveredResources": ["cpu", "memory"], "flavors": [{"name": "rf", "resources": [{"name": "cpu", "nominalQuota": "16"}, {"name": "memory", "nominalQuota": "64Gi"}]}]}],
                        },
                    },
                },
                {"timestamp": "2026-01-01T00:00:00Z", "eventType": "New", "payload": {"apiVersion": "kueue.x-k8s.io/v1beta2", "kind": "LocalQueue", "metadata": {"name": "lq", "namespace": "ai-lab"}, "spec": {"clusterQueue": "cq"}}},
            ]
            wf_events = [
                {
                    "timestamp": "2026-01-01T00:00:00Z",
                    "eventType": "New",
                    "workloadState": "Admitted",
                    "payload": {
                        "objectKind": "Pod",
                        "name": "pg-w-0",
                        "namespace": "ai-lab",
                        "creationTime": "2026-01-01T00:00:00Z",
                        "localQueueName": "lq",
                        "object": {
                            "apiVersion": "v1",
                            "kind": "Pod",
                            "metadata": {
                                "name": "pg-w-0",
                                "namespace": "ai-lab",
                                "labels": {"kueue.x-k8s.io/queue-name": "lq"},
                                "annotations": {
                                    "kueue.x-k8s.io/pod-group-name": "ann-group",
                                    "kueue.x-k8s.io/pod-group-total-count": "3",
                                },
                            },
                            "spec": {"restartPolicy": "Never", "containers": [{"name": "c", "image": "pause", "resources": {"requests": {"cpu": "2", "memory": "4Gi"}}}]},
                        },
                    },
                },
            ]
            exec_events = [
                {
                    "timestamp": "2026-01-01T00:00:00Z",
                    "eventType": "Admitted",
                    "workloadState": "Admitted",
                    "payload": {
                        "workloadName": "ann-group",
                        "namespace": "ai-lab",
                        "ownerKind": "PodGroup",
                        "ownerName": "ann-group",
                        "localQueueName": "lq",
                        "clusterQueueName": "cq",
                        "creationTime": "2026-01-01T00:00:00Z",
                        "admittedTime": "2026-01-01T00:00:00Z",
                    },
                },
            ]
            (sc_dir / "infra.json").write_text("\n".join(json.dumps(x) for x in infra_events) + "\n", encoding="utf-8")
            (sc_dir / "workflows.json").write_text("\n".join(json.dumps(x) for x in wf_events) + "\n", encoding="utf-8")
            (sc_dir / "execution-time.json").write_text("\n".join(json.dumps(x) for x in exec_events) + "\n", encoding="utf-8")

            bundle = load_scenario_bundle(sc_dir, speedup=1.0)
            self.assertEqual(len(bundle.initial_workloads), 1)
            self.assertEqual(len(bundle.initial_workloads[0].pods), 3)

    def test_resolve_podset_assignment_preserves_per_podset_flavors(self) -> None:
        """Multi-PodSet workloads must keep distinct flavors per PodSet instead of overwriting with the last flavor."""
        from runner.loader import _ParsedInfra, _resolve_podset_assignment_for_group

        cpu_node = {"metadata": {"name": "cpu-n0", "labels": {"pool": "cpu"}}}
        gpu_node = {"metadata": {"name": "gpu-n0", "labels": {"pool": "gpu"}}}
        cpu_flv = {"metadata": {"name": "cpu-flv"}, "spec": {"nodeLabels": {"pool": "cpu"}}}
        gpu_flv = {"metadata": {"name": "gpu-flv"}, "spec": {"nodeLabels": {"pool": "gpu"}}}
        cq_doc = {
            "metadata": {"name": "cq-mixed"},
            "spec": {
                "resourceGroups": [
                    {
                        "coveredResources": ["cpu", "memory", "nvidia.com/gpu"],
                        "flavors": [
                            {"name": "cpu-flv", "resources": [{"name": "cpu", "nominalQuota": "32"}, {"name": "memory", "nominalQuota": "64Gi"}, {"name": "nvidia.com/gpu", "nominalQuota": "0"}]},
                            {"name": "gpu-flv", "resources": [{"name": "cpu", "nominalQuota": "32"}, {"name": "memory", "nominalQuota": "64Gi"}, {"name": "nvidia.com/gpu", "nominalQuota": "8"}]},
                        ],
                    }
                ]
            },
        }
        parsed = _ParsedInfra(
            nodes_by_name={"cpu-n0": cpu_node, "gpu-n0": gpu_node},
            namespaces_by_name={},
            runtime_classes_by_name={},
            topologies_by_name={},
            flavors_by_name={"cpu-flv": cpu_flv, "gpu-flv": gpu_flv},
            cohorts_by_name={},
            priority_classes_by_name={},
            cqs_by_name={"cq-mixed": cq_doc},
            lqs_by_key={},
            pod_new_events={},
            pod_delete_epochs={},
            scheduled_infra_events=[],
            kueue_configuration=None,
        )
        scraped_psas = [
            {"name": "driver", "count": 1, "flavors": {"cpu": "cpu-flv", "memory": "cpu-flv"}},
            {"name": "worker", "count": 2, "flavors": {"cpu": "gpu-flv", "memory": "gpu-flv", "nvidia.com/gpu": "gpu-flv"}},
        ]
        driver_psa, _ = _resolve_podset_assignment_for_group(
            role_hash="drv00001",
            count=1,
            reqs={"cpu": "2", "memory": "4Gi"},
            placements=["cpu-n0"],
            cq_doc=cq_doc,
            parsed_infra=parsed,
            scraped_psas=scraped_psas,
            ps_hint="driver",
            group_index=0,
            total_groups=2,
        )
        worker_psa, _ = _resolve_podset_assignment_for_group(
            role_hash="wrk00002",
            count=2,
            reqs={"cpu": "4", "memory": "8Gi", "nvidia.com/gpu": "1"},
            placements=["gpu-n0"],
            cq_doc=cq_doc,
            parsed_infra=parsed,
            scraped_psas=scraped_psas,
            ps_hint="worker",
            group_index=1,
            total_groups=2,
        )
        self.assertEqual(driver_psa["flavors"], {"cpu": "cpu-flv", "memory": "cpu-flv"})
        self.assertEqual(worker_psa["flavors"], {"cpu": "gpu-flv", "memory": "gpu-flv", "nvidia.com/gpu": "gpu-flv"})

        # When no scraped assignment can be tied to a pod group (names differ, counts tie, group count differs),
        # keep the flavor that all scraped assignments agree on instead of picking the first flavor that fits the node.
        shared_node = {"metadata": {"name": "n0", "labels": {"pool": "a"}}}
        spot_flv = {"metadata": {"name": "spot"}, "spec": {"nodeLabels": {"pool": "a"}}}
        on_demand_flv = {"metadata": {"name": "on-demand"}, "spec": {"nodeLabels": {"pool": "a"}}}
        cq_two = {
            "metadata": {"name": "cq-two"},
            "spec": {
                "resourceGroups": [
                    {
                        "coveredResources": ["cpu", "memory"],
                        "flavors": [
                            {"name": "spot", "resources": [{"name": "cpu", "nominalQuota": "32"}, {"name": "memory", "nominalQuota": "64Gi"}]},
                            {"name": "on-demand", "resources": [{"name": "cpu", "nominalQuota": "32"}, {"name": "memory", "nominalQuota": "64Gi"}]},
                        ],
                    }
                ]
            },
        }
        parsed_two = _ParsedInfra(
            nodes_by_name={"n0": shared_node},
            namespaces_by_name={},
            runtime_classes_by_name={},
            topologies_by_name={},
            flavors_by_name={"spot": spot_flv, "on-demand": on_demand_flv},
            cohorts_by_name={},
            priority_classes_by_name={},
            cqs_by_name={"cq-two": cq_two},
            lqs_by_key={},
            pod_new_events={},
            pod_delete_epochs={},
        )
        agreeing_psas = [
            {"name": "driver", "count": 2, "flavors": {"cpu": "on-demand", "memory": "on-demand"}},
            {"name": "worker", "count": 2, "flavors": {"cpu": "on-demand", "memory": "on-demand"}},
        ]
        disagreeing_psas = [
            {"name": "driver", "count": 2, "flavors": {"cpu": "on-demand", "memory": "on-demand"}},
            {"name": "worker", "count": 2, "flavors": {"cpu": "spot", "memory": "on-demand"}},
        ]
        for g_idx in range(3):
            common = dict(
                role_hash=f"hash{g_idx}",
                count=2,
                reqs={"cpu": "2", "memory": "4Gi"},
                placements=["n0"],
                cq_doc=cq_two,
                parsed_infra=parsed_two,
                ps_hint=f"unmatched-{g_idx}",
                group_index=g_idx,
                total_groups=3,
            )
            agreed_psa, _ = _resolve_podset_assignment_for_group(scraped_psas=agreeing_psas, **common)
            self.assertEqual(agreed_psa["flavors"], {"cpu": "on-demand", "memory": "on-demand"})
            # cpu is disputed -> first flavor that fits the node ('spot'); memory is agreed -> 'on-demand'.
            disputed_psa, _ = _resolve_podset_assignment_for_group(scraped_psas=disagreeing_psas, **common)
            self.assertEqual(disputed_psa["flavors"], {"cpu": "spot", "memory": "on-demand"})

    def test_two_pass_initial_jump_rebuilds_t0_lifetimes_on_early_partial_execution(self) -> None:
        """An early t0 PartialExecution before the first NotAdmitted arrival must not truncate other t0 lifetimes."""
        from runner.loader import load_scenario_bundle

        with tempfile.TemporaryDirectory() as tmp:
            sc_dir = Path(tmp)
            infra_events = [
                {
                    "timestamp": "2026-01-01T00:00:00Z",
                    "eventType": "New",
                    "payload": {
                        "apiVersion": "v1",
                        "kind": "Node",
                        "metadata": {"name": "n0", "labels": {"type": "kwok"}},
                        "status": {"capacity": {"cpu": "32", "memory": "128Gi", "pods": "110"}, "allocatable": {"cpu": "32", "memory": "128Gi", "pods": "110"}},
                    },
                },
                {"timestamp": "2026-01-01T00:00:00Z", "eventType": "New", "payload": {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "ai-lab"}}},
                {"timestamp": "2026-01-01T00:00:00Z", "eventType": "New", "payload": {"apiVersion": "kueue.x-k8s.io/v1beta2", "kind": "ResourceFlavor", "metadata": {"name": "rf"}, "spec": {"nodeLabels": {"type": "kwok"}}}},
                {
                    "timestamp": "2026-01-01T00:00:00Z",
                    "eventType": "New",
                    "payload": {
                        "apiVersion": "kueue.x-k8s.io/v1beta2",
                        "kind": "ClusterQueue",
                        "metadata": {"name": "cq"},
                        "spec": {
                            "namespaceSelector": {},
                            "resourceGroups": [{"coveredResources": ["cpu", "memory"], "flavors": [{"name": "rf", "resources": [{"name": "cpu", "nominalQuota": "32"}, {"name": "memory", "nominalQuota": "128Gi"}]}]}],
                        },
                    },
                },
                {"timestamp": "2026-01-01T00:00:00Z", "eventType": "New", "payload": {"apiVersion": "kueue.x-k8s.io/v1beta2", "kind": "LocalQueue", "metadata": {"name": "lq", "namespace": "ai-lab"}, "spec": {"clusterQueue": "cq"}}},
                # Steady t0 pod running until +50s
                {"timestamp": "2026-01-01T00:00:00Z", "eventType": "New", "payload": {"apiVersion": "v1", "kind": "Pod", "name": "steady-0", "namespace": "ai-lab", "placement": "n0", "schedulingTime": "2026-01-01T00:00:00Z", "firstNoticeTime": "2026-01-01T00:00:00Z", "resourceRequest": {"cpu": "2", "memory": "4Gi"}, "workloadName": "job-steady-1", "ownerKind": "Job", "ownerName": "steady"}},
                {"timestamp": "2026-01-01T00:00:50Z", "eventType": "Deleted", "payload": {"apiVersion": "v1", "kind": "Pod", "name": "steady-0", "namespace": "ai-lab", "deletionTime": "2026-01-01T00:00:50Z"}},
                # Non-Kueue infra pod ending at +25s (must NOT be dropped when jump converges to +10s!)
                {"timestamp": "2026-01-01T00:00:00Z", "eventType": "New", "payload": {"apiVersion": "v1", "kind": "Pod", "name": "sys-agent-0", "namespace": "kube-system", "placement": "n0", "schedulingTime": "2026-01-01T00:00:00Z", "firstNoticeTime": "2026-01-01T00:00:00Z", "resourceRequest": {"cpu": "1", "memory": "1Gi"}}},
                {"timestamp": "2026-01-01T00:00:25Z", "eventType": "Deleted", "payload": {"apiVersion": "v1", "kind": "Pod", "name": "sys-agent-0", "namespace": "kube-system", "deletionTime": "2026-01-01T00:00:25Z"}},
                # Non-Kueue pod that appears after every candidate start (+45s) and must be scheduled exactly once.
                {"timestamp": "2026-01-01T00:00:45Z", "eventType": "New", "payload": {"apiVersion": "v1", "kind": "Pod", "name": "late-sys-0", "namespace": "kube-system", "placement": "n0", "schedulingTime": "2026-01-01T00:00:45Z", "firstNoticeTime": "2026-01-01T00:00:45Z", "resourceRequest": {"cpu": "1", "memory": "1Gi"}}},
                {"timestamp": "2026-01-01T00:00:48Z", "eventType": "Deleted", "payload": {"apiVersion": "v1", "kind": "Pod", "name": "late-sys-0", "namespace": "kube-system", "deletionTime": "2026-01-01T00:00:48Z"}},
            ]

            def _make_job(name: str, ctime: str) -> dict:
                return {
                    "apiVersion": "batch/v1",
                    "kind": "Job",
                    "metadata": {"name": name, "namespace": "ai-lab", "creationTimestamp": ctime, "labels": {"kueue.x-k8s.io/queue-name": "lq"}},
                    "spec": {"parallelism": 1, "completions": 1, "template": {"spec": {"restartPolicy": "Never", "containers": [{"name": "c", "image": "pause", "resources": {"requests": {"cpu": "2", "memory": "4Gi"}}}]}}},
                }

            wf_events = [
                # t0 job interrupted at +10s
                {"timestamp": "2026-01-01T00:00:00Z", "eventType": "New", "workloadState": "Admitted", "payload": {"objectKind": "Job", "name": "preempted-t0", "namespace": "ai-lab", "creationTime": "2026-01-01T00:00:00Z", "localQueueName": "lq", "object": _make_job("preempted-t0", "2026-01-01T00:00:00Z")}},
                {"timestamp": "2026-01-01T00:00:10Z", "eventType": "Update", "workloadState": "NotAdmitted", "payload": {"objectKind": "Job", "name": "preempted-t0", "namespace": "ai-lab", "creationTime": "2026-01-01T00:00:00Z", "localQueueName": "lq", "object": _make_job("preempted-t0", "2026-01-01T00:00:00Z")}},
                # t0 steady job running until +50s
                {"timestamp": "2026-01-01T00:00:00Z", "eventType": "New", "workloadState": "Admitted", "payload": {"objectKind": "Job", "name": "steady", "namespace": "ai-lab", "creationTime": "2026-01-01T00:00:00Z", "localQueueName": "lq", "object": _make_job("steady", "2026-01-01T00:00:00Z")}},
                # First new NotAdmitted arrival at +40s
                {"timestamp": "2026-01-01T00:00:40Z", "eventType": "New", "workloadState": "NotAdmitted", "payload": {"objectKind": "Job", "name": "new-arrival", "namespace": "ai-lab", "creationTime": "2026-01-01T00:00:40Z", "localQueueName": "lq", "object": _make_job("new-arrival", "2026-01-01T00:00:40Z")}},
            ]
            exec_events = [
                {"timestamp": "2026-01-01T00:00:00Z", "eventType": "Admitted", "workloadState": "Admitted", "payload": {"workloadName": "job-preempted-1", "namespace": "ai-lab", "ownerKind": "Job", "ownerName": "preempted-t0", "localQueueName": "lq", "clusterQueueName": "cq", "creationTime": "2026-01-01T00:00:00Z", "admittedTime": "2026-01-01T00:00:00Z"}},
                {"timestamp": "2026-01-01T00:00:10Z", "eventType": "PartialExecution", "workloadState": "NotAdmitted", "payload": {"executionType": "Partial", "workloadName": "job-preempted-1", "namespace": "ai-lab", "ownerKind": "Job", "ownerName": "preempted-t0", "localQueueName": "lq", "clusterQueueName": "cq", "creationTime": "2026-01-01T00:00:00Z", "admittedTime": "2026-01-01T00:00:00Z", "endTime": "2026-01-01T00:00:10Z", "executionDurationSeconds": 10.0, "preempted": True}},
                {"timestamp": "2026-01-01T00:00:00Z", "eventType": "Admitted", "workloadState": "Admitted", "payload": {"workloadName": "job-steady-1", "namespace": "ai-lab", "ownerKind": "Job", "ownerName": "steady", "localQueueName": "lq", "clusterQueueName": "cq", "creationTime": "2026-01-01T00:00:00Z", "admittedTime": "2026-01-01T00:00:00Z"}},
                {"timestamp": "2026-01-01T00:00:50Z", "eventType": "FullExecution", "workloadState": "Admitted", "payload": {"executionType": "Full", "workloadName": "job-steady-1", "namespace": "ai-lab", "ownerKind": "Job", "ownerName": "steady", "localQueueName": "lq", "clusterQueueName": "cq", "creationTime": "2026-01-01T00:00:00Z", "admittedTime": "2026-01-01T00:00:00Z", "endTime": "2026-01-01T00:00:50Z", "executionDurationSeconds": 50.0}},
                {"timestamp": "2026-01-01T00:00:50Z", "eventType": "FullExecution", "workloadState": "Admitted", "payload": {"executionType": "Full", "workloadName": "job-new-1", "namespace": "ai-lab", "ownerKind": "Job", "ownerName": "new-arrival", "localQueueName": "lq", "clusterQueueName": "cq", "creationTime": "2026-01-01T00:00:40Z", "admittedTime": "2026-01-01T00:00:40Z", "endTime": "2026-01-01T00:00:50Z", "executionDurationSeconds": 10.0}},
            ]
            (sc_dir / "infra.json").write_text("\n".join(json.dumps(x) for x in infra_events) + "\n", encoding="utf-8")
            (sc_dir / "workflows.json").write_text("\n".join(json.dumps(x) for x in wf_events) + "\n", encoding="utf-8")
            (sc_dir / "execution-time.json").write_text("\n".join(json.dumps(x) for x in exec_events) + "\n", encoding="utf-8")

            bundle = load_scenario_bundle(sc_dir, speedup=1.0, skip_initial_offset=True)
            self.assertAlmostEqual(bundle.initial_jump_raw_sec, 10.0, places=3)
            # steady-0 runs until +50s, so from effective_start (+10s) remaining must be 40s (NOT 10s!).
            self.assertEqual(len(bundle.initial_workloads), 1)
            self.assertAlmostEqual(bundle.initial_workloads[0].pods[0].remaining_duration_sec, 40.0, places=2)
            # sys-agent-0 ends at +25s (> +10s), so it must NOT be dropped and has 15s remaining!
            self.assertEqual(len(bundle.initial_pods), 1)
            self.assertAlmostEqual(bundle.initial_pods[0].remaining_duration_sec, 15.0, places=2)
            # The jump shift builds the initial state twice. The post-t0 non-Kueue pod must still be scheduled once.
            pod_events = [
                (ie.event_type, ie.name) for ie in bundle.scheduled_infra_events if ie.object_kind == "Pod"
            ]
            self.assertEqual(pod_events, [("New", "late-sys-0")])

    def test_wfpr_requeue_timestamp_default_and_backoff_scaling(self) -> None:
        """CLI requeue-timestamp must default to None (preserving scenario Creation) and default backoffs must scale."""
        from chamber.manifests import patch_kueue_wait_for_pods_ready_config, resolve_wait_for_pods_ready_config

        args = testrunner.build_arg_parser().parse_args(["testscenarios/demo"])
        self.assertIsNone(args.wait_for_pods_ready_requeue_timestamp)

        cfg = resolve_wait_for_pods_ready_config(
            enabled=True,
            raw_timeout="10m",
            requeuing_timestamp="Creation",
            speedup=60.0,
        )
        self.assertEqual(cfg.requeuing_timestamp, "Creation")
        self.assertEqual(cfg.effective_timeout_sec, 10.0)
        self.assertEqual(cfg.effective_backoff_base_sec, 1)
        self.assertEqual(cfg.effective_backoff_max_sec, 60)

        with tempfile.TemporaryDirectory() as tmp:
            cfg_dir = Path(tmp) / "config"
            cfg_dir.mkdir(parents=True)
            cfg_file = cfg_dir / "controller_manager_config.yaml"
            cfg_file.write_text("apiVersion: config.kueue.x-k8s.io/v1beta2\nkind: Configuration\n", encoding="utf-8")
            patch_kueue_wait_for_pods_ready_config(Path(tmp), cfg)
            rendered = cfg_file.read_text(encoding="utf-8")
            self.assertIn("timestamp: Creation", rendered)
            self.assertIn("backoffBaseSeconds: 1", rendered)
            self.assertIn("backoffMaxSeconds: 60", rendered)

    def test_scrape_output_dir_terminates_scraper_on_t0_barrier_failure(self) -> None:
        """If _wait_for_scraper_t0_barrier fails, cluster-scraper subprocess must be terminated cleanly."""
        import contextlib
        import signal

        demo_dir = REPO_ROOT / "testscenarios" / "demo"
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp) / "scraped"
            fake_bin = Path(tmp) / "cluster-scraper"
            fake_bin.write_text("#!/bin/sh\n", encoding="utf-8")

            fake_proc = mock.MagicMock()
            fake_proc.poll.return_value = None
            fake_proc.wait.return_value = 0

            fake_mgr = mock.MagicMock()
            fake_mgr.kubeconfig_path = Path(tmp) / "kwok.kubeconfig"

            @contextlib.contextmanager
            def _fake_managed_chamber(*_a, **_kw):
                yield fake_mgr

            with (
                mock.patch.object(testrunner, "ChamberManager", return_value=fake_mgr),
                mock.patch.object(testrunner, "managed_chamber", side_effect=_fake_managed_chamber),
                mock.patch.object(testrunner, "apply_scenario_queues_and_wait"),
                mock.patch("subprocess.Popen", return_value=fake_proc),
                mock.patch.object(
                    testrunner,
                    "_wait_for_scraper_t0_barrier",
                    side_effect=RuntimeError("t0 barrier timed out"),
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "t0 barrier timed out"):
                    testrunner.main(
                        [
                            str(demo_dir),
                            "--scrape-output-dir",
                            str(out_dir),
                            "--scraper-bin",
                            str(fake_bin),
                            "--no-metrics-report",
                        ]
                    )

            fake_proc.send_signal.assert_called_once_with(signal.SIGTERM)
            fake_proc.wait.assert_called_once()

    def test_fifo_creation_epoch_tier_spacing_stays_responsive_and_ordered(self) -> None:
        """Later creation-time tiers wait in event_heap: infra events run between tiers and creation order is kept."""
        import dataclasses

        from chamber_common import ExecutionTimeIndex
        from runner.loader import ScenarioBundle, ScheduledInfraEvent, ScheduledWorkflowItem
        from runner.replay import run_scenario_replay

        event_log: list[str] = []

        def _mk_wf(idx: int) -> ScheduledWorkflowItem:
            return ScheduledWorkflowItem(
                target_wall_offset_sec=0.0,
                seq=idx,
                raw_offset_sec=0.0,
                raw_offset_from_t0_sec=0.0,
                event_timestamp="2026-01-01T00:00:00Z",
                object_kind="Job",
                name=f"tier-job-{idx}",
                namespace="ai-lab",
                local_queue_name="lq",
                workload_state="NotAdmitted",
                trigger_reason="initial_not_admitted",
                execution_duration_sec=1.0,
                compressed_duration_sec=1.0,
                indexed_from_execution_time=True,
                pod_count=1,
                manifest={"apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": f"tier-job-{idx}", "namespace": "ai-lab"}},
                creation_epoch=1700000000.0 + idx * 5.0,
            )

        bundle = ScenarioBundle(
            scenario_dir=Path("."),
            infra_path=Path("infra.json"),
            workflows_path=Path("workflows.json"),
            execution_time_path=Path("execution-time.json"),
            exec_index=ExecutionTimeIndex(),
            t0_epoch=1767225600.0,
            t0_iso="2026-01-01T00:00:00Z",
            effective_start_epoch=1767225600.0,
            effective_start_iso="2026-01-01T00:00:00Z",
            initial_jump_raw_sec=0.0,
            initial_jump_wall_sec=0.0,
            expired_initial_pods_count=0,
            t_end_epoch=1767225610.0,
            t_end_iso="2026-01-01T00:00:10Z",
            infra_event_count=0,
            workflow_event_count=3,
            nodes=[],
            namespaces=[],
            prereq_and_queue_docs=[],
            initial_pods=[],
            scheduled_workflows=[_mk_wf(0), _mk_wf(1), _mk_wf(2)],
            skipped_already_admitted_workflows=[],
            skipped_child_workflows=0,
            workload_namespaces=["ai-lab"],
            primary_workload_namespace="ai-lab",
            infra_pods_namespace="infra-pods",
            cluster_queue_names=["cq-mid"],
            queueing_strategy="StrictFIFO",
            initial_workloads=[],
            scheduled_infra_events=[
                ScheduledInfraEvent(
                    target_wall_offset_sec=0.3,
                    seq=10,
                    raw_offset_sec=0.3,
                    raw_offset_from_t0_sec=0.3,
                    event_timestamp="2026-01-01T00:00:00.3Z",
                    event_type="Update",
                    object_kind="ClusterQueue",
                    name="cq-mid",
                    namespace="",
                    manifest={"apiVersion": "kueue.x-k8s.io/v1beta2", "kind": "ClusterQueue", "metadata": {"name": "cq-mid"}},
                )
            ],
            scheduled_workflow_deletions=[],
        )

        mgr = ChamberManager(chamber_namespace="kueue-optimizer-chamber-ut-tiers", apiserver_port=16497)
        try:
            def fake_apply(docs, max_workers=16):
                for d in docs:
                    event_log.append(f"wf:{d['metadata']['name']}")

            def fake_ssa(doc):
                event_log.append(f"infra:{doc['metadata']['name']}")

            fake_now = [1000.0]

            def fake_mono() -> float:
                return fake_now[0]

            def fake_wall_time() -> float:
                return fake_now[0]

            def fake_sleep(sec: float) -> None:
                fake_now[0] += max(0.0, float(sec))

            def fake_query_status(*_a, **_kw):
                return ClusterStatusSnapshot(
                    workloads_admitted=0,
                    workloads_pending=0,
                    workloads_finished=0,
                    pods_running=0,
                    pods_pending=0,
                    pods_succeeded=0,
                    pods_failed=0,
                    jobsets_completed=0,
                    jobsets_failed=0,
                    cq_summary=[],
                )

            with (
                mock.patch.object(mgr, "apply_manifests_parallel", side_effect=fake_apply),
                mock.patch.object(mgr, "ssa_apply_doc", side_effect=fake_ssa),
                mock.patch("runner.replay.query_cluster_status", side_effect=fake_query_status),
                mock.patch("runner.replay.time.monotonic", side_effect=fake_mono),
                mock.patch("runner.replay.time.time", side_effect=fake_wall_time),
                mock.patch("runner.replay.time.sleep", side_effect=fake_sleep),
            ):
                run_scenario_replay(
                    mgr=mgr,
                    bundle=bundle,
                    speedup=1.0,
                    max_run_seconds=10.0,
                    status_interval_sec=5.0,
                    wait_for_completion=False,
                    initial_state_already_bootstrapped=True,
                )

            # Tier 0 fires at t=0, infra event fires at t=0.3 BEFORE tier 1 (t>=1.02) and tier 2 (t>=2.04)!
            self.assertEqual(event_log, ["wf:tier-job-0", "infra:cq-mid", "wf:tier-job-1", "wf:tier-job-2"])

            # Phase 2: a status poll that takes 1.0s and a workflow that becomes due at +0.5s while tiers
            # are still waiting. Every tier must be created in a later whole second than the previous one
            # (creationTimestamp has 1s resolution), and the late arrival must not pass the waiting tiers.
            late_wf = dataclasses.replace(
                _mk_wf(3), target_wall_offset_sec=0.5, raw_offset_sec=0.5, raw_offset_from_t0_sec=0.5
            )
            bundle_slow = dataclasses.replace(
                bundle,
                scheduled_workflows=[_mk_wf(0), _mk_wf(1), _mk_wf(2), late_wf],
                scheduled_infra_events=[],
            )
            fake_now[0] = 1000.0
            creates: list[tuple[str, int]] = []
            poll_calls = [0]

            def fake_apply_timed(docs, max_workers=16):
                for d in docs:
                    creates.append((d["metadata"]["name"], int(fake_now[0])))

            def fake_slow_status(*_a, **_kw):
                poll_calls[0] += 1
                if poll_calls[0] == 2:
                    fake_now[0] += 1.0
                return fake_query_status()

            with (
                mock.patch.object(mgr, "apply_manifests_parallel", side_effect=fake_apply_timed),
                mock.patch("runner.replay.query_cluster_status", side_effect=fake_slow_status),
                mock.patch("runner.replay.time.monotonic", side_effect=fake_mono),
                mock.patch("runner.replay.time.time", side_effect=fake_wall_time),
                mock.patch("runner.replay.time.sleep", side_effect=fake_sleep),
            ):
                run_scenario_replay(
                    mgr=mgr,
                    bundle=bundle_slow,
                    speedup=1.0,
                    max_run_seconds=30.0,
                    status_interval_sec=1.0,
                    wait_for_completion=False,
                    initial_state_already_bootstrapped=True,
                )

            self.assertGreaterEqual(poll_calls[0], 2)
            self.assertEqual([c[0] for c in creates], ["tier-job-0", "tier-job-1", "tier-job-2", "tier-job-3"])
            create_seconds = [c[1] for c in creates]
            self.assertEqual(create_seconds, sorted(set(create_seconds)), f"tiers share a wall second: {creates}")
        finally:
            mgr.close()


class DeploymentScrapingAndReplayTests(unittest.TestCase):
    """Unit tests covering Deployment manifest sanitization, st81-st90 bundle loading, updates, and saturation."""

    def test_deployment_sanitization_restart_policy_scale_zero_and_long_running_annotations(self) -> None:
        from runner.validation import _inject_pod_execution_time_and_sanitize_workflow

        raw_deploy = {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "srv-zero", "namespace": "ai-lab"},
            "spec": {
                "replicas": 0,
                "paused": False,
                "selector": {"matchLabels": {"app": "srv-zero"}},
                "template": {
                    "metadata": {
                        "annotations": {
                            "kueue-optimizer.gke.io/template-hash": "th-0000000000000001",
                            "kueue-optimizer.gke.io/pod-execution-time-seconds": "10",
                        }
                    },
                    "spec": {
                        "containers": [{"name": "srv", "resources": {"requests": {"cpu": "2"}}}],
                    },
                },
            },
        }
        doc, pod_cnt = _inject_pod_execution_time_and_sanitize_workflow(
            raw_obj=raw_deploy,
            exec_str="10",
            raw_duration_sec=10.0,
            compressed_duration_sec=10.0,
        )
        self.assertEqual(pod_cnt, 0)
        self.assertEqual(doc["spec"]["replicas"], 0)
        self.assertIs(doc["spec"]["paused"], False)
        tpl = doc["spec"]["template"]
        self.assertEqual(tpl["spec"]["restartPolicy"], "Always")
        self.assertEqual(tpl["metadata"]["annotations"]["kueue-optimizer.gke.io/long-running-pod"], "true")
        self.assertEqual(tpl["metadata"]["annotations"]["kueue-optimizer.gke.io/infinite-duration"], "true")
        self.assertEqual(
            tpl["metadata"]["annotations"]["kueue-optimizer.gke.io/template-hash"],
            "th-0000000000000001",
        )
        self.assertNotIn("kueue-optimizer.gke.io/pod-execution-time-seconds", tpl["metadata"]["annotations"])

    def test_st81_to_st90_deployment_scenarios_load_and_validate_cleanly(self) -> None:
        suite_dir = REPO_ROOT / "testscenarios" / "self-tests"

        b81 = testrunner.load_scenario_bundle(
            suite_dir / "st81-deployment-scale-up-headroom",
            missing_duration_policy="error",
        )
        self.assertEqual(len(b81.scheduled_workflows), 1)
        self.assertEqual(len(b81.scheduled_workflow_updates), 1)
        self.assertEqual(b81.scheduled_workflow_updates[0].replicas, 6)
        self.assertIn("ScaleUp", b81.scheduled_workflow_updates[0].update_types)

        b83 = testrunner.load_scenario_bundle(
            suite_dir / "st83-deployment-scale-down-unblocks-batch-and-zero",
            missing_duration_policy="error",
        )
        self.assertEqual(len(b83.scheduled_workflows), 3)
        self.assertEqual([u.replicas for u in b83.scheduled_workflow_updates], [4, 0])
        self.assertTrue(all("ScaleDown" in u.update_types for u in b83.scheduled_workflow_updates))

        b85 = testrunner.load_scenario_bundle(
            suite_dir / "st85-deployment-rolling-upgrade-max-unavailable",
            missing_duration_policy="error",
        )
        self.assertEqual(len(b85.scheduled_workflow_updates), 1)
        self.assertEqual(b85.scheduled_workflow_updates[0].update_types, ("Upgrade",))
        self.assertEqual(b85.scheduled_workflow_updates[0].template_hash, "th-0000000000000002")

        b87 = testrunner.load_scenario_bundle(
            suite_dir / "st87-deployment-pause-defers-rollout-and-resume",
            missing_duration_policy="error",
        )
        self.assertEqual(len(b87.scheduled_workflow_updates), 3)
        self.assertEqual([u.paused for u in b87.scheduled_workflow_updates], [True, True, False])
        self.assertEqual(
            [u.update_types for u in b87.scheduled_workflow_updates],
            [("Paused",), ("Upgrade",), ("Resumed",)],
        )

        b89 = testrunner.load_scenario_bundle(
            suite_dir / "st89-deployment-t0-partial-status-and-preemption",
            skip_initial_offset=False,
            missing_duration_policy="error",
        )
        self.assertEqual(len(b89.initial_deployments), 3)
        self.assertEqual(len(b89.initial_pods), 0)
        by_name_89 = {d.name: (d.admitted_replicas, d.desired_replicas) for d in b89.initial_deployments}
        self.assertEqual(
            by_name_89,
            {"steady-srv": (2, 2), "bg-embed": (2, 2), "partial-api": (4, 6)},
        )
        self.assertEqual(b89.initial_workload_pods_count, 8)
        self.assertEqual(len(b89.scheduled_workflows), 1)
        self.assertEqual(b89.scheduled_workflows[0].name, "urgent-eval")

        b90 = testrunner.load_scenario_bundle(
            suite_dir / "st90-deployment-t0-delete-and-recreate-lifecycle",
            skip_initial_offset=False,
            missing_duration_policy="error",
        )
        self.assertEqual(len(b90.initial_deployments), 1)
        self.assertEqual(b90.initial_deployments[0].name, "ephemeral-srv")
        self.assertEqual(b90.initial_deployments[0].admitted_replicas, 4)
        self.assertEqual(len(b90.scheduled_workflow_deletions), 2)
        self.assertEqual(
            [(d.name, d.incarnation_index) for d in b90.scheduled_workflow_deletions],
            [("ephemeral-srv", 0), ("canary-srv", 0)],
        )
        self.assertEqual(
            [(w.name, w.incarnation_index, w.pod_count) for w in b90.scheduled_workflows],
            [("ephemeral-srv", 1, 6), ("canary-srv", 0, 2)],
        )

    def test_deployment_replay_scale_to_zero_and_retired_vs_preempted_saturation(self) -> None:
        from datetime import datetime, timezone
        import time

        from chamber.lifecycle import ChamberManager, ClusterStatusSnapshot
        from metrics.saturation import report_effective_uninterrupted_saturation
        from runner.replay import run_scenario_replay

        suite_dir = REPO_ROOT / "testscenarios" / "self-tests"
        b83 = testrunner.load_scenario_bundle(
            suite_dir / "st83-deployment-scale-down-unblocks-batch-and-zero",
            speedup=40.0,
        )
        mgr = ChamberManager(chamber_namespace="kueue-optimizer-chamber-ut83", apiserver_port=16499)
        try:
            applied_updates: list[int] = []
            live_wls: list[dict] = []

            def _mk_srv_wl(idx: int, admit_iso: str) -> dict:
                return {
                    "metadata": {
                        "name": f"pod-srv-pool-rs-{idx}",
                        "namespace": "st-workloads",
                        "ownerReferences": [{"kind": "Pod", "name": f"srv-pool-rs-{idx}"}],
                    },
                    "spec": {
                        "podSets": [
                            {
                                "name": "main",
                                "count": 1,
                                "template": {
                                    "metadata": {},
                                    "spec": {
                                        "containers": [
                                            {
                                                "name": "srv",
                                                "resources": {
                                                    "requests": {
                                                        "cpu": "2",
                                                        "memory": "8Gi",
                                                        "google.com/tpu": "2",
                                                    }
                                                },
                                            }
                                        ]
                                    },
                                },
                            }
                        ]
                    },
                    "status": {
                        "admission": {"clusterQueue": "cq-dep-scaledown"},
                        "conditions": [
                            {"type": "Admitted", "status": "True", "lastTransitionTime": admit_iso}
                        ],
                    },
                }

            t_start_iso = datetime.fromtimestamp(time.time() - 5.0, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

            def fake_apply_parallel(docs, **_kw):
                for d in docs:
                    if d.get("kind") == "Deployment":
                        live_wls[:] = [_mk_srv_wl(i, t_start_iso) for i in range(8)]

            def fake_ssa(doc):
                if doc.get("kind") == "Deployment":
                    rep = int(doc["spec"]["replicas"])
                    applied_updates.append(rep)
                    now_ret_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                    kept = live_wls[:rep]
                    # Simulate half of retired Deployment pod Workloads lingering in etcd with OwnerNotFound before GC.
                    lingering: list[dict] = []
                    for idx_r, old_w in enumerate(live_wls[rep:]):
                        if idx_r % 2 == 0:
                            w_cp = json.loads(json.dumps(old_w))
                            w_cp["status"]["conditions"].append(
                                {
                                    "type": "Finished",
                                    "status": "True",
                                    "reason": "OwnerNotFound",
                                    "message": "The workload's owner no longer exists",
                                    "lastTransitionTime": now_ret_iso,
                                }
                            )
                            lingering.append(w_cp)
                    live_wls[:] = kept + lingering

            def fake_api_req(method, path, **_kw):
                if method == "GET" and path.endswith("/workloads"):
                    return 200, json.dumps({"items": list(live_wls)}).encode("utf-8")
                if method == "GET" and path == "/api/v1/nodes":
                    return 200, json.dumps({"items": b83.nodes}).encode("utf-8")
                if method == "GET" and path == "/api/v1/pods":
                    return 200, json.dumps({"items": []}).encode("utf-8")
                return 200, b"{}"

            def fake_q_status(*_a, **_kw):
                fin = {("st-workloads", "job-batch-0"), ("st-workloads", "job-batch-1")} if applied_updates else set()
                return ClusterStatusSnapshot(
                    workloads_admitted=len(live_wls),
                    workloads_pending=0,
                    workloads_finished=len(fin),
                    pods_running=len(live_wls),
                    pods_pending=0,
                    pods_succeeded=len(fin),
                    pods_failed=0,
                    jobsets_completed=0,
                    jobsets_failed=0,
                    cq_summary=[],
                    finished_workload_keys=fin,
                    present_workload_keys=fin | {("st-workloads", w["metadata"]["name"]) for w in live_wls},
                    workloads_query_ok=True,
                )

            with (
                mock.patch.object(mgr, "ensure_kwok_connection"),
                mock.patch.object(mgr, "apply_manifests_parallel", side_effect=fake_apply_parallel),
                mock.patch.object(mgr, "ssa_apply_doc", side_effect=fake_ssa),
                mock.patch.object(mgr, "api_request", side_effect=fake_api_req),
                mock.patch("runner.replay.query_cluster_status", side_effect=fake_q_status),
            ):
                s_ep, e_ep, sub_cnt, _ = run_scenario_replay(
                    mgr=mgr,
                    bundle=b83,
                    speedup=40.0,
                    max_run_seconds=10.0,
                    status_interval_sec=0.1,
                    wait_for_completion=True,
                    initial_state_already_bootstrapped=True,
                )
                sat_snap = mgr.capture_cluster_saturation_snapshot(eval_epoch=e_ep)

            self.assertEqual(sub_cnt, 3)
            self.assertEqual(applied_updates, [4, 0])
            self.assertLess(e_ep - s_ep, 4.0)
            self.assertIsNotNone(sat_snap)
            self.assertEqual(len(sat_snap["workloads"]), 8)

            sat_rows, sat_subtitle = report_effective_uninterrupted_saturation(
                sat_snap,
                start_epoch=s_ep - 5.0,
                end_epoch=e_ep + 1.0,
            )
            self.assertTrue(len(sat_rows) >= 3)
            self.assertIn("Qualifying Workloads: 8 (completed=8", sat_subtitle)
            self.assertIn("Excluded (preempted/reclaimed/failed): 0", sat_subtitle)
        finally:
            mgr.close()

    def test_native_sidecar_and_initial_deployment_multi_container_quota_requests(self) -> None:
        from chamber.scenario import extract_pod_spec_resources
        from runner.loader import InitialDeploymentBootstrap
        from runner.replay import _build_single_pod_deployment_workload_status

        # 1. Native sidecar (`restartPolicy: Always`) before vs. after sequential initContainer + regular containers + overhead + pod-level resources.
        reqs_after = extract_pod_spec_resources(
            {
                "initContainers": [
                    {"name": "init", "resources": {"requests": {"cpu": "4", "memory": "1Gi"}}},
                    {
                        "name": "sidecar",
                        "restartPolicy": "Always",
                        "resources": {"requests": {"cpu": "1", "memory": "2Gi"}},
                    },
                ],
                "containers": [{"name": "main", "resources": {"requests": {"cpu": "1", "memory": "1Gi"}}}],
            }
        )
        self.assertEqual(reqs_after, {"cpu": 4.0, "memory": 3.0})

        reqs_before = extract_pod_spec_resources(
            {
                "initContainers": [
                    {"name": "sidecar", "restartPolicy": "Always", "resources": {"requests": {"cpu": "1"}}},
                    {"name": "init", "resources": {"requests": {"cpu": "4"}}},
                ],
                "containers": [
                    {"name": "proxy", "resources": {"requests": {"memory": "1Gi"}, "limits": {"cpu": "2"}}},
                    {"name": "worker", "resources": {"requests": {"cpu": "1", "google.com/tpu": "4"}}},
                ],
                "overhead": {"cpu": "250m", "memory": "0"},
            }
        )
        self.assertAlmostEqual(reqs_before["cpu"], 5.25)
        self.assertAlmostEqual(reqs_before["memory"], 1.0)
        self.assertAlmostEqual(reqs_before["google.com/tpu"], 4.0)

        reqs_pod_lvl = extract_pod_spec_resources(
            {
                "containers": [{"name": "main", "resources": {"requests": {"cpu": "1", "memory": "1Gi"}}}],
                "resources": {"requests": {"cpu": "6"}},
            }
        )
        self.assertEqual(reqs_pod_lvl, {"cpu": 6.0, "memory": 1.0})

        # 2. _build_single_pod_deployment_workload_status must charge full idep.per_pod_requests, NOT containers[0] alone!
        cq_doc = {
            "spec": {
                "resourceGroups": [
                    {
                        "coveredResources": ["cpu", "memory", "google.com/tpu"],
                        "flavors": [{"name": "tpu-flv"}],
                    }
                ]
            }
        }
        idep = InitialDeploymentBootstrap(
            namespace="ai-lab",
            name="multi-srv",
            applied_name="multi-srv",
            local_queue_name="lq",
            cluster_queue_name="cq",
            desired_replicas=1,
            admitted_replicas=1,
            paused=False,
            original_age_sec=5.0,
            placements=["node-0"],
            per_pod_requests={"cpu": "5250m", "memory": "3Gi", "google.com/tpu": "4"},
            priority_class_name="p-std",
            priority=100,
            is_tas=False,
            manifest={},
            cq_doc=cq_doc,
            flavors_by_name={"tpu-flv": {"spec": {}}},
            topologies_by_name={},
            nodes_by_name={"node-0": {"metadata": {"labels": {}}}},
            scraped_pod_set_assignments=[],
        )
        wl_doc = {
            "spec": {
                "podSets": [
                    {
                        "name": "main",
                        "count": 1,
                        "template": {
                            "spec": {
                                "containers": [
                                    # Sidecar at containers[0] has NO google.com/tpu and only 1 CPU!
                                    {"name": "sidecar", "resources": {"requests": {"cpu": "1", "memory": "512Mi"}}},
                                    {"name": "tpu-worker", "resources": {"requests": {"cpu": "3", "google.com/tpu": "4"}}},
                                ]
                            }
                        },
                    }
                ]
            }
        }
        st = _build_single_pod_deployment_workload_status(idep, wl_doc, "node-0", "2026-06-01T00:00:00Z")
        psa_usage = st["admission"]["podSetAssignments"][0]["resourceUsage"]
        self.assertEqual(psa_usage["google.com/tpu"], "4")
        self.assertEqual(psa_usage["cpu"], "5250m")
        self.assertEqual(psa_usage["memory"], "3Gi")

    def test_pending_no_reservation_and_retirement_budget_edge_cases(self) -> None:
        from dataclasses import replace as dc_replace
        from datetime import datetime, timezone
        import time

        from chamber.lifecycle import ChamberManager, ClusterStatusSnapshot
        from metrics.saturation import report_effective_uninterrupted_saturation
        from runner.loader import ScheduledWorkflowItem, ScheduledWorkflowUpdate
        from runner.replay import run_scenario_replay

        suite_dir = REPO_ROOT / "testscenarios" / "self-tests"
        base_b83 = testrunner.load_scenario_bundle(
            suite_dir / "st83-deployment-scale-down-unblocks-batch-and-zero",
            speedup=40.0,
        )

        nodes_8tpu = [
            {
                "metadata": {"name": f"node-{i}"},
                "status": {"allocatable": {"cpu": "16", "memory": "64Gi", "pods": "110", "google.com/tpu": "4"}},
            }
            for i in range(2)
        ]

        def _mk_wl(idx: int, admitted: bool, t0_iso: str) -> dict:
            conds = (
                [{"type": "Admitted", "status": "True", "reason": "Admitted", "lastTransitionTime": t0_iso}]
                if admitted
                else [
                    # Kueue v0.20 UnadmittedWorkloadsObservability sets Admitted=False with lastTransitionTime on pending pods!
                    {
                        "type": "Admitted",
                        "status": "False",
                        "reason": "NoReservation",
                        "lastTransitionTime": t0_iso,
                    }
                ]
            )
            st: dict = {"conditions": conds}
            if admitted:
                st["admission"] = {"clusterQueue": "cq-8tpu"}
            return {
                "apiVersion": "kueue.x-k8s.io/v1beta2",
                "kind": "Workload",
                "metadata": {
                    "name": f"pod-srv-rs-{idx}",
                    "namespace": "ai-lab",
                    "creationTimestamp": t0_iso,
                    "ownerReferences": [{"kind": "Pod", "name": f"srv-rs-{idx}"}],
                },
                "spec": {
                    "podSets": [
                        {
                            "name": "main",
                            "count": 1,
                            "template": {
                                "spec": {
                                    "containers": [
                                        {
                                            "name": "srv",
                                            "resources": {
                                                "requests": {"cpu": "2", "memory": "8Gi", "google.com/tpu": "2"}
                                            },
                                        }
                                    ]
                                }
                            },
                        }
                    ]
                },
                "status": st,
            }

        # Case A (Comment 1 + Comment 5 pending scale-down & Evicted+OwnerNotFound):
        # 8-TPU cluster, 8x2-TPU Deployment (4 admitted + 4 pending with Admitted=False/lastTransitionTime):
        #   - ScaleDown 8 -> 6 removes 2 pending pods (must leave 0 clean budget, NOT 2!)
        #   - Pod 3 is preempted WITH both Evicted=True AND Finished=True/OwnerNotFound (must be Excluded, NOT completed!)
        #   - ScaleDown 6 -> 0 retires the remaining 3 admitted pods cleanly (completed=3, Excluded=1, NEVER >100%!)
        base_ep = time.time() - 20.0
        t0_iso = datetime.fromtimestamp(base_ep, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        dep_init_doc = {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "srv", "namespace": "ai-lab"},
            "spec": {"replicas": 8, "paused": False},
        }
        bundle = dc_replace(
            base_b83,
            t0_epoch=base_ep,
            effective_start_epoch=base_ep,
            t_end_epoch=base_ep + 20.0,
            workload_namespaces=["ai-lab"],
            infra_pods_namespace="infra-pods",
            nodes=nodes_8tpu,
            initial_workloads=[],
            initial_deployments=[],
            initial_pods=[],
            scheduled_infra_events=[],
            scheduled_workflows=[
                ScheduledWorkflowItem(
                    target_wall_offset_sec=0.0,
                    seq=1,
                    raw_offset_sec=0.0,
                    raw_offset_from_t0_sec=0.0,
                    event_timestamp=t0_iso,
                    object_kind="Deployment",
                    name="srv",
                    namespace="ai-lab",
                    local_queue_name="lq",
                    workload_state="NotAdmitted",
                    trigger_reason="post_t0_arrival",
                    execution_duration_sec=0.0,
                    compressed_duration_sec=0.0,
                    indexed_from_execution_time=False,
                    pod_count=8,
                    manifest=dep_init_doc,
                    creation_epoch=base_ep,
                )
            ],
            scheduled_workflow_updates=[
                # 1. Pending-pod scale-down 8 -> 6 (removes 2 pending pods)
                ScheduledWorkflowUpdate(
                    target_wall_offset_sec=0.05,
                    seq=2,
                    raw_offset_sec=5.0,
                    raw_offset_from_t0_sec=5.0,
                    event_timestamp=t0_iso,
                    object_kind="Deployment",
                    name="srv",
                    applied_name="srv",
                    namespace="ai-lab",
                    local_queue_name="lq",
                    replicas=6,
                    paused=False,
                    template_hash="th-1",
                    update_types=("ScaleDown",),
                    manifest={"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": "srv", "namespace": "ai-lab"}, "spec": {"replicas": 6, "paused": False}},
                ),
                # 2. Paused Upgrade while paused=True (must NOT add clean retirement budget until Resumed!)
                ScheduledWorkflowUpdate(
                    target_wall_offset_sec=0.10,
                    seq=3,
                    raw_offset_sec=10.0,
                    raw_offset_from_t0_sec=10.0,
                    event_timestamp=t0_iso,
                    object_kind="Deployment",
                    name="srv",
                    applied_name="srv",
                    namespace="ai-lab",
                    local_queue_name="lq",
                    replicas=6,
                    paused=True,
                    template_hash="th-2",
                    update_types=("Paused", "Upgrade"),
                    manifest={"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": "srv", "namespace": "ai-lab"}, "spec": {"replicas": 6, "paused": True}},
                ),
                # 3. Combined Resumed + ScaleDown 6 -> 2 (rolls 3 remaining v1 pods into 2 v2 pods; after convergence,
                #    one v2 pod is preempted and MUST NOT consume leftover budget!)
                ScheduledWorkflowUpdate(
                    target_wall_offset_sec=0.18,
                    seq=4,
                    raw_offset_sec=15.0,
                    raw_offset_from_t0_sec=15.0,
                    event_timestamp=t0_iso,
                    object_kind="Deployment",
                    name="srv",
                    applied_name="srv",
                    namespace="ai-lab",
                    local_queue_name="lq",
                    replicas=2,
                    paused=False,
                    template_hash="th-2",
                    update_types=("Resumed", "ScaleDown"),
                    manifest={"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": "srv", "namespace": "ai-lab"}, "spec": {"replicas": 2, "paused": False}},
                ),
                # 4. Final ScaleDown 2 -> 0 (retires only the 1 remaining non-preempted v2 pod cleanly)
                ScheduledWorkflowUpdate(
                    target_wall_offset_sec=0.30,
                    seq=5,
                    raw_offset_sec=20.0,
                    raw_offset_from_t0_sec=20.0,
                    event_timestamp=t0_iso,
                    object_kind="Deployment",
                    name="srv",
                    applied_name="srv",
                    namespace="ai-lab",
                    local_queue_name="lq",
                    replicas=0,
                    paused=False,
                    template_hash="th-2",
                    update_types=("ScaleDown",),
                    manifest={"apiVersion": "apps/v1", "kind": "Deployment", "metadata": {"name": "srv", "namespace": "ai-lab"}, "spec": {"replicas": 0, "paused": False}},
                ),
            ],
            scheduled_workflow_deletions=[],
        )

        mgr = ChamberManager(chamber_namespace="kueue-optimizer-chamber-ut-pr13", apiserver_port=16598)
        try:
            live_wls: list[dict] = []
            phase_ref = ["init"]

            def fake_apply_parallel(docs, **_kw):
                for d in docs:
                    if d.get("kind") == "Deployment":
                        # 4 admitted + 4 pending (each pending has Admitted=False with lastTransitionTime!)
                        live_wls[:] = [_mk_wl(i, i < 4, t0_iso) for i in range(8)]

            def fake_ssa(doc):
                rep = int(doc["spec"]["replicas"])
                paused = bool(doc["spec"].get("paused", False))
                now_ret = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                if rep == 6 and not paused:
                    # Drop 2 never-admitted pending pods (indices 6, 7)
                    live_wls[:] = live_wls[:6]
                elif rep == 6 and paused:
                    # While paused, pod 3 is preempted AND gets Finished=True/OwnerNotFound before GC!
                    preempted_w3 = json.loads(json.dumps(live_wls[3]))
                    preempted_w3["status"]["conditions"].extend(
                        [
                            {"type": "Evicted", "status": "True", "reason": "Preempted", "lastTransitionTime": now_ret},
                            {"type": "Preempted", "status": "True", "reason": "InClusterQueue", "lastTransitionTime": now_ret},
                            {"type": "Finished", "status": "True", "reason": "OwnerNotFound", "lastTransitionTime": now_ret},
                        ]
                    )
                    live_wls[3] = preempted_w3
                elif rep == 2 and not paused:
                    # Deferred Upgrade + ScaleDown retires the 3 remaining v1 pods (0, 1, 2) and starts 2 v2 pods (100, 101),
                    # then pod 101 vanishes without ScaleDown (preempted & GC'd after rollout converged)!
                    live_wls[:] = [_mk_wl(100, True, now_ret), _mk_wl(101, True, now_ret)]
                    phase_ref[0] = "rolled_v2"
                elif rep == 0:
                    live_wls.clear()

            api_poll_after_v2 = [0]

            def fake_api_req(method, path, **_kw):
                if method == "GET" and path.endswith("/workloads"):
                    if phase_ref[0] == "rolled_v2":
                        api_poll_after_v2[0] += 1
                        if api_poll_after_v2[0] == 2 and len(live_wls) == 2:
                            # Simulate pod 101 being preempted & GC'd after v2 converged (must NOT use leftover budget!)
                            live_wls[:] = live_wls[:1]
                    return 200, json.dumps({"items": list(live_wls)}).encode("utf-8")
                if method == "GET" and path == "/api/v1/nodes":
                    return 200, json.dumps({"items": nodes_8tpu}).encode("utf-8")
                if method == "GET" and path == "/api/v1/pods":
                    return 200, json.dumps({"items": []}).encode("utf-8")
                return 200, b"{}"

            def fake_q_status(*_a, **_kw):
                return ClusterStatusSnapshot(
                    workloads_admitted=sum(1 for w in live_wls if (w.get("status") or {}).get("admission")),
                    workloads_pending=sum(1 for w in live_wls if not (w.get("status") or {}).get("admission")),
                    workloads_finished=0,
                    pods_running=sum(1 for w in live_wls if (w.get("status") or {}).get("admission")),
                    pods_pending=0,
                    pods_succeeded=0,
                    pods_failed=0,
                    jobsets_completed=0,
                    jobsets_failed=0,
                    cq_summary=[],
                    finished_workload_keys=set(),
                    present_workload_keys={("ai-lab", w["metadata"]["name"]) for w in live_wls},
                    workloads_query_ok=True,
                )

            with (
                mock.patch.object(mgr, "ensure_kwok_connection"),
                mock.patch.object(mgr, "apply_manifests_parallel", side_effect=fake_apply_parallel),
                mock.patch.object(mgr, "ssa_apply_doc", side_effect=fake_ssa),
                mock.patch.object(mgr, "api_request", side_effect=fake_api_req),
                mock.patch("runner.replay.query_cluster_status", side_effect=fake_q_status),
            ):
                _s_ep, e_ep, _sub_cnt, _ = run_scenario_replay(
                    mgr=mgr,
                    bundle=bundle,
                    speedup=50.0,
                    max_run_seconds=5.0,
                    status_interval_sec=0.04,
                    wait_for_completion=True,
                    initial_state_already_bootstrapped=True,
                )
                sat_snap = mgr.capture_cluster_saturation_snapshot(eval_epoch=e_ep)

            self.assertIsNotNone(sat_snap)
            # Total ever-admitted pods = 6 (4 v1 + 2 v2); the 4 never-admitted pending pods MUST NOT be recorded!
            # Cleanly retired = 4 (v1 pods 0, 1, 2 + v2 pod 100); Excluded (preempted) = 2 (v1 pod 3 + v2 pod 101).
            self.assertEqual(len(sat_snap["workloads"]), 6)
            sat_rows, sat_subtitle = report_effective_uninterrupted_saturation(
                sat_snap,
                start_epoch=base_ep,
                end_epoch=e_ep,
            )
            self.assertIn("Qualifying Workloads: 4 (completed=4 [4 pods], active_at_end=0 [0 pods])", sat_subtitle)
            self.assertIn("Excluded (preempted/reclaimed/failed): 2 [2 pods]", sat_subtitle)
            tpu_row = next(r for r in sat_rows if r[0] == "google.com/tpu")
            eff_pct = float(tpu_row[2].rstrip("%"))
            self.assertLessEqual(eff_pct, 85.0, f"TPU saturation must never exceed 100% (~75%), got {tpu_row}")
        finally:
            mgr.close()

    def test_bootstrap_initial_deployments_paused_wfpr_two_phase_patch_error_and_order(self) -> None:
        """Verify paused t0 Deployment unpause/restore, two-phase now->backdated PATCH, Ready wait, PATCH error, order, and multi-flavor node matching."""
        from chamber.parsers import parse_rfc3339_epoch
        from runner.loader import InitialDeploymentBootstrap, InitialPodSpec, InitialWorkloadPodGroup
        from runner.replay import (
            _bootstrap_initial_deployments,
            _build_single_pod_deployment_workload_status,
            apply_scenario_queues_and_wait,
        )

        # 1. Multi-flavor scraped_pod_set_assignments must respect node_doc flavor match.
        cq_multi = {
            "spec": {
                "resourceGroups": [
                    {
                        "coveredResources": ["cpu", "google.com/tpu"],
                        "flavors": [{"name": "flv-a"}, {"name": "flv-b"}],
                    }
                ]
            }
        }
        idep_multi = InitialDeploymentBootstrap(
            namespace="ai-lab",
            name="web",
            applied_name="web",
            local_queue_name="lq-main",
            cluster_queue_name="cq-main",
            desired_replicas=1,
            admitted_replicas=1,
            paused=True,
            original_age_sec=3600.0,
            placements=["node-a"],
            per_pod_requests={"cpu": "2", "google.com/tpu": "2"},
            priority_class_name="",
            priority=None,
            is_tas=False,
            manifest={
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {"name": "web", "namespace": "ai-lab"},
                "spec": {"replicas": 1, "paused": True},
            },
            cq_doc=cq_multi,
            flavors_by_name={
                "flv-a": {"spec": {"nodeLabels": {"pool": "a"}}},
                "flv-b": {"spec": {"nodeLabels": {"pool": "b"}}},
            },
            topologies_by_name={},
            nodes_by_name={"node-a": {"metadata": {"labels": {"pool": "a"}}}},
            scraped_pod_set_assignments=[
                {"name": "main", "flavors": {"cpu": "flv-a", "google.com/tpu": "flv-a"}},
                {"name": "main", "flavors": {"cpu": "flv-b", "google.com/tpu": "flv-b"}},
            ],
        )
        st_multi = _build_single_pod_deployment_workload_status(
            idep_multi,
            {"spec": {"podSets": [{"name": "main"}]}},
            "node-a",
            "2026-01-01T00:00:00Z",
        )
        self.assertEqual(
            st_multi["admission"]["podSetAssignments"][0]["flavors"],
            {"cpu": "flv-a", "google.com/tpu": "flv-a"},
        )

        # 2. Unmocked _bootstrap_initial_deployments: paused t0 Deployment applied paused=False -> patched with now ->
        #    waits for Pod Ready=True -> patched with back-dated wl_ts -> restored to paused=True.
        mgr = ChamberManager(chamber_namespace="kueue-optimizer-chamber-test-boot", apiserver_port=16498)
        try:
            applied_paused_seq: list[bool] = []
            patch_timestamps: list[str] = []
            pod_get_calls = [0]

            def fake_apply(docs, max_workers=16):
                for d in docs:
                    if d.get("kind") == "Deployment":
                        applied_paused_seq.append(bool((d.get("spec") or {}).get("paused", False)))

            def fake_api(method: str, path: str, body: bytes | None = None, headers: dict | None = None):
                if method == "GET" and path.endswith("/replicasets"):
                    # Only create ReplicaSet once Deployment has been applied unpaused!
                    if applied_paused_seq and applied_paused_seq[-1] is False:
                        return 200, json.dumps(
                            {
                                "items": [
                                    {
                                        "metadata": {
                                            "name": "web-rs1",
                                            "ownerReferences": [{"kind": "Deployment", "name": "web"}],
                                        }
                                    }
                                ]
                            }
                        ).encode("utf-8")
                    return 200, b'{"items": []}'
                if method == "GET" and path.endswith("/namespaces/ai-lab/pods"):
                    return 200, json.dumps(
                        {
                            "items": [
                                {
                                    "metadata": {
                                        "name": "web-rs1-p0",
                                        "namespace": "ai-lab",
                                        "ownerReferences": [{"kind": "ReplicaSet", "name": "web-rs1"}],
                                    }
                                }
                            ]
                        }
                    ).encode("utf-8")
                if method == "GET" and path == "/api/v1/pods":
                    pod_get_calls[0] += 1
                    # First poll: Running but Ready=False (must NOT return yet!); second poll: Ready=True.
                    ready_status = "True" if pod_get_calls[0] >= 2 else "False"
                    return 200, json.dumps(
                        {
                            "items": [
                                {
                                    "metadata": {"name": "web-rs1-p0", "namespace": "ai-lab"},
                                    "status": {
                                        "phase": "Running",
                                        "conditions": [{"type": "Ready", "status": ready_status}],
                                    },
                                }
                            ]
                        }
                    ).encode("utf-8")
                if method == "GET" and path.endswith("/workloads"):
                    return 200, json.dumps(
                        {
                            "items": [
                                {
                                    "metadata": {
                                        "name": "pod-web-rs1-p0-abcde",
                                        "namespace": "ai-lab",
                                        "ownerReferences": [{"kind": "Pod", "name": "web-rs1-p0"}],
                                    },
                                    "spec": {"podSets": [{"name": "main", "count": 1}]},
                                }
                            ]
                        }
                    ).encode("utf-8")
                if method == "PATCH" and path.endswith("/status") and body:
                    st_payload = json.loads(body.decode("utf-8"))["status"]
                    adm_c = next(c for c in st_payload["conditions"] if c["type"] == "Admitted")
                    patch_timestamps.append(adm_c["lastTransitionTime"])
                    return 200, b"{}"
                return 200, b"{}"

            with (
                mock.patch.object(mgr, "apply_manifests_parallel", side_effect=fake_apply),
                mock.patch.object(mgr, "api_request", side_effect=fake_api),
            ):
                _bootstrap_initial_deployments(mgr, [idep_multi])

            self.assertEqual(applied_paused_seq, [False, True])
            self.assertEqual(len(patch_timestamps), 2)
            self.assertGreaterEqual(pod_get_calls[0], 2)
            t_first = parse_rfc3339_epoch(patch_timestamps[0])
            t_second = parse_rfc3339_epoch(patch_timestamps[1])
            self.assertIsNotNone(t_first)
            self.assertIsNotNone(t_second)
            self.assertGreater(t_first - t_second, 3500.0)

            # 3. Non-2xx status PATCH fails fast instead of hanging until pod timeout.
            def fake_api_reject(method: str, path: str, body: bytes | None = None, headers: dict | None = None):
                if method == "PATCH" and path.endswith("/status"):
                    return 422, b'{"message":"invalid status"}'
                return fake_api(method, path, body, headers)

            applied_paused_seq.clear()
            with (
                mock.patch.object(mgr, "apply_manifests_parallel", side_effect=fake_apply),
                mock.patch.object(mgr, "api_request", side_effect=fake_api_reject),
            ):
                with self.assertRaises(RuntimeError) as ctx_err:
                    _bootstrap_initial_deployments(mgr, [idep_multi])
                self.assertIn("HTTP 422", str(ctx_err.exception))

            # 4. apply_scenario_queues_and_wait places pinned pods BEFORE _bootstrap_initial_deployments,
            #    and both BEFORE ClusterQueue/LocalQueue.
            order_log: list[str] = []
            pinned_pod = InitialPodSpec(
                name="pinned-0",
                namespace="infra-pods",
                placement="node-a",
                resource_request={"cpu": "2"},
                remaining_duration_sec=100.0,
                is_infinite=True,
                manifest={"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "pinned-0", "namespace": "infra-pods"}},
            )
            prereqs = [
                {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "ai-lab"}},
                {"apiVersion": "kueue.x-k8s.io/v1beta2", "kind": "ResourceFlavor", "metadata": {"name": "flv-a"}},
                {"apiVersion": "kueue.x-k8s.io/v1beta2", "kind": "ClusterQueue", "metadata": {"name": "cq-main"}},
                {"apiVersion": "kueue.x-k8s.io/v1beta2", "kind": "LocalQueue", "metadata": {"name": "lq-main", "namespace": "ai-lab"}},
            ]

            def record_apply(docs, max_workers=16):
                kinds = sorted({str(d.get("kind") or "") for d in docs})
                if kinds:
                    order_log.append(f"apply:{','.join(kinds)}")

            with (
                mock.patch.object(mgr, "apply_manifests_parallel", side_effect=record_apply),
                mock.patch.object(mgr, "wait_for_pods_running", side_effect=lambda *_a, **_kw: order_log.append("wait_pinned")),
                mock.patch("runner.replay._bootstrap_initial_deployments", side_effect=lambda *_a, **_kw: order_log.append("bootstrap_deps")),
                mock.patch.object(
                    mgr,
                    "api_request",
                    side_effect=lambda m, p, **kw: (
                        200,
                        json.dumps(
                            {
                                "items": [
                                    {
                                        "metadata": {"name": "cq-main" if "clusterqueues" in p else "lq-main", "namespace": "ai-lab"},
                                        "status": {"conditions": [{"type": "Active", "status": "True"}]},
                                    }
                                ]
                            }
                        ).encode("utf-8"),
                    ),
                ),
            ):
                apply_scenario_queues_and_wait(
                    mgr,
                    prereqs,
                    initial_deployments=[idep_multi],
                    initial_pods=[pinned_pod],
                )
            self.assertLess(order_log.index("wait_pinned"), order_log.index("bootstrap_deps"))
            self.assertLess(order_log.index("bootstrap_deps"), order_log.index("apply:ClusterQueue,LocalQueue"))
        finally:
            mgr.close()

    def test_deployment_loader_revision_digest_recreate_orphan_updates_tpl_lq_and_zero_replicas(self) -> None:
        """Verify revision annotation dedup, fast recreate not teleported to t0, orphan update filtering, template-only queue-name on updates, and t0 replicas=0 resources."""
        import os

        with tempfile.TemporaryDirectory() as tmpdir:
            t0 = "2026-08-01T10:00:00Z"
            infra_events = [
                {
                    "timestamp": t0,
                    "eventType": "New",
                    "payload": {
                        "apiVersion": "v1",
                        "kind": "Node",
                        "metadata": {"name": "node-0", "labels": {"kubernetes.io/hostname": "node-0"}},
                        "status": {"allocatable": {"cpu": "16", "memory": "64Gi", "google.com/tpu": "8"}},
                    },
                },
                {
                    "timestamp": t0,
                    "eventType": "New",
                    "payload": {
                        "apiVersion": "kueue.x-k8s.io/v1beta2",
                        "kind": "ResourceFlavor",
                        "metadata": {"name": "rf-main"},
                    },
                },
                {
                    "timestamp": t0,
                    "eventType": "New",
                    "payload": {
                        "apiVersion": "kueue.x-k8s.io/v1beta2",
                        "kind": "ClusterQueue",
                        "metadata": {"name": "cq-main"},
                        "spec": {
                            "resourceGroups": [
                                {
                                    "coveredResources": ["cpu", "memory", "google.com/tpu"],
                                    "flavors": [{"name": "rf-main", "resources": [{"name": "cpu", "nominalQuota": "16"}, {"name": "memory", "nominalQuota": "64Gi"}, {"name": "google.com/tpu", "nominalQuota": "8"}]}],
                                }
                            ]
                        },
                    },
                },
                {
                    "timestamp": t0,
                    "eventType": "New",
                    "payload": {
                        "apiVersion": "kueue.x-k8s.io/v1beta2",
                        "kind": "LocalQueue",
                        "metadata": {"name": "lq-main", "namespace": "ai-lab"},
                        "spec": {"clusterQueue": "cq-main"},
                    },
                },
            ]

            def mk_dep_obj(name: str, replicas: int, rev: str, *, tpl_only_lq: bool = False, lq: str = "lq-main", tas: bool = False):
                meta_lbls = {} if tpl_only_lq or not lq else {"kueue.x-k8s.io/queue-name": lq}
                tpl_lbls = {"app": name}
                if tpl_only_lq and lq:
                    tpl_lbls["kueue.x-k8s.io/queue-name"] = lq
                tpl_anns = {"kueue-optimizer.gke.io/template-hash": "th-0000000000000001"}
                if tas:
                    tpl_anns["kueue.x-k8s.io/podset-required-topology"] = "kubernetes.io/hostname"
                return {
                    "apiVersion": "apps/v1",
                    "kind": "Deployment",
                    "metadata": {
                        "name": name,
                        "namespace": "ai-lab",
                        "labels": meta_lbls,
                        "annotations": {"deployment.kubernetes.io/revision": rev},
                    },
                    "spec": {
                        "replicas": replicas,
                        "selector": {"matchLabels": {"app": name}},
                        "template": {
                            "metadata": {"labels": tpl_lbls, "annotations": tpl_anns},
                            "spec": {
                                "containers": [
                                    {
                                        "name": "c",
                                        "image": "pause:3.9",
                                        "resources": {"requests": {"cpu": "4", "memory": "8Gi", "google.com/tpu": "2"}},
                                    }
                                ]
                            },
                        },
                    },
                }

            wf_events = [
                # 1. t0 Deployment with template-only queue-name + revision bump (must produce 0 SpecUpdates on rev bump,
                #    and real ScaleUp must carry queue-name in metadata.labels) + deleted at +20s and recreated at +20.1s.
                {
                    "timestamp": t0,
                    "eventType": "New",
                    "workloadState": "Admitted",
                    "payload": {
                        "objectKind": "Deployment",
                        "name": "web",
                        "namespace": "ai-lab",
                        "creationTime": "2026-08-01T09:59:00Z",
                        "localQueueName": "lq-main",
                        "replicas": 2,
                        "readyReplicas": 2,
                        "object": mk_dep_obj("web", 2, "1", tpl_only_lq=True),
                    },
                },
                {
                    "timestamp": "2026-08-01T10:00:05Z",
                    "eventType": "Update",
                    "workloadState": "Admitted",
                    "payload": {
                        "objectKind": "Deployment",
                        "name": "web",
                        "namespace": "ai-lab",
                        "creationTime": "2026-08-01T09:59:00Z",
                        "localQueueName": "lq-main",
                        "replicas": 2,
                        "readyReplicas": 2,
                        "object": mk_dep_obj("web", 2, "2", tpl_only_lq=True),
                    },
                },
                {
                    "timestamp": "2026-08-01T10:00:10Z",
                    "eventType": "Update",
                    "workloadState": "Admitted",
                    "payload": {
                        "objectKind": "Deployment",
                        "name": "web",
                        "namespace": "ai-lab",
                        "creationTime": "2026-08-01T09:59:00Z",
                        "localQueueName": "lq-main",
                        "replicas": 3,
                        "readyReplicas": 2,
                        "updateTypes": ["ScaleUp"],
                        "object": mk_dep_obj("web", 3, "2", tpl_only_lq=True),
                    },
                },
                {
                    "timestamp": "2026-08-01T10:00:20Z",
                    "eventType": "Deleted",
                    "workloadState": "Admitted",
                    "payload": {
                        "objectKind": "Deployment",
                        "name": "web",
                        "namespace": "ai-lab",
                        "creationTime": "2026-08-01T09:59:00Z",
                        "localQueueName": "lq-main",
                        "replicas": 3,
                        "readyReplicas": 3,
                        "object": mk_dep_obj("web", 3, "2", tpl_only_lq=True),
                    },
                },
                {
                    "timestamp": "2026-08-01T10:00:20.100Z",
                    "eventType": "New",
                    "workloadState": "Admitted",
                    "payload": {
                        "objectKind": "Deployment",
                        "name": "web",
                        "namespace": "ai-lab",
                        "creationTime": "2026-08-01T10:00:20.100Z",
                        "localQueueName": "lq-main",
                        "replicas": 2,
                        "readyReplicas": 0,
                        "object": mk_dep_obj("web", 2, "1", tpl_only_lq=True),
                    },
                },
                # 2. Unmapped CQ Deployment with an update -> update MUST be filtered out (not orphaned).
                {
                    "timestamp": t0,
                    "eventType": "New",
                    "workloadState": "Admitted",
                    "payload": {
                        "objectKind": "Deployment",
                        "name": "unmapped-dep",
                        "namespace": "ai-lab",
                        "creationTime": "2026-08-01T09:59:00Z",
                        "localQueueName": "",
                        "replicas": 1,
                        "readyReplicas": 1,
                        "object": mk_dep_obj("unmapped-dep", 1, "1", lq=""),
                    },
                },
                {
                    "timestamp": "2026-08-01T10:00:08Z",
                    "eventType": "Update",
                    "workloadState": "Admitted",
                    "payload": {
                        "objectKind": "Deployment",
                        "name": "unmapped-dep",
                        "namespace": "ai-lab",
                        "creationTime": "2026-08-01T09:59:00Z",
                        "localQueueName": "",
                        "replicas": 4,
                        "readyReplicas": 1,
                        "updateTypes": ["ScaleUp"],
                        "object": mk_dep_obj("unmapped-dep", 4, "1", lq=""),
                    },
                },
                # 3. t0 Admitted Deployment with replicas=0 -> must keep real GPU/TPU requests and is_tas=True.
                {
                    "timestamp": t0,
                    "eventType": "New",
                    "workloadState": "Admitted",
                    "payload": {
                        "objectKind": "Deployment",
                        "name": "zero-tas",
                        "namespace": "ai-lab",
                        "creationTime": "2026-08-01T09:59:00Z",
                        "localQueueName": "lq-main",
                        "replicas": 0,
                        "readyReplicas": 0,
                        "object": mk_dep_obj("zero-tas", 0, "1", tas=True),
                    },
                },
            ]

            with open(os.path.join(tmpdir, "infra.json"), "w", encoding="utf-8") as f:
                for ev in infra_events:
                    f.write(json.dumps(ev) + "\n")
            with open(os.path.join(tmpdir, "workflows.json"), "w", encoding="utf-8") as f:
                for ev in wf_events:
                    f.write(json.dumps(ev) + "\n")
            with open(os.path.join(tmpdir, "execution-time.json"), "w", encoding="utf-8") as f:
                f.write("")

            bundle = testrunner.load_scenario_bundle(Path(tmpdir), speedup=1.0, skip_initial_offset=False)

            # F6: Only 1 ScaleUp update for web (revision-only bump ignored); N7: 0 updates for unmapped-dep.
            self.assertEqual([(u.name, u.update_types, u.replicas) for u in bundle.scheduled_workflow_updates], [("web", ("ScaleUp",), 3)])
            # N8: template-only queue-name injected into ScheduledWorkflowUpdate metadata.labels.
            self.assertEqual(
                bundle.scheduled_workflow_updates[0].manifest["metadata"]["labels"].get("kueue.x-k8s.io/queue-name"),
                "lq-main",
            )
            # N2: Only 1 initial_deployments entry for web (inc 0); inc 1 at +20.1s is in scheduled_workflows!
            web_inits = [d for d in bundle.initial_deployments if d.name == "web"]
            self.assertEqual(len(web_inits), 1)
            self.assertEqual(web_inits[0].incarnation_index, 0)
            web_sched = [w for w in bundle.scheduled_workflows if w.name == "web"]
            self.assertEqual(len(web_sched), 1)
            self.assertEqual(web_sched[0].incarnation_index, 1)
            # NEW-7: zero-tas keeps real per_pod_requests and is_tas=True, plus deployment-name annotation.
            zero_dep = next(d for d in bundle.initial_deployments if d.name == "zero-tas")
            self.assertEqual(zero_dep.per_pod_requests.get("google.com/tpu"), "2")
            self.assertTrue(zero_dep.is_tas)
            self.assertEqual(
                zero_dep.manifest["spec"]["template"]["metadata"]["annotations"].get("kueue-optimizer.gke.io/deployment-name"),
                "zero-tas",
            )

    def test_deployment_replay_chronological_update_delete_58char_name_plain_pod_and_elig_set(self) -> None:
        """Verify chronological update/delete & cancelled-create update suppression, 58-char Deployment credit, plain-pod non-collision, and elig_set membership."""
        from dataclasses import replace as dc_replace
        from datetime import datetime, timezone
        import time

        from chamber.lifecycle import ClusterStatusSnapshot
        from metrics.saturation import report_effective_uninterrupted_saturation
        from runner.loader import (
            ScheduledWorkflowDeletion,
            ScheduledWorkflowItem,
            ScheduledWorkflowUpdate,
            load_scenario_bundle,
        )
        from runner.replay import run_scenario_replay

        st83_dir = REPO_ROOT / "testscenarios" / "self-tests" / "st83-deployment-scale-down-unblocks-batch-and-zero"
        base_b83 = load_scenario_bundle(st83_dir, speedup=100.0)

        long_dep = "a" * 58
        base_ep = time.time() - 10.0
        base_iso = datetime.fromtimestamp(base_ep, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        def mk_dep_manifest(name: str, replicas: int):
            return {
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {"name": name, "namespace": "ai-lab", "labels": {"kueue.x-k8s.io/queue-name": "lq-main"}},
                "spec": {
                    "replicas": replicas,
                    "paused": False,
                    "template": {
                        "metadata": {
                            "annotations": {
                                "kueue-optimizer.gke.io/long-running-pod": "true",
                                "kueue-optimizer.gke.io/infinite-duration": "true",
                                "kueue-optimizer.gke.io/deployment-name": name,
                            }
                        },
                        "spec": {
                            "containers": [
                                {"name": "c", "image": "pause:3.9", "resources": {"requests": {"google.com/tpu": "2"}}}
                            ]
                        },
                    },
                },
            }

        bundle = dc_replace(
            base_b83,
            t0_epoch=base_ep,
            t0_iso=base_iso,
            effective_start_epoch=base_ep,
            effective_start_iso=base_iso,
            t_end_epoch=base_ep + 30.0,
            t_end_iso=base_iso,
            nodes=[],
            initial_pods=[],
            initial_workloads=[],
            initial_deployments=[],
            scheduled_infra_events=[],
            workload_namespaces=["ai-lab"],
            infra_pods_namespace="infra-pods",
            scheduled_workflows=[
                # 58-char Deployment created at t=0.0s, upgraded at 0.08s, scaled to 0 at 0.20s
                ScheduledWorkflowItem(
                    target_wall_offset_sec=0.0,
                    seq=1,
                    raw_offset_sec=0.0,
                    raw_offset_from_t0_sec=0.0,
                    event_timestamp=base_iso,
                    object_kind="Deployment",
                    name=long_dep,
                    namespace="ai-lab",
                    local_queue_name="lq-main",
                    workload_state="NotAdmitted",
                    trigger_reason="initial_not_admitted",
                    execution_duration_sec=10.0,
                    compressed_duration_sec=10.0,
                    indexed_from_execution_time=False,
                    pod_count=1,
                    manifest=mk_dep_manifest(long_dep, 1),
                    creation_epoch=base_ep,
                    incarnation_index=0,
                ),
                # Cancelled-before-submit Deployment: create at +0.15s, cancelled by delete at +0.05s, stale update at +0.18s
                ScheduledWorkflowItem(
                    target_wall_offset_sec=0.15,
                    seq=2,
                    raw_offset_sec=15.0,
                    raw_offset_from_t0_sec=15.0,
                    event_timestamp=base_iso,
                    object_kind="Deployment",
                    name="ghost-srv",
                    namespace="ai-lab",
                    local_queue_name="lq-main",
                    workload_state="NotAdmitted",
                    trigger_reason="initial_not_admitted",
                    execution_duration_sec=10.0,
                    compressed_duration_sec=10.0,
                    indexed_from_execution_time=False,
                    pod_count=2,
                    manifest=mk_dep_manifest("ghost-srv", 2),
                    creation_epoch=base_ep + 15.0,
                    incarnation_index=0,
                ),
                # Same-tick update (+0.02s) + delete (+0.03s) Deployment: update MUST run before delete, never after!
                ScheduledWorkflowItem(
                    target_wall_offset_sec=0.0,
                    seq=3,
                    raw_offset_sec=0.0,
                    raw_offset_from_t0_sec=0.0,
                    event_timestamp=base_iso,
                    object_kind="Deployment",
                    name="ephemeral",
                    namespace="ai-lab",
                    local_queue_name="lq-main",
                    workload_state="NotAdmitted",
                    trigger_reason="initial_not_admitted",
                    execution_duration_sec=10.0,
                    compressed_duration_sec=10.0,
                    indexed_from_execution_time=False,
                    pod_count=1,
                    manifest=mk_dep_manifest("ephemeral", 1),
                    creation_epoch=base_ep,
                    incarnation_index=0,
                ),
            ],
            scheduled_workflow_updates=[
                ScheduledWorkflowUpdate(
                    target_wall_offset_sec=0.02,
                    seq=1,
                    raw_offset_sec=2.0,
                    raw_offset_from_t0_sec=2.0,
                    event_timestamp=base_iso,
                    object_kind="Deployment",
                    name="ephemeral",
                    applied_name="ephemeral",
                    namespace="ai-lab",
                    local_queue_name="lq-main",
                    incarnation_index=0,
                    replicas=2,
                    paused=False,
                    template_hash="th-0000000000000002",
                    update_types=("ScaleUp",),
                    manifest=mk_dep_manifest("ephemeral", 2),
                ),
                ScheduledWorkflowUpdate(
                    target_wall_offset_sec=0.08,
                    seq=2,
                    raw_offset_sec=8.0,
                    raw_offset_from_t0_sec=8.0,
                    event_timestamp=base_iso,
                    object_kind="Deployment",
                    name=long_dep,
                    applied_name=long_dep,
                    namespace="ai-lab",
                    local_queue_name="lq-main",
                    incarnation_index=0,
                    replicas=1,
                    paused=False,
                    template_hash="th-0000000000000002",
                    update_types=("Upgrade",),
                    manifest=mk_dep_manifest(long_dep, 1),
                ),
                ScheduledWorkflowUpdate(
                    target_wall_offset_sec=0.18,
                    seq=3,
                    raw_offset_sec=18.0,
                    raw_offset_from_t0_sec=18.0,
                    event_timestamp=base_iso,
                    object_kind="Deployment",
                    name="ghost-srv",
                    applied_name="ghost-srv",
                    namespace="ai-lab",
                    local_queue_name="lq-main",
                    incarnation_index=0,
                    replicas=4,
                    paused=False,
                    template_hash="th-0000000000000001",
                    update_types=("ScaleUp",),
                    manifest=mk_dep_manifest("ghost-srv", 4),
                ),
                ScheduledWorkflowUpdate(
                    target_wall_offset_sec=0.22,
                    seq=4,
                    raw_offset_sec=22.0,
                    raw_offset_from_t0_sec=22.0,
                    event_timestamp=base_iso,
                    object_kind="Deployment",
                    name=long_dep,
                    applied_name=long_dep,
                    namespace="ai-lab",
                    local_queue_name="lq-main",
                    incarnation_index=0,
                    replicas=0,
                    paused=False,
                    template_hash="th-0000000000000002",
                    update_types=("ScaleDown",),
                    manifest=mk_dep_manifest(long_dep, 0),
                ),
            ],
            scheduled_workflow_deletions=[
                ScheduledWorkflowDeletion(
                    target_wall_offset_sec=0.03,
                    seq=1,
                    raw_offset_sec=3.0,
                    raw_offset_from_t0_sec=3.0,
                    event_timestamp=base_iso,
                    object_kind="Deployment",
                    manifest_kind="Deployment",
                    manifest_api_version="apps/v1",
                    name="ephemeral",
                    applied_name="ephemeral",
                    namespace="ai-lab",
                    delete_reason="cancelled",
                    incarnation_index=0,
                ),
                ScheduledWorkflowDeletion(
                    target_wall_offset_sec=0.05,
                    seq=2,
                    raw_offset_sec=5.0,
                    raw_offset_from_t0_sec=5.0,
                    event_timestamp=base_iso,
                    object_kind="Deployment",
                    manifest_kind="Deployment",
                    manifest_api_version="apps/v1",
                    name="ghost-srv",
                    applied_name="ghost-srv",
                    namespace="ai-lab",
                    delete_reason="cancelled",
                    incarnation_index=0,
                ),
            ],
        )

        mgr = ChamberManager(chamber_namespace="kueue-optimizer-chamber-test-r2", apiserver_port=16497)
        try:
            actions: list[str] = []
            live_wls: list[dict] = []
            nodes = [{"metadata": {"name": "n0"}, "status": {"allocatable": {"google.com/tpu": "8"}}}]

            def mk_long_wl(pod_name: str, suffix: str):
                return {
                    "metadata": {
                        "name": f"pod-{pod_name[:50]}-{suffix}",
                        "namespace": "ai-lab",
                        "ownerReferences": [{"kind": "Pod", "name": pod_name}],
                    },
                    "spec": {
                        "podSets": [
                            {
                                "name": "main",
                                "count": 1,
                                # Like a live Kueue Workload: the template holds Pod.spec only, no Pod annotations.
                                "template": {
                                    "metadata": {},
                                    "spec": {"containers": [{"resources": {"requests": {"google.com/tpu": "2"}}}]},
                                },
                            }
                        ]
                    },
                    "status": {
                        "admission": {
                            "clusterQueue": "cq-main",
                            "podSetAssignments": [
                                {"name": "main", "count": 1, "flavors": {"google.com/tpu": "rf"}, "resourceUsage": {"google.com/tpu": "2"}}
                            ],
                        },
                        "conditions": [{"type": "Admitted", "status": "True", "lastTransitionTime": base_iso}],
                    },
                }

            # Plain finite Pod named "<ephemeral>-smoke-test" must NOT be classified as serving.
            plain_pod_wl = {
                "metadata": {
                    "name": "pod-ephemeral-smoke-test-99999",
                    "namespace": "ai-lab",
                    "ownerReferences": [{"kind": "Pod", "name": "ephemeral-smoke-test"}],
                },
                "spec": {
                    "podSets": [
                        {
                            "name": "main",
                            "count": 1,
                            "template": {
                                "metadata": {},
                                "spec": {"containers": [{"resources": {"requests": {"google.com/tpu": "2"}}}]},
                            },
                        }
                    ]
                },
                "status": {
                    "admission": {
                        "clusterQueue": "cq-main",
                        "podSetAssignments": [
                            {"name": "main", "count": 1, "flavors": {"google.com/tpu": "rf"}, "resourceUsage": {"google.com/tpu": "2"}}
                        ],
                    },
                    "conditions": [{"type": "Admitted", "status": "True", "lastTransitionTime": base_iso}],
                },
            }

            long_v1_wl = mk_long_wl(f"{long_dep}x1y2z", "11111")
            long_v2_wl = mk_long_wl(f"{long_dep}z9y8x", "22222")

            # The annotations exist only on the Pods, as in a live chamber.
            serving_pod_ann = {
                "kueue-optimizer.gke.io/long-running-pod": "true",
                "kueue-optimizer.gke.io/infinite-duration": "true",
                "kueue-optimizer.gke.io/deployment-name": long_dep,
                "kueue.x-k8s.io/pod-suspending-parent": "deployment",
            }
            pod_ann_by_name = {
                f"{long_dep}x1y2z": serving_pod_ann,
                f"{long_dep}z9y8x": serving_pod_ann,
                "ephemeral-smoke-test": {"kueue-optimizer.gke.io/pod-execution-time-seconds": "10"},
            }
            pod_list_calls = [0]
            upgrade_polls = [0]

            def fake_apply_parallel(docs, max_workers=16):
                for d in docs:
                    n = (d.get("metadata") or {}).get("name")
                    actions.append(f"CREATE:{n}")
                    if n == long_dep:
                        live_wls.append(long_v1_wl)
                        live_wls.append(plain_pod_wl)

            def fake_ssa(doc):
                n = (doc.get("metadata") or {}).get("name")
                rep = (doc.get("spec") or {}).get("replicas")
                actions.append(f"SSA:{n}:{rep}")
                if n == long_dep and rep == 1:
                    # Upgrade starts: v2 pod admitted while v1 is still running!
                    live_wls.append(long_v2_wl)
                    upgrade_polls[0] = 1
                elif n == long_dep and rep == 0:
                    # ScaleDown to 0 retires v1 cleanly.
                    live_wls[:] = [w for w in live_wls if w is not long_v1_wl]

            def fake_api(method: str, path: str, body: bytes | None = None, headers: dict | None = None):
                if method == "DELETE" and "/deployments/" in path:
                    dep_n = path.rsplit("/", 1)[-1]
                    actions.append(f"DELETE:{dep_n}")
                    return 200, b"{}"
                if method == "GET" and path.endswith("/workloads"):
                    if upgrade_polls[0] == 1:
                        upgrade_polls[0] = 2
                    elif upgrade_polls[0] == 2:
                        # Brand-new v2 pod (NOT in retire_eligible_wls!) gets preempted & GC'd between polls
                        # while v1 is still alive and upgrade budget=1 is open -> MUST be Excluded, NOT Completed!
                        live_wls[:] = [w for w in live_wls if w is not long_v2_wl]
                        upgrade_polls[0] = 3
                    return 200, json.dumps({"items": list(live_wls)}).encode("utf-8")
                if method == "GET" and path.startswith("/api/v1/namespaces/") and path.endswith("/pods"):
                    pod_list_calls[0] += 1
                    live_pod_names = [w["metadata"]["ownerReferences"][0]["name"] for w in live_wls]
                    return 200, json.dumps(
                        {
                            "items": [
                                {"metadata": {"name": pn, "namespace": "ai-lab", "annotations": pod_ann_by_name[pn]}}
                                for pn in live_pod_names
                            ]
                        }
                    ).encode("utf-8")
                if method == "GET" and path == "/api/v1/nodes":
                    return 200, json.dumps({"items": nodes}).encode("utf-8")
                if method == "GET" and path == "/api/v1/pods":
                    return 200, json.dumps({"items": []}).encode("utf-8")
                return 200, b"{}"

            def fake_q_status(*_a, **_kw):
                return ClusterStatusSnapshot(
                    workloads_admitted=len(live_wls),
                    workloads_pending=0,
                    workloads_finished=0,
                    pods_running=len(live_wls),
                    pods_pending=0,
                    pods_succeeded=0,
                    pods_failed=0,
                    jobsets_completed=0,
                    jobsets_failed=0,
                    cq_summary=[],
                    finished_workload_keys=set(),
                    present_workload_keys={("ai-lab", w["metadata"]["name"]) for w in live_wls},
                    workloads_query_ok=True,
                )

            with (
                mock.patch.object(mgr, "ensure_kwok_connection"),
                mock.patch.object(mgr, "apply_manifests_parallel", side_effect=fake_apply_parallel),
                mock.patch.object(mgr, "ssa_apply_doc", side_effect=fake_ssa),
                mock.patch.object(mgr, "api_request", side_effect=fake_api),
                mock.patch("runner.replay.query_cluster_status", side_effect=fake_q_status),
            ):
                _s_ep, e_ep, _sub, _ = run_scenario_replay(
                    mgr=mgr,
                    bundle=bundle,
                    speedup=100.0,
                    max_run_seconds=0.4,
                    status_interval_sec=0.03,
                    wait_for_completion=True,
                    initial_state_already_bootstrapped=True,
                )
                sat_snap = mgr.capture_cluster_saturation_snapshot(eval_epoch=e_ep)

            # F5: ephemeral update MUST happen before ephemeral delete; ghost-srv MUST NEVER be created or updated.
            self.assertIn("SSA:ephemeral:2", actions)
            self.assertIn("DELETE:ephemeral", actions)
            self.assertLess(actions.index("SSA:ephemeral:2"), actions.index("DELETE:ephemeral"))
            self.assertFalse(any("ghost-srv" in a for a in actions), f"cancelled ghost-srv must not run: {actions}")

            # R8: plain finite Pod 'ephemeral-smoke-test' was NOT captured into _retired_serving_workloads.
            self.assertNotIn(("ai-lab", "pod-ephemeral-smoke-test-99999"), mgr._retired_serving_workloads)
            # Both classifications come from the owning Pods: the mock Workloads carry no annotations.
            self.assertGreaterEqual(pod_list_calls[0], 1)

            # N6 + R5: long_v1 cleanly retired (Completed), long_v2 preempted during upgrade (Excluded), plain_pod active_at_end.
            _rows, sat_subtitle = report_effective_uninterrupted_saturation(
                sat_snap,
                start_epoch=base_ep,
                end_epoch=e_ep,
            )
            self.assertIn("completed=1 [1 pods]", sat_subtitle)
            self.assertIn("Excluded (preempted/reclaimed/failed): 1 [1 pods]", sat_subtitle)
        finally:
            mgr.close()

    def test_deployment_update_and_delete_wait_for_fifo_delayed_create(self) -> None:
        """An Update or Delete must not overtake the earlier Create of the same Deployment that waits behind the FIFO gate."""
        from dataclasses import replace as dc_replace
        from datetime import datetime, timezone
        import time

        from chamber.lifecycle import ChamberManager, ClusterStatusSnapshot
        from runner.loader import (
            ScheduledWorkflowDeletion,
            ScheduledWorkflowItem,
            ScheduledWorkflowUpdate,
            load_scenario_bundle,
        )
        from runner.replay import run_scenario_replay

        st83_dir = REPO_ROOT / "testscenarios" / "self-tests" / "st83-deployment-scale-down-unblocks-batch-and-zero"
        base_b83 = load_scenario_bundle(st83_dir, speedup=100.0)
        base_ep = time.time() - 30.0
        base_iso = datetime.fromtimestamp(base_ep, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        def mk_dep_manifest(name: str, replicas: int):
            return {
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {"name": name, "namespace": "ai-lab", "labels": {"kueue.x-k8s.io/queue-name": "lq-main"}},
                "spec": {
                    "replicas": replicas,
                    "paused": False,
                    "template": {
                        "metadata": {"annotations": {"kueue-optimizer.gke.io/deployment-name": name}},
                        "spec": {"containers": [{"name": "c", "image": "pause:3.9"}]},
                    },
                },
            }

        def mk_create(seq: int, name: str, creation_offset_sec: float):
            return ScheduledWorkflowItem(
                target_wall_offset_sec=0.0,
                seq=seq,
                raw_offset_sec=0.0,
                raw_offset_from_t0_sec=0.0,
                event_timestamp=base_iso,
                object_kind="Deployment",
                name=name,
                namespace="ai-lab",
                local_queue_name="lq-main",
                workload_state="NotAdmitted",
                trigger_reason="initial_not_admitted",
                execution_duration_sec=10.0,
                compressed_duration_sec=10.0,
                indexed_from_execution_time=False,
                pod_count=1,
                manifest=mk_dep_manifest(name, 1),
                creation_epoch=base_ep + creation_offset_sec,
                incarnation_index=0,
            )

        def mk_delete(seq: int, name: str, target_sec: float):
            return ScheduledWorkflowDeletion(
                target_wall_offset_sec=target_sec,
                seq=seq,
                raw_offset_sec=target_sec * 100.0,
                raw_offset_from_t0_sec=target_sec * 100.0,
                event_timestamp=base_iso,
                object_kind="Deployment",
                manifest_kind="Deployment",
                manifest_api_version="apps/v1",
                name=name,
                applied_name=name,
                namespace="ai-lab",
                delete_reason="cancelled",
                incarnation_index=0,
            )

        # Three Deployments are due at t=0 with three different creationTimestamps, so the replay
        # creates one tier at a time and holds the later tiers behind the FIFO gate (up to ~1s each).
        bundle = dc_replace(
            base_b83,
            t0_epoch=base_ep,
            t0_iso=base_iso,
            effective_start_epoch=base_ep,
            effective_start_iso=base_iso,
            t_end_epoch=base_ep + 30.0,
            t_end_iso=base_iso,
            nodes=[],
            initial_pods=[],
            initial_workloads=[],
            initial_deployments=[],
            scheduled_infra_events=[],
            workload_namespaces=["ai-lab"],
            infra_pods_namespace="infra-pods",
            scheduled_workflows=[
                mk_create(1, "first-srv", 0.0),
                mk_create(2, "gated-srv", 5.0),
                mk_create(3, "brief-srv", 10.0),
            ],
            scheduled_workflow_updates=[
                # Due together with the gated create: it must run after that create, not before it.
                ScheduledWorkflowUpdate(
                    target_wall_offset_sec=0.0,
                    seq=4,
                    raw_offset_sec=0.0,
                    raw_offset_from_t0_sec=0.0,
                    event_timestamp=base_iso,
                    object_kind="Deployment",
                    name="gated-srv",
                    applied_name="gated-srv",
                    namespace="ai-lab",
                    local_queue_name="lq-main",
                    incarnation_index=0,
                    replicas=3,
                    paused=False,
                    template_hash="th-0000000000000001",
                    update_types=("ScaleUp",),
                    manifest=mk_dep_manifest("gated-srv", 3),
                ),
            ],
            scheduled_workflow_deletions=[
                # Strictly after the brief-srv create time: the replay must run Create -> Delete.
                mk_delete(5, "brief-srv", 0.01),
                mk_delete(6, "first-srv", 0.02),
                mk_delete(7, "gated-srv", 0.02),
            ],
        )

        mgr = ChamberManager(chamber_namespace="kueue-optimizer-chamber-test-fifo", apiserver_port=16496)
        try:
            actions: list[str] = []

            def fake_apply_parallel(docs, max_workers=16):
                for d in docs:
                    actions.append(f"CREATE:{(d.get('metadata') or {}).get('name')}")

            def fake_ssa(doc):
                actions.append(f"SSA:{(doc.get('metadata') or {}).get('name')}:{(doc.get('spec') or {}).get('replicas')}")

            def fake_api(method: str, path: str, body: bytes | None = None, headers: dict | None = None):
                if method == "DELETE" and "/deployments/" in path:
                    actions.append(f"DELETE:{path.rsplit('/', 1)[-1].split('?', 1)[0]}")
                    return 200, b"{}"
                if method == "GET" and (path.endswith("/workloads") or path.endswith("/pods")):
                    return 200, json.dumps({"items": []}).encode("utf-8")
                return 200, b"{}"

            def fake_q_status(*_a, **_kw):
                return ClusterStatusSnapshot(
                    workloads_admitted=0,
                    workloads_pending=0,
                    workloads_finished=0,
                    pods_running=0,
                    pods_pending=0,
                    pods_succeeded=0,
                    pods_failed=0,
                    jobsets_completed=0,
                    jobsets_failed=0,
                    cq_summary=[],
                    finished_workload_keys=set(),
                    present_workload_keys=set(),
                    workloads_query_ok=True,
                )

            with (
                mock.patch.object(mgr, "ensure_kwok_connection"),
                mock.patch.object(mgr, "apply_manifests_parallel", side_effect=fake_apply_parallel),
                mock.patch.object(mgr, "ssa_apply_doc", side_effect=fake_ssa),
                mock.patch.object(mgr, "api_request", side_effect=fake_api),
                mock.patch("runner.replay.query_cluster_status", side_effect=fake_q_status),
            ):
                run_scenario_replay(
                    mgr=mgr,
                    bundle=bundle,
                    speedup=100.0,
                    max_run_seconds=8.0,
                    status_interval_sec=0.05,
                    wait_for_completion=True,
                    initial_state_already_bootstrapped=True,
                )

            # The gated Deployment is created once with its original spec, and updated only afterwards.
            self.assertEqual(actions.count("CREATE:gated-srv"), 1, actions)
            self.assertIn("SSA:gated-srv:3", actions)
            self.assertLess(actions.index("CREATE:gated-srv"), actions.index("SSA:gated-srv:3"), actions)
            self.assertLess(actions.index("SSA:gated-srv:3"), actions.index("DELETE:gated-srv"), actions)
            # The brief Deployment is created and then deleted, not cancelled before its create.
            self.assertEqual(actions.count("CREATE:brief-srv"), 1, actions)
            self.assertLess(actions.index("CREATE:brief-srv"), actions.index("DELETE:brief-srv"), actions)
            self.assertLess(actions.index("CREATE:first-srv"), actions.index("DELETE:first-srv"), actions)
        finally:
            mgr.close()

    def test_fast_exiting_standalone_pod_sharing_deployment_prefix_is_never_classified_as_serving(self) -> None:
        """A standalone Pod whose name starts with '<deploy>-' must stay non-serving even if deleted before Pod list or finished before _sync_serving_workloads."""
        from dataclasses import replace as dc_replace
        from datetime import datetime, timezone
        import time

        from chamber.lifecycle import ChamberManager, ClusterStatusSnapshot
        from runner.loader import (
            ScheduledWorkflowDeletion,
            ScheduledWorkflowItem,
            load_scenario_bundle,
        )
        from runner.replay import run_scenario_replay

        st83_dir = REPO_ROOT / "testscenarios/self-tests/st83-deployment-scale-down-unblocks-batch-and-zero"
        base_b83 = load_scenario_bundle(st83_dir, speedup=100.0)
        base_ep = time.time() - 10.0
        base_iso = datetime.fromtimestamp(base_ep, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        dep_manifest = {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": "ephemeral", "namespace": "ai-lab", "labels": {"kueue.x-k8s.io/queue-name": "lq-main"}},
            "spec": {
                "replicas": 1,
                "paused": False,
                "template": {
                    "metadata": {"annotations": {"kueue-optimizer.gke.io/deployment-name": "ephemeral"}},
                    "spec": {"containers": [{"name": "c", "image": "pause:3.9"}]},
                },
            },
        }
        pod_manifest = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "ephemeral-fast-exit",
                "namespace": "ai-lab",
                "labels": {"kueue.x-k8s.io/queue-name": "lq-main"},
                "annotations": {"kueue-optimizer.gke.io/pod-execution-time-seconds": "1"},
            },
            "spec": {"restartPolicy": "Never", "containers": [{"name": "c", "image": "pause:3.9"}]},
        }
        fast_pod_wl = {
            "metadata": {
                "name": "pod-ephemeral-fast-exit-abcde",
                "namespace": "ai-lab",
                "ownerReferences": [{"kind": "Pod", "name": "ephemeral-fast-exit"}],
            },
            "spec": {"podSets": [{"name": "main", "count": 1, "template": {"metadata": {}, "spec": {"containers": []}}}]},
            "status": {
                "admission": {"clusterQueue": "cq-main"},
                "conditions": [
                    {"type": "Admitted", "status": "True", "lastTransitionTime": base_iso},
                    {"type": "Finished", "status": "True", "reason": "Succeeded", "lastTransitionTime": base_iso},
                ],
            },
        }

        bundle = dc_replace(
            base_b83,
            t0_epoch=base_ep,
            t0_iso=base_iso,
            effective_start_epoch=base_ep,
            effective_start_iso=base_iso,
            t_end_epoch=base_ep + 10.0,
            t_end_iso=base_iso,
            nodes=[],
            initial_pods=[],
            initial_workloads=[],
            initial_deployments=[],
            scheduled_infra_events=[],
            workload_namespaces=["ai-lab"],
            infra_pods_namespace="infra-pods",
            scheduled_workflows=[
                ScheduledWorkflowItem(
                    target_wall_offset_sec=0.0,
                    seq=1,
                    raw_offset_sec=0.0,
                    raw_offset_from_t0_sec=0.0,
                    event_timestamp=base_iso,
                    object_kind="Deployment",
                    name="ephemeral",
                    namespace="ai-lab",
                    local_queue_name="lq-main",
                    workload_state="NotAdmitted",
                    trigger_reason="initial_not_admitted",
                    execution_duration_sec=10.0,
                    compressed_duration_sec=10.0,
                    indexed_from_execution_time=False,
                    pod_count=1,
                    manifest=dep_manifest,
                    creation_epoch=base_ep,
                    incarnation_index=0,
                ),
                ScheduledWorkflowItem(
                    target_wall_offset_sec=0.0,
                    seq=2,
                    raw_offset_sec=0.0,
                    raw_offset_from_t0_sec=0.0,
                    event_timestamp=base_iso,
                    object_kind="Pod",
                    name="ephemeral-fast-exit",
                    namespace="ai-lab",
                    local_queue_name="lq-main",
                    workload_state="NotAdmitted",
                    trigger_reason="initial_not_admitted",
                    execution_duration_sec=1.0,
                    compressed_duration_sec=0.1,
                    indexed_from_execution_time=True,
                    pod_count=1,
                    manifest=pod_manifest,
                    creation_epoch=base_ep,
                    incarnation_index=0,
                ),
            ],
            scheduled_workflow_updates=[],
            scheduled_workflow_deletions=[
                # Fires right after creation: _execute_workflow_deletion calls query_cluster_status ->
                # _non_serving_finished_keys BEFORE _sync_serving_workloads has ever seen fast_pod_wl.
                ScheduledWorkflowDeletion(
                    target_wall_offset_sec=0.01,
                    seq=3,
                    raw_offset_sec=1.0,
                    raw_offset_from_t0_sec=1.0,
                    event_timestamp=base_iso,
                    object_kind="Deployment",
                    manifest_kind="Deployment",
                    manifest_api_version="apps/v1",
                    name="ephemeral",
                    applied_name="ephemeral",
                    namespace="ai-lab",
                    delete_reason="cancelled",
                    incarnation_index=0,
                ),
            ],
        )

        mgr = ChamberManager(chamber_namespace="kueue-optimizer-chamber-test-fastpod", apiserver_port=16495)
        try:
            def fake_api(method: str, path: str, body: bytes | None = None, headers: dict | None = None):
                if method == "GET" and path.endswith("/workloads"):
                    return 200, json.dumps({"items": [fast_pod_wl]}).encode("utf-8")
                if method == "GET" and path.endswith("/pods"):
                    # Simulate fast-exiting Pod already gone before the first Pod list!
                    return 200, json.dumps({"items": []}).encode("utf-8")
                return 200, b"{}"

            def fake_q_status(*_a, **_kw):
                return ClusterStatusSnapshot(
                    workloads_admitted=0,
                    workloads_pending=0,
                    workloads_finished=1,
                    pods_running=0,
                    pods_pending=0,
                    pods_succeeded=1,
                    pods_failed=0,
                    jobsets_completed=0,
                    jobsets_failed=0,
                    cq_summary=[],
                    finished_workload_keys={("ai-lab", "pod-ephemeral-fast-exit-abcde")},
                    present_workload_keys={("ai-lab", "pod-ephemeral-fast-exit-abcde")},
                    workloads_query_ok=True,
                )

            t_start = time.monotonic()
            with (
                mock.patch.object(mgr, "ensure_kwok_connection"),
                mock.patch.object(mgr, "apply_manifests_parallel"),
                mock.patch.object(mgr, "api_request", side_effect=fake_api),
                mock.patch("runner.replay.query_cluster_status", side_effect=fake_q_status),
            ):
                run_scenario_replay(
                    mgr=mgr,
                    bundle=bundle,
                    speedup=100.0,
                    max_run_seconds=3.0,
                    status_interval_sec=0.05,
                    wait_for_completion=True,
                    initial_state_already_bootstrapped=True,
                )
            elapsed = time.monotonic() - t_start

            # Must NOT be misclassified as a retired serving Deployment workload, and replay must exit
            # promptly upon completion rather than stalling until max_run_seconds=3.0s.
            self.assertNotIn(("ai-lab", "pod-ephemeral-fast-exit-abcde"), mgr._retired_serving_workloads)
            self.assertLess(elapsed, 2.0, f"Replay stalled instead of counting standalone Pod as finished ({elapsed:.2f}s)")
        finally:
            mgr.close()

    def test_st91_to_st95_hashing_anonymization_scenarios_load_and_validate_cleanly(self) -> None:
        import re
        import shutil

        suite_dir = REPO_ROOT / "testscenarios" / "self-tests"
        h_name_re = re.compile(r"^h-[0-9a-f]{32}$")
        th_re = re.compile(r"^th-[0-9a-f]{16}$")

        # 1. st91 (anonymized) vs st92 (plaintext control) exact structural & timing parity.
        b91 = testrunner.load_scenario_bundle(
            suite_dir / "st91-hash-names-jobset-batch-anonymized-replay",
            skip_initial_offset=False,
            missing_duration_policy="error",
        )
        b92 = testrunner.load_scenario_bundle(
            suite_dir / "st92-hash-names-jobset-batch-plaintext-control",
            skip_initial_offset=False,
            missing_duration_policy="error",
        )
        self.assertEqual(len(b91.scheduled_workflows), 8)
        self.assertEqual(len(b92.scheduled_workflows), 8)
        self.assertTrue(all(h_name_re.match(w.name) for w in b91.scheduled_workflows))
        self.assertTrue(all(w.name.startswith("batch-train-") for w in b92.scheduled_workflows))
        self.assertEqual(
            [
                (w.object_kind, w.pod_count, round(w.target_wall_offset_sec, 3), round(w.execution_duration_sec, 3))
                for w in b91.scheduled_workflows
            ],
            [
                (w.object_kind, w.pod_count, round(w.target_wall_offset_sec, 3), round(w.execution_duration_sec, 3))
                for w in b92.scheduled_workflows
            ],
        )

        # 2. st93: hashed t0 JobSet pods separated from hashed non-Kueue infra-pods.
        b93 = testrunner.load_scenario_bundle(
            suite_dir / "st93-hash-names-t0-mixed-workloads-and-infra-pods",
            skip_initial_offset=False,
            missing_duration_policy="error",
        )
        self.assertEqual(len(b93.initial_workloads), 2)
        self.assertEqual(len(b93.initial_pods), 2)
        self.assertEqual(len(b93.scheduled_workflows), 2)
        self.assertTrue(all(h_name_re.match(w.owner_name) for w in b93.initial_workloads))
        self.assertTrue(all(h_name_re.match(p.name) for p in b93.initial_pods))
        self.assertEqual([p.placement for p in b93.initial_pods], ["st-node-02", "st-node-02"])

        # 3. st94: hashed Deployment salted Upgrade + ScaleDown 4 -> 2 -> 0 + hashed batch Job.
        b94 = testrunner.load_scenario_bundle(
            suite_dir / "st94-hash-names-deployment-salted-upgrade-and-scale",
            skip_initial_offset=False,
            missing_duration_policy="error",
        )
        self.assertEqual(len(b94.scheduled_workflows), 2)
        self.assertEqual([w.object_kind for w in b94.scheduled_workflows], ["Deployment", "Job"])
        self.assertTrue(all(h_name_re.match(w.name) for w in b94.scheduled_workflows))
        dep_doc = b94.scheduled_workflows[0].manifest
        sel_app = dep_doc["spec"]["selector"]["matchLabels"]["app"]
        tpl_app = dep_doc["spec"]["template"]["metadata"]["labels"]["app"]
        self.assertEqual(sel_app, tpl_app)
        self.assertRegex(sel_app, h_name_re)
        v1_hash = dep_doc["spec"]["template"]["metadata"]["annotations"]["kueue-optimizer.gke.io/template-hash"]
        self.assertRegex(v1_hash, th_re)
        self.assertEqual(len(b94.scheduled_workflow_updates), 3)
        self.assertEqual(
            [u.update_types for u in b94.scheduled_workflow_updates],
            [("Upgrade",), ("ScaleDown",), ("ScaleDown",)],
        )
        self.assertEqual([u.replicas for u in b94.scheduled_workflow_updates], [4, 2, 0])
        v2_hash = b94.scheduled_workflow_updates[0].template_hash
        self.assertRegex(v2_hash, th_re)
        self.assertNotEqual(v1_hash, v2_hash)

        # 4. st95: hashed 7-level TAS + annotation-only t0 PodGroups + LowerPriority preemption.
        b95 = testrunner.load_scenario_bundle(
            suite_dir / "st95-hash-names-tas-podgroup-annotation-and-preemption",
            skip_initial_offset=False,
            missing_duration_policy="error",
        )
        self.assertEqual(len(b95.initial_workloads), 2)
        self.assertEqual(len(b95.initial_pods), 0)
        self.assertEqual(len(b95.scheduled_workflows), 3)
        self.assertTrue(all(w.is_tas and w.owner_kind == "PodGroup" for w in b95.initial_workloads))
        self.assertTrue(all(h_name_re.match(w.owner_name) for w in b95.initial_workloads))
        self.assertTrue(all(h_name_re.match(w.name) for w in b95.scheduled_workflows))

        # 5. execution-time.json is the only duration source: no workflow carries the replay-only
        #    pod duration annotation, so every finite workflow must resolve through its hashed owner name.
        for b_chk in (b91, b92, b93, b94, b95):
            finite = [w for w in b_chk.scheduled_workflows if w.object_kind != "Deployment"]
            self.assertTrue(finite)
            self.assertFalse(any(w.has_explicit_pod_exec_annotation for w in finite))
            self.assertTrue(all(w.indexed_from_execution_time for w in finite))

        # A hashed owner name that execution-time.json does not contain must fail under policy "error".
        with tempfile.TemporaryDirectory() as tmp:
            broken_dir = Path(tmp) / "st91-broken-owner-link"
            shutil.copytree(suite_dir / "st91-hash-names-jobset-batch-anonymized-replay", broken_dir)
            ex_path = broken_dir / "execution-time.json"
            broken_lines = []
            for line in ex_path.read_text(encoding="utf-8").splitlines():
                ev = json.loads(line)
                ev["payload"]["ownerName"] = "h-" + "0" * 32
                broken_lines.append(json.dumps(ev))
            ex_path.write_text("\n".join(broken_lines) + "\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "no recorded execution duration"):
                testrunner.load_scenario_bundle(
                    broken_dir,
                    skip_initial_offset=False,
                    missing_duration_policy="error",
                )

        # 6. Zero raw sensitive strings leaked into any JSONL trace of st91, st93, st94, st95.
        forbidden_tokens = [
            "alice-researcher",
            "ml-core",
            "secret-llm-v4",
            "cc-999",
            "serving-sre",
            "llm-serving-prod",
            "batch-train-",
            "t0-train-",
            "unmanaged-daemon-",
            "infer-gateway",
            "offline-eval-",
            "t0-tas-pg-",
            "tas-low-job-",
            "tas-high-urgent-",
            "API_KEY",
        ]
        for slug in (
            "st91-hash-names-jobset-batch-anonymized-replay",
            "st93-hash-names-t0-mixed-workloads-and-infra-pods",
            "st94-hash-names-deployment-salted-upgrade-and-scale",
            "st95-hash-names-tas-podgroup-annotation-and-preemption",
        ):
            for fname in ("infra.json", "workflows.json", "execution-time.json"):
                raw = (suite_dir / slug / fname).read_text(encoding="utf-8")
                for tok in forbidden_tokens:
                    self.assertNotIn(tok, raw, f"Sensitive token {tok!r} leaked into {slug}/{fname}")


if __name__ == "__main__":
    unittest.main()
