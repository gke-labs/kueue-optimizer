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


if __name__ == "__main__":
    unittest.main()
