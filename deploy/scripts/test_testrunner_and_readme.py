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
    build_cluster_queue_manifest,
    extract_workflow_pods_and_requests,
    format_pod_execution_seconds,
    index_execution_times,
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

            with mock.patch("chamber_common.subprocess.run", side_effect=fake_run):
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


if __name__ == "__main__":
    unittest.main()
