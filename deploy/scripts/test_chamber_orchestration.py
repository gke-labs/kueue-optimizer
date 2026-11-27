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

"""Unit tests for chamber orchestration fixes (C1-C10): recovery, Prometheus, flags, PKI, YAML, signals, Docker."""

from __future__ import annotations

import base64
from contextlib import redirect_stderr, redirect_stdout
import io
import json
import math
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from chamber import lifecycle as lc
from chamber.cli import managed_chamber
from chamber.constants import DEFAULT_APISERVER_PORT, PKI_REQUIRED_KEYS
from chamber.lifecycle import (
    ChamberManager,
    _handle_termination_signal,
    install_termination_signal_handlers,
    restore_termination_signal_handlers,
    termination_signal_handlers,
)
from chamber.parsers import (
    DockerUnavailableError,
    compute_prometheus_port,
    compute_registry_port,
    docker_image_exists_locally,
    parse_duration_seconds,
)
from chamber.pki import ensure_pki_dir, fetch_pki_from_cluster
from chamber.yaml_utils import _yaml_string, docs_to_pretty_yaml, to_clean_yaml
import chamber_common
import demo
from metrics.prometheus import PrometheusClient


class FakeClock:
    def __init__(self, start: float = 1_000_000.0) -> None:
        self.t = start

    def time(self) -> float:
        return self.t

    def sleep(self, sec: float) -> None:
        self.t += sec


class KueueFeatureGateRecoverySelectorTests(unittest.TestCase):
    """C1: feature-gate crash recovery selects actual Kueue pod labels from deploy/base/kueue.yaml."""

    def test_fg_recovery_selector_matches_kueue_deployment_and_recovers(self) -> None:
        with tempfile.TemporaryDirectory(prefix="kueue-opt-c1-") as tmp_str:
            tmp_dir = Path(tmp_str)
            mgr = ChamberManager(
                chamber_namespace="kueue-optimizer-chamber-c1",
                kubeconfig_out=tmp_dir / "kc",
                kueue_feature_gates="UnknownNewGate=true",
                kueue_image="us-central1-docker.pkg.dev/k8s-staging-images/kueue/kueue:main",
            )
            try:
                base_dst = mgr.prepare_tmp_deploy_dir(prefer_cluster_pki=False, check_cluster_ports=False)
                rendered = subprocess.run(
                    ["kubectl", "--kubeconfig=/dev/null", "kustomize", str(base_dst)],
                    check=True,
                    text=True,
                    capture_output=True,
                ).stdout

                # Parse rendered Deployment manifests offline via kubectl --local (no third-party YAML lib).
                dep_docs = [
                    doc
                    for doc in rendered.split("\n---\n")
                    if "\nkind: Deployment\n" in f"\n{doc}\n"
                ]
                self.assertTrue(dep_docs, "Expected at least one Deployment in rendered chamber manifests")
                deployments_file = tmp_dir / "deployments.yaml"
                deployments_file.write_text("\n---\n".join(dep_docs) + "\n", encoding="utf-8")
                dep_json = subprocess.run(
                    [
                        "kubectl",
                        "--kubeconfig=/dev/null",
                        "annotate",
                        "--local",
                        "-f",
                        str(deployments_file),
                        "test=1",
                        "-o",
                        "json",
                    ],
                    check=True,
                    text=True,
                    capture_output=True,
                ).stdout
                decoder = json.JSONDecoder()
                deployments: list[dict[str, object]] = []
                idx = 0
                raw_len = len(dep_json)
                while idx < raw_len:
                    while idx < raw_len and dep_json[idx].isspace():
                        idx += 1
                    if idx >= raw_len:
                        break
                    obj, idx = decoder.raw_decode(dep_json, idx)
                    if isinstance(obj, dict):
                        if obj.get("kind") == "List":
                            deployments.extend(obj.get("items") or [])
                        else:
                            deployments.append(obj)

                seen_selector: list[str] = []
                get_dep_calls = 0
                applied_cmds: list[list[str]] = []

                def fake_host_kubectl(args: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
                    nonlocal get_dep_calls
                    if args[2:5] == ["get", "deployment", "kueue-controller-manager"]:
                        get_dep_calls += 1
                        if get_dep_calls == 1:
                            dep_body = {
                                "metadata": {"generation": 2},
                                "spec": {"replicas": 1},
                                "status": {"observedGeneration": 2, "updatedReplicas": 1, "availableReplicas": 0, "readyReplicas": 0},
                            }
                        else:
                            dep_body = {
                                "metadata": {"generation": 3},
                                "spec": {"replicas": 1},
                                "status": {"observedGeneration": 3, "updatedReplicas": 1, "availableReplicas": 1, "readyReplicas": 1},
                            }
                        return subprocess.CompletedProcess(args, 0, stdout=json.dumps(dep_body), stderr="")
                    if args[2:4] == ["get", "pods"] and "-l" in args:
                        sel = args[args.index("-l") + 1]
                        seen_selector.append(sel)
                        pod_list = {
                            "items": [
                                {
                                    "metadata": {"name": "kueue-controller-manager-abc"},
                                    "status": {
                                        "containerStatuses": [
                                            {
                                                "state": {"waiting": {"reason": "CrashLoopBackOff"}},
                                                "lastState": {"terminated": {"exitCode": 1}},
                                            }
                                        ]
                                    },
                                }
                            ]
                        }
                        return subprocess.CompletedProcess(args, 0, stdout=json.dumps(pod_list), stderr="")
                    if args[2:4] == ["logs", "kueue-controller-manager-abc"]:
                        return subprocess.CompletedProcess(
                            args,
                            0,
                            stdout="ERROR setup failed: unrecognized feature gate: UnknownNewGate\n",
                            stderr="",
                        )
                    applied_cmds.append(list(args))
                    return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

                with (
                    mock.patch.object(mgr, "host_kubectl", side_effect=fake_host_kubectl),
                    mock.patch.object(lc.time, "sleep"),
                    redirect_stdout(io.StringIO()),
                ):
                    mgr._wait_for_kueue_rollout_with_fg_recovery(base_dst, {"UnknownNewGate": True})

                self.assertEqual(len(seen_selector), 1)
                sel_k, sel_v = seen_selector[0].split("=", 1)
                matching_deps = [
                    d["metadata"]["name"]
                    for d in deployments
                    if (((d.get("spec") or {}).get("template") or {}).get("metadata") or {})
                    .get("labels", {})
                    .get(sel_k)
                    == sel_v
                ]
                self.assertEqual(matching_deps, ["kueue-controller-manager"])
                # Must have re-applied patched config and issued rollout restart.
                self.assertTrue(any(cmd[0] == "apply" for cmd in applied_cmds))
                self.assertTrue(any("restart" in cmd for cmd in applied_cmds))
                cfg_text = (base_dst / "config" / "controller_manager_config.yaml").read_text(encoding="utf-8")
                self.assertNotIn("UnknownNewGate", cfg_text)
            finally:
                mgr.close()


class PrometheusScrapeWaitAndIsolationTests(unittest.TestCase):
    """C2 & C3: duration parsing in wait_for_prometheus_scrape + port isolation & env precedence."""

    def test_wait_for_prometheus_scrape_respects_duration_units(self) -> None:
        self.assertAlmostEqual(parse_duration_seconds("500ms") or 0.0, 0.5)
        self.assertAlmostEqual(parse_duration_seconds("1m") or 0.0, 60.0)
        self.assertAlmostEqual(parse_duration_seconds("1m30s") or 0.0, 90.0)

        cases = [
            ("10s", 15.0),
            ("30s", 35.0),
            ("1m", 65.0),
            ("2m", 125.0),
            ("500ms", 5.5),
            ("0.5s", 5.5),
            ("1m30s", 95.0),
        ]
        for interval, expected_wait in cases:
            with self.subTest(interval=interval):
                with tempfile.TemporaryDirectory(prefix="kueue-opt-c2-") as tmp_str:
                    mgr = ChamberManager(
                        chamber_namespace="kueue-optimizer-chamber-c2",
                        scrape_interval=interval,
                        kubeconfig_out=Path(tmp_str) / "kc",
                    )
                    clock = FakeClock()
                    fake_opener = mock.Mock()
                    fake_opener.open.side_effect = OSError("no local prometheus")
                    stale = subprocess.CompletedProcess(
                        [], 0, stdout='{"data":{"result":[{"value":[0,"999000"]}]}}', stderr=""
                    )
                    try:
                        with (
                            mock.patch.object(mgr, "ensure_prometheus_connection"),
                            mock.patch.object(mgr, "host_kubectl", return_value=stale),
                            mock.patch.object(lc.urllib.request, "build_opener", return_value=fake_opener),
                            mock.patch.object(lc.time, "time", clock.time),
                            mock.patch.object(lc.time, "sleep", clock.sleep),
                        ):
                            t0 = clock.t
                            mgr.wait_for_prometheus_scrape(target_epoch=t0)
                            self.assertAlmostEqual(clock.t - t0, expected_wait, places=3)
                    finally:
                        mgr.close()

    def test_env_port_overrides_do_not_clobber_per_chamber_derived_ports(self) -> None:
        with mock.patch.dict(
            os.environ,
            {"LOCAL_PROMETHEUS_PORT": "9090", "LOCAL_REGISTRY_PORT": "30500"},
            clear=False,
        ):
            # Default apiserver port still respects legacy env overrides.
            self.assertEqual(compute_prometheus_port(DEFAULT_APISERVER_PORT), 9090)
            self.assertEqual(compute_registry_port(DEFAULT_APISERVER_PORT), 30500)

            # Distinct per-chamber --port values derive isolated ports instead of colliding.
            p_a = compute_prometheus_port(16451)
            p_b = compute_prometheus_port(16452)
            self.assertEqual(p_a, 19098)
            self.assertEqual(p_b, 19099)
            self.assertNotEqual(p_a, p_b)

            r_a = compute_registry_port(16451)
            r_b = compute_registry_port(16452)
            self.assertEqual(r_a, 30508)
            self.assertEqual(r_b, 30509)
            self.assertNotEqual(r_a, r_b)

            # Explicit port arguments always take precedence.
            self.assertEqual(compute_prometheus_port(16451, explicit_prom_port=19555), 19555)
            self.assertEqual(compute_registry_port(16451, explicit_registry_port=31111), 31111)

        # High apiserver ports stay within valid TCP port bounds.
        wrapped_prom = compute_prometheus_port(63000)
        self.assertGreaterEqual(wrapped_prom, 1024)
        self.assertLessEqual(wrapped_prom, 65535)

    def test_ensure_prometheus_connection_does_not_adopt_foreign_listener(self) -> None:
        # 1. Explicit --prometheus-port occupied by another listener -> must refuse instead of silently adopting.
        with tempfile.TemporaryDirectory(prefix="kueue-opt-c3a-") as tmp_str:
            mgr_explicit = ChamberManager(
                chamber_namespace="kueue-optimizer-chamber-arm-b",
                apiserver_port=16452,
                prometheus_port=19099,
                kubeconfig_out=Path(tmp_str) / "kc",
            )
            try:
                with (
                    mock.patch.object(mgr_explicit, "_pkill_chamber_port_forward", return_value=False),
                    mock.patch.object(mgr_explicit, "_is_local_tcp_port_listening", return_value=False),
                    mock.patch.object(mgr_explicit, "_probe_prometheus_ready", return_value=True),
                    mock.patch.object(lc.subprocess, "Popen") as m_popen,
                ):
                    with self.assertRaisesRegex(RuntimeError, "already in use by another process"):
                        mgr_explicit.ensure_prometheus_connection()
                    m_popen.assert_not_called()
            finally:
                mgr_explicit.close()

        # 2. Derived port occupied by another listener -> allocates a free local port and owns its own Popen.
        with tempfile.TemporaryDirectory(prefix="kueue-opt-c3b-") as tmp_str:
            mgr_derived = ChamberManager(
                chamber_namespace="kueue-optimizer-chamber-arm-b",
                apiserver_port=16452,
                kubeconfig_out=Path(tmp_str) / "kc",
            )
            orig_port = mgr_derived.prometheus_port
            fake_proc = mock.Mock()
            fake_proc.poll.return_value = None
            try:
                with (
                    mock.patch.object(mgr_derived, "_pkill_chamber_port_forward", return_value=False),
                    mock.patch.object(
                        mgr_derived,
                        "_is_local_tcp_port_listening",
                        side_effect=lambda p: p == orig_port,
                    ),
                    mock.patch.object(mgr_derived, "_allocate_free_local_port", return_value=45123),
                    mock.patch.object(
                        mgr_derived,
                        "_probe_prometheus_ready",
                        side_effect=lambda p, timeout=1.5: p == 45123,
                    ),
                    mock.patch.object(lc.subprocess, "Popen", return_value=fake_proc) as m_popen,
                    redirect_stdout(io.StringIO()),
                ):
                    mgr_derived.ensure_prometheus_connection()
                    self.assertEqual(mgr_derived.prometheus_port, 45123)
                    m_popen.assert_called_once()
                    cmd = m_popen.call_args[0][0]
                    self.assertIn("45123:9090", cmd)
                    self.assertIn("kueue-optimizer-chamber-arm-b", cmd)

                    # Second call reuses our own live port-forward without spawning a second process.
                    mgr_derived.ensure_prometheus_connection()
                    m_popen.assert_called_once()
            finally:
                mgr_derived._prom_pf_proc = None
                mgr_derived.close()

    def test_ensure_prometheus_connection_waits_for_pkill_teardown_on_explicit_port(self) -> None:
        with tempfile.TemporaryDirectory(prefix="kueue-opt-c3-pkill-") as tmp_str:
            mgr = ChamberManager(
                chamber_namespace="kueue-optimizer-chamber-c3-pkill",
                apiserver_port=16452,
                prometheus_port=19099,
                kubeconfig_out=Path(tmp_str) / "kc",
            )
            fake_proc = mock.Mock()
            fake_proc.poll.return_value = None
            # Port stays listening for 2 teardown checks after pkill, then clears before occupation check.
            listening_seq = iter([True, True, False, False])
            probe_seq = iter([False, True])
            try:
                with (
                    mock.patch.object(mgr, "_pkill_chamber_port_forward", return_value=True),
                    mock.patch.object(mgr, "_is_local_tcp_port_listening", side_effect=lambda _p: next(listening_seq)),
                    mock.patch.object(mgr, "_probe_prometheus_ready", side_effect=lambda _p, timeout=1.5: next(probe_seq)),
                    mock.patch.object(lc.subprocess, "Popen", return_value=fake_proc) as m_popen,
                    mock.patch.object(lc.time, "sleep") as m_sleep,
                ):
                    mgr.ensure_prometheus_connection()
                    self.assertEqual(mgr.prometheus_port, 19099)
                    m_popen.assert_called_once()
                    self.assertGreaterEqual(m_sleep.call_count, 2)
            finally:
                mgr._prom_pf_proc = None
                mgr.close()

    def test_ensure_prometheus_connection_retries_early_exit_and_raises_on_exhaustion(self) -> None:
        # 1. Derived port early exit on attempt 1 -> reaps dead proc and succeeds on fresh ephemeral port.
        with tempfile.TemporaryDirectory(prefix="kueue-opt-c3-retry-") as tmp_str:
            mgr = ChamberManager(
                chamber_namespace="kueue-optimizer-chamber-c3-retry",
                apiserver_port=16453,
                kubeconfig_out=Path(tmp_str) / "kc",
            )
            dead_proc = mock.Mock()
            dead_proc.poll.return_value = 1
            live_proc = mock.Mock()
            live_proc.poll.return_value = None
            try:
                with (
                    mock.patch.object(mgr, "_pkill_chamber_port_forward", return_value=False),
                    mock.patch.object(mgr, "_is_local_tcp_port_listening", return_value=False),
                    mock.patch.object(mgr, "_allocate_free_local_port", return_value=45201),
                    mock.patch.object(
                        mgr,
                        "_probe_prometheus_ready",
                        side_effect=lambda p, timeout=1.5: p == 45201,
                    ),
                    mock.patch.object(lc.subprocess, "Popen", side_effect=[dead_proc, live_proc]) as m_popen,
                    mock.patch.object(lc.time, "sleep"),
                    redirect_stdout(io.StringIO()),
                ):
                    mgr.ensure_prometheus_connection()
                    self.assertEqual(mgr.prometheus_port, 45201)
                    self.assertEqual(m_popen.call_count, 2)
                    dead_proc.wait.assert_called_once()
                    self.assertIs(mgr._prom_pf_proc, live_proc)
            finally:
                mgr._prom_pf_proc = None
                mgr.close()

        # 2. Derived port: foreign listener answers just as our Popen dies on all 3 attempts -> must raise & reap.
        with tempfile.TemporaryDirectory(prefix="kueue-opt-c3-race-") as tmp_str:
            mgr = ChamberManager(
                chamber_namespace="kueue-optimizer-chamber-c3-race",
                apiserver_port=16454,
                kubeconfig_out=Path(tmp_str) / "kc",
            )
            race_procs: list[mock.Mock] = []
            for _ in range(3):
                p = mock.Mock()
                # Alive before probe, exited right after foreign listener answered probe, still exited during reap.
                p.poll.side_effect = [None, 1, 1]
                race_procs.append(p)
            try:
                with (
                    mock.patch.object(mgr, "_pkill_chamber_port_forward", return_value=False),
                    mock.patch.object(mgr, "_is_local_tcp_port_listening", return_value=False),
                    mock.patch.object(mgr, "_allocate_free_local_port", side_effect=[45301, 45302]),
                    mock.patch.object(mgr, "_probe_prometheus_ready", side_effect=[False, True, True, True]),
                    mock.patch.object(lc.subprocess, "Popen", side_effect=race_procs) as m_popen,
                    mock.patch.object(lc.time, "sleep"),
                    redirect_stdout(io.StringIO()),
                ):
                    with self.assertRaisesRegex(RuntimeError, "after 3 attempt"):
                        mgr.ensure_prometheus_connection()
                    self.assertEqual(m_popen.call_count, 3)
                    self.assertIsNone(mgr._prom_pf_proc)
                    for p in race_procs:
                        p.wait.assert_called_once()
            finally:
                mgr.close()

        # 3. Explicit --prometheus-port timing out without readiness -> terminates, reaps, and raises RuntimeError.
        with tempfile.TemporaryDirectory(prefix="kueue-opt-c3-timeout-") as tmp_str:
            mgr = ChamberManager(
                chamber_namespace="kueue-optimizer-chamber-c3-timeout",
                apiserver_port=16455,
                prometheus_port=19155,
                kubeconfig_out=Path(tmp_str) / "kc",
            )
            hung_proc = mock.Mock()
            hung_proc.poll.return_value = None
            try:
                with (
                    mock.patch.object(mgr, "_pkill_chamber_port_forward", return_value=False),
                    mock.patch.object(mgr, "_is_local_tcp_port_listening", return_value=False),
                    mock.patch.object(mgr, "_probe_prometheus_ready", return_value=False),
                    mock.patch.object(lc.subprocess, "Popen", return_value=hung_proc) as m_popen,
                    mock.patch.object(lc.time, "sleep"),
                ):
                    with self.assertRaisesRegex(RuntimeError, "after 1 attempt"):
                        mgr.ensure_prometheus_connection()
                    m_popen.assert_called_once()
                    hung_proc.terminate.assert_called_once()
                    hung_proc.wait.assert_called_once()
                    self.assertIsNone(mgr._prom_pf_proc)
                    self.assertEqual(mgr.prometheus_port, 19155)
            finally:
                mgr.close()

    def test_prometheus_client_never_adopts_foreign_default_ports(self) -> None:
        probed_urls: list[str] = []
        fake_proc = mock.Mock()
        fake_proc.poll.return_value = None

        def fake_is_ready(_self: PrometheusClient, base_url: str, timeout: float = 1.5) -> bool:
            probed_urls.append(base_url)
            return True

        with (
            mock.patch.object(PrometheusClient, "_is_url_ready", autospec=True, side_effect=fake_is_ready),
            mock.patch("metrics.prometheus.subprocess.Popen", return_value=fake_proc) as m_popen,
            mock.patch("metrics.prometheus.atexit.register"),
        ):
            client = PrometheusClient(prometheus_url=None, namespace="kueue-optimizer-chamber-arm-b")
            try:
                m_popen.assert_called_once()
                cmd = m_popen.call_args[0][0]
                self.assertEqual(cmd[:5], ["kubectl", "-n", "kueue-optimizer-chamber-arm-b", "port-forward", "svc/prometheus"])
                self.assertNotIn("http://127.0.0.1:19090", probed_urls)
                self.assertNotIn("http://127.0.0.1:9090", probed_urls)
                self.assertNotIn(client.prometheus_url, ("http://127.0.0.1:19090", "http://127.0.0.1:9090"))
                self.assertIsNotNone(client.prometheus_url)
            finally:
                client.close()

        # If the spawned port-forward dies early, prometheus_url is cleared (never stale/foreign) and proc is reaped.
        dead_proc = mock.Mock()
        dead_proc.poll.return_value = 1
        with (
            mock.patch.object(PrometheusClient, "_is_url_ready", return_value=False),
            mock.patch("metrics.prometheus.subprocess.Popen", return_value=dead_proc),
            mock.patch("metrics.prometheus.atexit.register"),
        ):
            client_dead = PrometheusClient(
                prometheus_url="http://127.0.0.1:19999",
                namespace="kueue-optimizer-chamber-arm-b",
            )
            self.assertIsNone(client_dead.prometheus_url)
            self.assertIsNone(client_dead._pf_proc)
            dead_proc.wait.assert_called_once()


class DemoFlagForwardingTests(unittest.TestCase):
    """C4: demo.py forwards fair-sharing strategies, AFS flags, and feature gates to ChamberManager."""

    def test_demo_main_forwards_controller_config_flags(self) -> None:
        captured: dict[str, object] = {}
        real_cm = demo.ChamberManager

        def spy(*args: object, **kwargs: object) -> ChamberManager:
            captured.update(kwargs)
            return real_cm(*args, **kwargs)  # type: ignore[arg-type]

        argv = [
            "--dry-run",
            "--namespace",
            "kueue-optimizer-chamber-demo-c4",
            "--fair-sharing-strategies",
            "LessThanInitialShare",
            "--admission-fair-sharing",
            "--afs-usage-half-life",
            "20s",
            "--afs-sampling-interval",
            "2s",
            "--afs-resource-weights",
            "cpu=1,memory=0",
            "--kueue-feature-gates",
            "TASBalancedPlacement=true",
        ]
        with mock.patch.object(demo, "ChamberManager", side_effect=spy), redirect_stdout(io.StringIO()):
            rc = demo.main(argv)

        self.assertEqual(rc, 0)
        self.assertEqual(captured.get("fair_sharing_strategies"), ("LessThanInitialShare",))
        self.assertEqual(captured.get("kueue_feature_gates"), {"TASBalancedPlacement": True})
        afs = captured.get("admission_fair_sharing")
        self.assertIsNotNone(afs)
        self.assertTrue(afs.enabled)  # type: ignore[union-attr]
        self.assertAlmostEqual(afs.raw_usage_half_life_sec, 20.0)  # type: ignore[union-attr]
        self.assertAlmostEqual(afs.effective_usage_half_life_sec, 20.0)  # type: ignore[union-attr]
        self.assertAlmostEqual(afs.raw_usage_sampling_interval_sec, 2.0)  # type: ignore[union-attr]
        self.assertAlmostEqual(afs.effective_usage_sampling_interval_sec, 2.0)  # type: ignore[union-attr]
        self.assertEqual(afs.resource_weights, {"cpu": 1.0, "memory": 0.0})  # type: ignore[union-attr]

    def test_demo_rejects_controller_flags_with_skip_chamber_deploy(self) -> None:
        err = io.StringIO()
        with redirect_stderr(err), redirect_stdout(io.StringIO()):
            with self.assertRaises(SystemExit) as ctx:
                demo.main(
                    [
                        "--skip-chamber-deploy",
                        "--kueue-feature-gates",
                        "TASBalancedPlacement=true",
                    ]
                )
        self.assertEqual(ctx.exception.code, 2)
        self.assertIn("--skip-chamber-deploy", err.getvalue())


class MetricsReportExitCodeTests(unittest.TestCase):
    """C5: run_kueue_metrics_report propagates non-zero exit codes from kueue_metrics_report.py."""

    def test_run_kueue_metrics_report_raises_on_failure_and_cleans_snapshot(self) -> None:
        with tempfile.TemporaryDirectory(prefix="kueue-opt-c5-") as tmp_str:
            mgr = ChamberManager(
                chamber_namespace="kueue-optimizer-chamber-c5",
                kubeconfig_out=Path(tmp_str) / "kc",
            )
            seen_snap_paths: list[Path] = []

            def fake_run(cmd: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
                if "--cluster-snapshot-json" in cmd:
                    seen_snap_paths.append(Path(cmd[cmd.index("--cluster-snapshot-json") + 1]))
                return subprocess.CompletedProcess(cmd, returncode=2)

            try:
                with (
                    mock.patch.object(mgr, "capture_cluster_saturation_snapshot", return_value={"nodes": []}),
                    mock.patch.object(mgr, "ensure_prometheus_connection"),
                    mock.patch.object(lc.subprocess, "run", side_effect=fake_run),
                ):
                    with self.assertRaisesRegex(RuntimeError, "exit code 2"):
                        mgr.run_kueue_metrics_report(
                            queueing_strategy="StrictFIFO",
                            start_epoch=100.0,
                            end_epoch=200.0,
                            jobset_count=1,
                            total_pods=4,
                            wait_for_scrape=False,
                        )
                self.assertEqual(len(seen_snap_paths), 1)
                self.assertFalse(seen_snap_paths[0].exists())
            finally:
                mgr.close()


class PkiErrorHandlingTests(unittest.TestCase):
    """C6: Secret/kwok-pki only falls back to ad-hoc generation on genuine NotFound."""

    def test_not_found_returns_false_and_allows_adhoc_fallback(self) -> None:
        not_found = subprocess.CompletedProcess(
            ["kubectl"],
            returncode=1,
            stdout="",
            stderr='Error from server (NotFound): secrets "kwok-pki" not found\n',
        )
        with tempfile.TemporaryDirectory(prefix="kueue-opt-c6-nf-") as tmp_str:
            pki_dir = Path(tmp_str) / "pki"
            with mock.patch("chamber.pki.subprocess.run", return_value=not_found):
                self.assertFalse(fetch_pki_from_cluster("kueue-optimizer-chamber-c6", pki_dir))

            with (
                mock.patch("chamber.pki.fetch_pki_from_cluster", return_value=False),
                mock.patch("chamber.pki.generate_adhoc_pki") as m_gen,
            ):
                _, source = ensure_pki_dir("kueue-optimizer-chamber-c6", pki_dir, prefer_cluster=True)
                self.assertEqual(source, "adhoc")
                m_gen.assert_called_once()

    def test_transient_timeout_rbac_and_corrupt_secret_raise(self) -> None:
        with tempfile.TemporaryDirectory(prefix="kueue-opt-c6-err-") as tmp_str:
            pki_dir = Path(tmp_str) / "pki"

            timeout_proc = subprocess.CompletedProcess(
                ["kubectl"],
                returncode=1,
                stdout="",
                stderr="Unable to connect to the server: dial tcp 10.1.2.3:443: i/o timeout\n",
            )
            with mock.patch("chamber.pki.subprocess.run", return_value=timeout_proc):
                with self.assertRaisesRegex(RuntimeError, "i/o timeout"):
                    fetch_pki_from_cluster("kueue-optimizer-chamber-c6", pki_dir)

            rbac_proc = subprocess.CompletedProcess(
                ["kubectl"],
                returncode=1,
                stdout="",
                stderr='Error from server (Forbidden): secrets "kwok-pki" is forbidden\n',
            )
            with mock.patch("chamber.pki.subprocess.run", return_value=rbac_proc):
                with self.assertRaisesRegex(RuntimeError, "Forbidden"):
                    fetch_pki_from_cluster("kueue-optimizer-chamber-c6", pki_dir)

            with mock.patch(
                "chamber.pki.subprocess.run",
                side_effect=subprocess.TimeoutExpired(["kubectl"], 15.0),
            ):
                with self.assertRaisesRegex(RuntimeError, "Secret/kwok-pki"):
                    fetch_pki_from_cluster("kueue-optimizer-chamber-c6", pki_dir)

            bad_json_proc = subprocess.CompletedProcess(["kubectl"], returncode=0, stdout="{not-json", stderr="")
            with mock.patch("chamber.pki.subprocess.run", return_value=bad_json_proc):
                with self.assertRaisesRegex(RuntimeError, "parse Secret/kwok-pki JSON"):
                    fetch_pki_from_cluster("kueue-optimizer-chamber-c6", pki_dir)

            incomplete_proc = subprocess.CompletedProcess(
                ["kubectl"],
                returncode=0,
                stdout=json.dumps({"data": {"ca.crt": base64.b64encode(b"x").decode("ascii")}}),
                stderr="",
            )
            with mock.patch("chamber.pki.subprocess.run", return_value=incomplete_proc):
                with self.assertRaisesRegex(RuntimeError, "missing required keys"):
                    fetch_pki_from_cluster("kueue-optimizer-chamber-c6", pki_dir)

    def test_valid_cluster_secret_succeeds(self) -> None:
        secret_data = {k: base64.b64encode(f"val-{k}".encode("utf-8")).decode("ascii") for k in PKI_REQUIRED_KEYS}
        ok_proc = subprocess.CompletedProcess(
            ["kubectl"],
            returncode=0,
            stdout=json.dumps({"data": secret_data}),
            stderr="",
        )
        with tempfile.TemporaryDirectory(prefix="kueue-opt-c6-ok-") as tmp_str:
            pki_dir = Path(tmp_str) / "pki"
            with mock.patch("chamber.pki.subprocess.run", return_value=ok_proc):
                self.assertTrue(fetch_pki_from_cluster("kueue-optimizer-chamber-c6", pki_dir))
            for k in PKI_REQUIRED_KEYS:
                self.assertEqual((pki_dir / k).read_text(encoding="utf-8"), f"val-{k}")
            self.assertTrue((pki_dir / "token.csv").is_file())

    def test_non_dict_empty_values_corrupt_base64_and_atomic_no_partial_write(self) -> None:
        valid_data = {k: base64.b64encode(f"val-{k}".encode("utf-8")).decode("ascii") for k in PKI_REQUIRED_KEYS}

        bad_cases: list[tuple[str, str, str]] = [
            ("non_dict_root", "[]", "must be an object"),
            ("non_dict_data", json.dumps({"data": ["not", "a", "dict"]}), "missing or non-object 'data'"),
            ("null_data", json.dumps({"data": None}), "missing or non-object 'data'"),
            ("empty_required_key", json.dumps({"data": {**valid_data, "ca.crt": ""}}), "empty or non-string"),
            ("whitespace_required_key", json.dumps({"data": {**valid_data, "admin.key": "   "}}), "empty or non-string"),
            ("non_string_required_key", json.dumps({"data": {**valid_data, "ca.key": 123}}), "empty or non-string"),
            ("corrupt_required_b64", json.dumps({"data": {**valid_data, "admin.crt": "not!valid@base64"}}), "base64-decode"),
            (
                "corrupt_token_csv_b64",
                json.dumps({"data": {**valid_data, "token.csv": "not!valid@base64"}}),
                "base64-decode key 'token.csv'",
            ),
            (
                "non_string_token_csv",
                json.dumps({"data": {**valid_data, "token.csv": 42}}),
                "non-string value for key 'token.csv'",
            ),
        ]
        for label, payload, err_regex in bad_cases:
            with self.subTest(case=label):
                with tempfile.TemporaryDirectory(prefix=f"kueue-opt-c6-{label}-") as tmp_str:
                    pki_dir = Path(tmp_str) / "pki"
                    proc = subprocess.CompletedProcess(["kubectl"], returncode=0, stdout=payload, stderr="")
                    with mock.patch("chamber.pki.subprocess.run", return_value=proc):
                        with self.assertRaisesRegex(RuntimeError, err_regex):
                            fetch_pki_from_cluster("kueue-optimizer-chamber-c6", pki_dir)
                    # Atomic decode-before-write guarantee: no partial PKI files may poison cached retries.
                    if pki_dir.exists():
                        self.assertEqual(list(pki_dir.iterdir()), [])
                    self.assertFalse(any((pki_dir / k).exists() for k in PKI_REQUIRED_KEYS))


class YamlEmitterRoundTripTests(unittest.TestCase):
    """C7: YAML 1.1/1.2 scalar/key quoting and non-BMP escaping."""

    TRICKY_SCALARS = [
        "y",
        "n",
        "Y",
        "N",
        "yes",
        "No",
        "on",
        "OFF",
        "true",
        "false",
        "NULL",
        "~",
        "",
        "0x10",
        "0o17",
        "0b101",
        "1e3",
        "010",
        "1_000",
        "1_",
        "1_0_",
        "1__0",
        "1.0",
        "1.0_",
        "1_.0",
        "1._0",
        "1.2__3",
        "1_000.0_",
        "0.0_",
        ".5",
        ".5_",
        "0.",
        "+1",
        "-1",
        "+1_e2",
        "1_0e+1_",
        "-1_0e-1_",
        "1_.0e+2_",
        ".inf",
        ".NaN",
        "-.inf",
        "2024-01-01",
        "2001-12-14t21:59:43.10-05:00",
        "1:30",
        "190:20:30",
        "a: b",
        "a #b",
        "#x",
        "multi\nline",
        " lead",
        "trail ",
        "=",
        "<<",
        "!!str",
        "-",
        "---",
        "...",
        "a\tb",
        "@x",
        "%x",
        "`x`",
        "é",
        "\U0001F600",
        "1.30.5-gke.1014001",
        "n2-standard-4",
        "us-central1-a",
    ]

    def test_non_bmp_uses_8_hex_escape_and_kubectl_round_trips_labels_and_annotations(self) -> None:
        emitted_emoji = to_clean_yaml({"k": "\U0001F600"})
        self.assertIn("\\U0001f600", emitted_emoji)
        self.assertNotIn("\\ud83d", emitted_emoji.lower())

        # Plain safe identifiers remain unquoted.
        self.assertEqual(to_clean_yaml({"zone": "us-central1-a"}), "zone: us-central1-a")
        self.assertEqual(to_clean_yaml({"machine": "n2-standard-4"}), "machine: n2-standard-4")
        self.assertEqual(to_clean_yaml({"ver": "1.30.5-gke.1014001"}), "ver: 1.30.5-gke.1014001")

        # YAML 1.1 underscore-bearing integers and floats must always be quoted.
        underscore_nums = (
            "1_",
            "1_0_",
            "1__0",
            "1.0_",
            "1_.0",
            "1._0",
            "1.2__3",
            "1_000.0_",
            "0.0_",
            ".5_",
            "+1_e2",
            "1_0e+1_",
            "-1_0e-1_",
            "1_.0e+2_",
        )
        for num_str in underscore_nums:
            quoted = _yaml_string(num_str)
            self.assertTrue(
                quoted.startswith('"') and quoted.endswith('"'),
                f"Expected YAML 1.1 underscore numeric string {num_str!r} to be quoted, got {quoted!r}",
            )

        # Float formatting for YAML 1.1 compatibility.
        self.assertEqual(to_clean_yaml({"big": 1e16}), "big: 1.0e+16")
        self.assertEqual(to_clean_yaml({"small": 1e-07}), "small: 1.0e-07")
        self.assertEqual(
            to_clean_yaml({"neg_inf": float("-inf"), "not_a_num": float("nan"), "pos_inf": float("inf")}),
            "neg_inf: -.inf\nnot_a_num: .nan\npos_inf: .inf",
        )
        # Keys that look like floats ("inf", "nan") are quoted so YAML parsers keep them as strings.
        self.assertEqual(
            to_clean_yaml({"inf": float("inf"), "nan": float("nan")}),
            '"inf": .inf\n"nan": .nan',
        )

        # Verify through kubectl's goyaml.v2 decoder for both values and keys!
        with tempfile.TemporaryDirectory(prefix="kueue-opt-c7-") as tmp_str:
            tmp_dir = Path(tmp_str)
            # Test tricky values in labels and annotations.
            label_vals = ["y", "N", "0x10", "0o17", "0b101", "yes", "1e3", "010", "2024-01-01"]
            ann_vals = [
                ".inf",
                "-.inf",
                ".NaN",
                "\U0001F600",
                "on",
                "off",
                "null",
                "~",
                "1:30",
                "---",
                "...",
                "1_",
                "1_0_",
                "1__0",
                "1.0_",
                "1_.0",
                "1._0",
                "1.2__3",
                "1_000.0_",
                "0.0_",
                "+1_e2",
                "1_0e+1_",
            ]
            node = {
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {
                    "name": "n1",
                    "labels": {f"example.com/l{i}": v for i, v in enumerate(label_vals)},
                    "annotations": {
                        **{f"example.com/a{i}": v for i, v in enumerate(ann_vals)},
                        # Also test tricky map keys directly!
                        "on": "v-on",
                        "no": "v-no",
                        "y": "v-y",
                        "010": "v-010",
                        "1.10": "v-110",
                        "0x10": "v-0x10",
                        "null": "v-null",
                        "~": "v-tilde",
                        "2024-01-01": "v-date",
                    },
                },
            }
            node_file = tmp_dir / "node.yaml"
            node_file.write_text(docs_to_pretty_yaml([node]), encoding="utf-8")
            proc = subprocess.run(
                ["kubectl", "--kubeconfig=/dev/null", "annotate", "--local", "-f", str(node_file), "zz=1", "-o", "json"],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(proc.returncode, 0, msg=f"kubectl rejected YAML: {proc.stderr}")
            parsed = json.loads(proc.stdout)
            for i, v in enumerate(label_vals):
                self.assertEqual(parsed["metadata"]["labels"][f"example.com/l{i}"], v)
            for i, v in enumerate(ann_vals):
                self.assertEqual(parsed["metadata"]["annotations"][f"example.com/a{i}"], v)
            for key, expected in (
                ("on", "v-on"),
                ("no", "v-no"),
                ("y", "v-y"),
                ("010", "v-010"),
                ("1.10", "v-110"),
                ("0x10", "v-0x10"),
                ("null", "v-null"),
                ("~", "v-tilde"),
                ("2024-01-01", "v-date"),
            ):
                self.assertEqual(parsed["metadata"]["annotations"].get(key), expected)

    def test_pyyaml_round_trip_if_available(self) -> None:
        try:
            import yaml  # type: ignore
        except ImportError:
            self.skipTest("PyYAML not installed in stdlib-only environment")

        for val in self.TRICKY_SCALARS:
            loaded = yaml.safe_load(to_clean_yaml({"k": val}))
            self.assertEqual(loaded["k"], val)
            self.assertIs(type(loaded["k"]), str)

            if val != "":
                loaded_key = yaml.safe_load(to_clean_yaml({"outer": {val: "x"}}))
                self.assertEqual(list(loaded_key["outer"].keys()), [val])
                self.assertIs(type(next(iter(loaded_key["outer"].keys()))), str)

        loaded_floats = yaml.safe_load(to_clean_yaml({"big": 1e16, "small": 1e-07, "inf": float("inf"), "nan": float("nan")}))
        self.assertEqual(loaded_floats["big"], 1e16)
        self.assertEqual(loaded_floats["small"], 1e-07)
        self.assertTrue(math.isinf(loaded_floats["inf"]) and loaded_floats["inf"] > 0)
        self.assertTrue(math.isnan(loaded_floats["nan"]))


class ChamberCommonDynamicRegistryPortTests(unittest.TestCase):
    """C8: chamber_common.py render/apply resolves dynamic per-chamber registry NodePorts."""

    def test_cli_render_respects_port_flag(self) -> None:
        out = io.StringIO()
        with redirect_stdout(out):
            rc = chamber_common._cli_main(
                [
                    "render",
                    "--namespace",
                    "kueue-optimizer-chamber-c8",
                    "--port",
                    "16445",
                    "--adhoc-only",
                ]
            )
        self.assertEqual(rc, 0)
        self.assertIn("nodePort: 30502", out.getvalue())

    def test_cli_render_collision_keeps_stdout_pure_yaml(self) -> None:
        fake_svcs = {
            "items": [
                {
                    "metadata": {"namespace": "kueue-optimizer-chamber-01", "name": "kwok-registry"},
                    "spec": {"ports": [{"port": 5000, "nodePort": 30500}]},
                }
            ]
        }

        def fake_host_kubectl(self_mgr: ChamberManager, args: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
            if args[:3] == ["get", "svc", "-A"]:
                return subprocess.CompletedProcess(args, 0, stdout=json.dumps(fake_svcs), stderr="")
            return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

        out = io.StringIO()
        err = io.StringIO()
        with (
            mock.patch.object(ChamberManager, "host_kubectl", autospec=True, side_effect=fake_host_kubectl),
            mock.patch("chamber.pki.fetch_pki_from_cluster", return_value=False),
            redirect_stdout(out),
            redirect_stderr(err),
        ):
            rc = chamber_common._cli_main(
                [
                    "render",
                    "--namespace",
                    "kueue-optimizer-chamber-02",
                ]
            )
        self.assertEqual(rc, 0)
        rendered_stdout = out.getvalue()
        rendered_stderr = err.getvalue()
        self.assertNotIn("Note:", rendered_stdout)
        self.assertIn("Note: NodePort 30500 is in use", rendered_stderr)
        self.assertIn("nodePort: 30501", rendered_stdout)
        self.assertNotIn("nodePort: 30500", rendered_stdout)
        try:
            import yaml  # type: ignore

            docs = [d for d in yaml.safe_load_all(rendered_stdout) if d is not None]
            self.assertTrue(docs, "Expected rendered YAML documents on stdout")
            for doc in docs:
                self.assertIsInstance(doc, dict)
            reg_svcs = [
                d
                for d in docs
                if d.get("kind") == "Service" and (d.get("metadata") or {}).get("name") == "kwok-registry"
            ]
            self.assertEqual(len(reg_svcs), 1)
            self.assertEqual(reg_svcs[0]["spec"]["ports"][0]["nodePort"], 30501)
        except ImportError:
            self.assertTrue(rendered_stdout.lstrip().startswith("apiVersion:"))

    def test_cli_apply_resolves_cluster_registry_port_collision(self) -> None:
        fake_svcs = {
            "items": [
                {
                    "metadata": {"namespace": "kueue-optimizer-chamber-01", "name": "kwok-registry"},
                    "spec": {"ports": [{"port": 5000, "nodePort": 30500}]},
                }
            ]
        }
        applied_reg_texts: list[str] = []

        def fake_host_kubectl(self_mgr: ChamberManager, args: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
            if args[:2] == ["get", "namespace"]:
                return subprocess.CompletedProcess(
                    args, 1, stdout="", stderr='Error from server (NotFound): namespaces "kueue-optimizer-chamber-02" not found'
                )
            if args[:3] == ["get", "svc", "-A"]:
                return subprocess.CompletedProcess(args, 0, stdout=json.dumps(fake_svcs), stderr="")
            if args[0] == "apply" and "-k" in args:
                k_dir = Path(args[args.index("-k") + 1])
                reg_file = k_dir / "registry.yaml"
                if not reg_file.exists():
                    reg_file = k_dir / "base" / "registry.yaml"
                applied_reg_texts.append(reg_file.read_text(encoding="utf-8"))
                return subprocess.CompletedProcess(args, 0, stdout="", stderr="")
            return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

        with (
            mock.patch.object(ChamberManager, "host_kubectl", autospec=True, side_effect=fake_host_kubectl),
            redirect_stdout(io.StringIO()),
            redirect_stderr(io.StringIO()),
        ):
            rc = chamber_common._cli_main(
                [
                    "apply",
                    "--namespace",
                    "kueue-optimizer-chamber-02",
                    "--adhoc-only",
                ]
            )
        self.assertEqual(rc, 0)
        self.assertEqual(len(applied_reg_texts), 1)
        self.assertIn("nodePort: 30501", applied_reg_texts[0])
        self.assertNotIn("nodePort: 30500", applied_reg_texts[0])


class SignalAndTempPkiCleanupTests(unittest.TestCase):
    """C9: SIGTERM/SIGHUP terminates port-forward children and cleans up temporary PKI dirs."""

    def _assert_signal_cleans_up(self, sig: signal.Signals) -> None:
        with tempfile.TemporaryDirectory(prefix="kueue-opt-c9-") as tmp_str:
            work = Path(tmp_str)
            stub_bin = work / "bin"
            stub_bin.mkdir()
            (stub_bin / "kubectl").write_text("#!/bin/sh\nexec sleep 45\n", encoding="utf-8")
            (stub_bin / "pkill").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            for name in ("kubectl", "pkill"):
                (stub_bin / name).chmod(0o755)

            with socket.socket() as sock:
                sock.bind(("127.0.0.1", 0))
                free_port = sock.getsockname()[1]

            marker_file = work / "ready.json"
            tmp_marker_file = work / "ready.json.tmp"
            child_code = (
                "import json, os, signal, sys, tempfile, time\n"
                "from pathlib import Path\n"
                f"signal.signal(signal.{sig.name}, signal.SIG_DFL)\n"
                f"sys.path.insert(0, {str(SCRIPT_DIR)!r})\n"
                "from chamber.lifecycle import ChamberManager, termination_signal_handlers\n"
                f"work = Path({str(work)!r})\n"
                "with termination_signal_handlers():\n"
                f"    mgr = ChamberManager(chamber_namespace='review-sig', prometheus_port={free_port}, kubeconfig_out=work / 'kc')\n"
                "    mgr._tmp_pki_dir = Path(tempfile.mkdtemp(prefix='kueue-optimizer-pki-', dir=str(work)))\n"
                "    mgr._prom_pf_proc = __import__('subprocess').Popen(['kubectl', 'port-forward'], stdout=-3, stderr=-3)\n"
                f"    Path({str(tmp_marker_file)!r}).write_text(json.dumps({{'pki': str(mgr._tmp_pki_dir), 'pf_pid': mgr._prom_pf_proc.pid}}), encoding='utf-8')\n"
                f"    os.replace({str(tmp_marker_file)!r}, {str(marker_file)!r})\n"
                "    time.sleep(60)\n"
            )
            env = dict(os.environ, PATH=f"{stub_bin}:{os.environ.get('PATH', '')}", KUBECONFIG="/dev/null")
            child = subprocess.Popen(
                [sys.executable, "-c", child_code],
                env=env,
                start_new_session=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
            pf_pid: int | None = None
            try:
                deadline = time.monotonic() + 10.0
                while not marker_file.exists() and time.monotonic() < deadline:
                    if child.poll() is not None:
                        err_out = child.stderr.read() if child.stderr else b""
                        self.fail(f"child exited early (rc={child.returncode}): {err_out!r}")
                    time.sleep(0.05)
                self.assertTrue(marker_file.exists(), "child did not reach ready state")
                info = json.loads(marker_file.read_text(encoding="utf-8"))
                pki_dir = Path(info["pki"])
                pf_pid = int(info["pf_pid"])
                self.assertTrue(pki_dir.exists())

                child.send_signal(sig)
                rc = child.wait(timeout=10.0)
                self.assertEqual(rc, 128 + int(sig))

                # Portably wait for terminated port-forward process to disappear via os.kill(pid, 0).
                def _pid_alive(pid: int) -> bool:
                    try:
                        os.kill(pid, 0)
                    except ProcessLookupError:
                        return False
                    except PermissionError:
                        return True
                    return True

                reap_deadline = time.monotonic() + 3.0
                while _pid_alive(pf_pid) and time.monotonic() < reap_deadline:
                    time.sleep(0.05)
                with self.assertRaises(ProcessLookupError, msg=f"port-forward pid {pf_pid} survived {sig.name}"):
                    os.kill(pf_pid, 0)
                self.assertFalse(pki_dir.exists(), f"temp PKI dir {pki_dir} survived {sig.name}")
            finally:
                if child.poll() is None:
                    child.kill()
                    child.wait(timeout=5)
                if pf_pid is not None:
                    try:
                        os.kill(pf_pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                if child.stderr:
                    child.stderr.close()

    def test_sigterm_terminates_children_and_removes_tmp_pki(self) -> None:
        self._assert_signal_cleans_up(signal.SIGTERM)

    def test_sighup_terminates_children_and_removes_tmp_pki(self) -> None:
        if hasattr(signal, "SIGHUP"):
            self._assert_signal_cleans_up(signal.SIGHUP)

    def test_signal_handler_restoration_and_preservation(self) -> None:
        orig_term = signal.getsignal(signal.SIGTERM)
        orig_hup = signal.getsignal(signal.SIGHUP) if hasattr(signal, "SIGHUP") else None
        try:
            # 1. Instantiating ChamberManager must NOT mutate process-global signal handlers.
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
            mgr = ChamberManager(chamber_namespace="kueue-optimizer-chamber-c9-init", apiserver_port=16487)
            try:
                self.assertIs(signal.getsignal(signal.SIGTERM), signal.SIG_DFL)
            finally:
                mgr.close()

            # 2. termination_signal_handlers installs on SIG_DFL and restores SIG_DFL on exit (normal and error).
            with termination_signal_handlers() as prev:
                self.assertEqual(prev.get(signal.SIGTERM), signal.SIG_DFL)
                self.assertIs(signal.getsignal(signal.SIGTERM), _handle_termination_signal)
                # Nested context manager must not overwrite or prematurely restore outer handler.
                with termination_signal_handlers() as inner_prev:
                    self.assertNotIn(signal.SIGTERM, inner_prev)
                self.assertIs(signal.getsignal(signal.SIGTERM), _handle_termination_signal)
            self.assertIs(signal.getsignal(signal.SIGTERM), signal.SIG_DFL)

            # 3. SIG_IGN (e.g. nohup) is preserved untouched.
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            with termination_signal_handlers() as prev_ign:
                self.assertNotIn(signal.SIGTERM, prev_ign)
                self.assertIs(signal.getsignal(signal.SIGTERM), signal.SIG_IGN)
            self.assertIs(signal.getsignal(signal.SIGTERM), signal.SIG_IGN)

            # 4. Custom caller-installed handlers are preserved untouched.
            def custom_handler(_signum: int, _frame: object) -> None:
                pass

            signal.signal(signal.SIGTERM, custom_handler)
            with termination_signal_handlers() as prev_custom:
                self.assertNotIn(signal.SIGTERM, prev_custom)
                self.assertIs(signal.getsignal(signal.SIGTERM), custom_handler)
            self.assertIs(signal.getsignal(signal.SIGTERM), custom_handler)

            # 5. Manual install/restore API returns previous handlers and restores cleanly.
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
            prev_manual = install_termination_signal_handlers()
            self.assertEqual(prev_manual.get(signal.SIGTERM), signal.SIG_DFL)
            self.assertIs(signal.getsignal(signal.SIGTERM), _handle_termination_signal)
            restore_termination_signal_handlers(prev_manual)
            self.assertIs(signal.getsignal(signal.SIGTERM), signal.SIG_DFL)
        finally:
            signal.signal(signal.SIGTERM, orig_term)
            if orig_hup is not None:
                signal.signal(signal.SIGHUP, orig_hup)

    def test_managed_chamber_runs_cleanup_on_signal_system_exit(self) -> None:
        orig_term = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        mgr = ChamberManager(chamber_namespace="kueue-optimizer-chamber-c9-mc", apiserver_port=16488)
        try:
            def _deploy(initial_nodes: object = None) -> None:
                mgr._created_namespace_in_this_run = True

            with (
                mock.patch.object(mgr, "deploy_chamber", side_effect=_deploy),
                mock.patch.object(mgr, "cleanup_chamber") as m_cleanup,
                redirect_stdout(io.StringIO()),
            ):
                with self.assertRaises(SystemExit) as ctx:
                    with managed_chamber(mgr, skip_deploy=False, should_cleanup=True):
                        self.assertIs(signal.getsignal(signal.SIGTERM), _handle_termination_signal)
                        raise SystemExit(128 + int(signal.SIGTERM))
                self.assertEqual(ctx.exception.code, 128 + int(signal.SIGTERM))
                m_cleanup.assert_called_once()
            self.assertIs(signal.getsignal(signal.SIGTERM), signal.SIG_DFL)
        finally:
            mgr.close()
            signal.signal(signal.SIGTERM, orig_term)


class DockerLocalImageDistinctionTests(unittest.TestCase):
    """C10: Distinguish missing Docker binary / daemon error from a genuinely remote registry image."""

    def test_docker_image_exists_locally_distinguishes_errors(self) -> None:
        with mock.patch("chamber.parsers.subprocess.run", side_effect=FileNotFoundError("docker")):
            with self.assertRaises(DockerUnavailableError):
                docker_image_exists_locally("my-kueue:dev")

        daemon_down = subprocess.CompletedProcess(
            ["docker"],
            returncode=1,
            stdout="",
            stderr="Cannot connect to the Docker daemon at unix:///var/run/docker.sock. Is the docker daemon running?\n",
        )
        with mock.patch("chamber.parsers.subprocess.run", return_value=daemon_down):
            with self.assertRaises(DockerUnavailableError):
                docker_image_exists_locally("my-kueue:dev")

        # Broken DOCKER_CONTEXT emits "context not found" — must raise DockerUnavailableError, NOT return False!
        for ctx_err in (
            'context "nonexistent-ctx": context not found\n',
            'failed to initialize: context "nonexistent-ctx": context not found\n',
            "unable to resolve docker endpoint: dial unix /bad.sock\n",
            "failed to connect to the docker API at unix:///bad.sock\n",
            "Cannot connect to Podman. Please verify your connection to the Linux system using `podman system connection list`\n",
        ):
            bad_ctx = subprocess.CompletedProcess(["docker"], returncode=1, stdout="", stderr=ctx_err)
            with mock.patch("chamber.parsers.subprocess.run", return_value=bad_ctx):
                with self.assertRaises(DockerUnavailableError, msg=f"Failed to raise for stderr: {ctx_err!r}"):
                    docker_image_exists_locally("my-kueue:dev")

        for nf_err in (
            "Error: No such image: us-central1-docker.pkg.dev/k8s-staging-images/kueue/kueue:main\n",
            "Error: my-kueue:dev: image not known\n",
            "Error: image not found\n",
            "Error: reference not found\n",
        ):
            not_found = subprocess.CompletedProcess(["docker"], returncode=1, stdout="", stderr=nf_err)
            with mock.patch("chamber.parsers.subprocess.run", return_value=not_found):
                self.assertFalse(
                    docker_image_exists_locally("us-central1-docker.pkg.dev/k8s-staging-images/kueue/kueue:main")
                )

        found = subprocess.CompletedProcess(["docker"], returncode=0, stdout="", stderr="")
        with mock.patch("chamber.parsers.subprocess.run", return_value=found):
            self.assertTrue(docker_image_exists_locally("my-kueue:dev"))

    def test_prepare_adhoc_kueue_image_fails_on_local_ref_when_docker_missing_but_allows_remote(self) -> None:
        # Unqualified local image without Docker -> fail fast with actionable RuntimeError + docker.io/ hint.
        mgr_local = ChamberManager(
            chamber_namespace="kueue-optimizer-chamber-c10a",
            kueue_image="myorg/kueue:v1",
        )
        try:
            with mock.patch(
                "chamber.registry.docker_image_exists_locally",
                side_effect=DockerUnavailableError("Docker CLI ('docker') was not found on PATH."),
            ):
                with self.assertRaises(RuntimeError) as ctx:
                    mgr_local._prepare_adhoc_kueue_image_before_deploy()
                self.assertIn("Cannot inspect or push local Kueue image 'myorg/kueue:v1'", str(ctx.exception))
                self.assertIn("docker.io/myorg/kueue:v1", str(ctx.exception))
        finally:
            mgr_local.close()

        # Remote registry image without Docker -> warn and keep remote reference.
        remote_ref = "us-central1-docker.pkg.dev/k8s-staging-images/kueue/kueue:main"
        mgr_remote = ChamberManager(
            chamber_namespace="kueue-optimizer-chamber-c10b",
            kueue_image=remote_ref,
        )
        err_buf = io.StringIO()
        try:
            with (
                mock.patch(
                    "chamber.registry.docker_image_exists_locally",
                    side_effect=DockerUnavailableError("Docker CLI ('docker') was not found on PATH."),
                ),
                redirect_stderr(err_buf),
            ):
                mgr_remote._prepare_adhoc_kueue_image_before_deploy()
            self.assertEqual(mgr_remote._resolved_kueue_image, remote_ref)
            self.assertIn("Skipping local Docker check", err_buf.getvalue())
        finally:
            mgr_remote.close()

        # --kueue-build-dir & push_image_to_chamber_registry preflight Docker BEFORE staging kwok-registry.
        with tempfile.TemporaryDirectory(prefix="kueue-opt-c10-build-") as tmp_str:
            src_dir = Path(tmp_str)
            (src_dir / "Dockerfile").write_text("FROM scratch\n", encoding="utf-8")
            mgr_build = ChamberManager(
                chamber_namespace="kueue-optimizer-chamber-c10c",
                kueue_build_dir=src_dir,
            )
            daemon_err = subprocess.CompletedProcess(
                ["docker", "info"],
                returncode=1,
                stdout="",
                stderr="Cannot connect to the Docker daemon\n",
            )
            try:
                for mock_kw in (
                    {"side_effect": FileNotFoundError("docker")},
                    {"return_value": daemon_err},
                ):
                    with (
                        mock.patch.object(mgr_build, "_ensure_registry_staged_or_connected") as m_stage,
                        mock.patch.object(mgr_build, "host_kubectl") as m_kubectl,
                        mock.patch("chamber.parsers.subprocess.run", **mock_kw),
                        redirect_stdout(io.StringIO()),
                    ):
                        with self.assertRaises(DockerUnavailableError):
                            mgr_build._prepare_adhoc_kueue_image_before_deploy()
                        m_stage.assert_not_called()
                        m_kubectl.assert_not_called()

                    with (
                        mock.patch.object(mgr_build, "_ensure_registry_staged_or_connected") as m_stage,
                        mock.patch.object(mgr_build, "host_kubectl") as m_kubectl,
                        mock.patch("chamber.parsers.subprocess.run", **mock_kw),
                        redirect_stdout(io.StringIO()),
                    ):
                        with self.assertRaises(DockerUnavailableError):
                            mgr_build.push_image_to_chamber_registry("my-kueue:dev", stage_if_needed=True)
                        m_stage.assert_not_called()
                        m_kubectl.assert_not_called()
            finally:
                mgr_build.close()


if __name__ == "__main__":
    unittest.main()
