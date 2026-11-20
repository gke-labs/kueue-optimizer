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

"""Unit tests for per-chamber in-memory OCI registry plumbing in chamber_common.py & chamber_registry.py."""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from chamber_common import (
    DEFAULT_APISERVER_PORT,
    DEFAULT_REGISTRY_BASE_PORT,
    NODEPORT_MAX,
    NODEPORT_MIN,
    ChamberManager,
    compute_registry_port,
    parse_image_ref,
)


class PerChamberRegistryUnitTests(unittest.TestCase):
    def test_compute_registry_port_defaults_and_offsets(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("LOCAL_REGISTRY_PORT", None)
            self.assertEqual(compute_registry_port(DEFAULT_APISERVER_PORT), DEFAULT_REGISTRY_BASE_PORT)
            self.assertEqual(compute_registry_port(16401), 30458)
            self.assertEqual(compute_registry_port(16430), 30487)
            self.assertEqual(compute_registry_port(16499), 30556)
            wrapped = compute_registry_port(60000)
            self.assertGreaterEqual(wrapped, NODEPORT_MIN)
            self.assertLessEqual(wrapped, NODEPORT_MAX)

    def test_compute_registry_port_explicit_and_validation(self) -> None:
        self.assertEqual(compute_registry_port(16443, explicit_registry_port=31234), 31234)
        with self.assertRaises(ValueError):
            compute_registry_port(16443, explicit_registry_port=29999)
        with self.assertRaises(ValueError):
            compute_registry_port(16443, explicit_registry_port=32768)

    def test_parse_image_ref_variants(self) -> None:
        self.assertEqual(parse_image_ref("kueue-dev"), ("", "kueue-dev", "latest"))
        self.assertEqual(parse_image_ref("my-local-kueue:v2"), ("", "my-local-kueue", "v2"))
        self.assertEqual(
            parse_image_ref("localhost:30500/kueue:adhoc"),
            ("localhost:30500", "kueue", "adhoc"),
        )
        self.assertEqual(
            parse_image_ref("localhost:30556/kueue:eval@sha256:abcdef012345"),
            ("localhost:30556", "kueue", "sha256:abcdef012345"),
        )
        self.assertEqual(
            parse_image_ref("us-central1-docker.pkg.dev/k8s-staging-images/kueue/kueue:main"),
            ("us-central1-docker.pkg.dev", "k8s-staging-images/kueue/kueue", "main"),
        )

    def test_resolve_cluster_registry_port_avoids_collisions_and_reuses_own(self) -> None:
        mgr = ChamberManager(chamber_namespace="kueue-optimizer-chamber-b", apiserver_port=16443)
        try:
            fake_svcs = {
                "items": [
                    {
                        "metadata": {"namespace": "kueue-optimizer-chamber-a", "name": "kwok-registry"},
                        "spec": {"ports": [{"port": 5000, "nodePort": 30500}]},
                    }
                ]
            }
            fake_proc = subprocess.CompletedProcess(
                args=["kubectl"], returncode=0, stdout=json.dumps(fake_svcs), stderr=""
            )
            with mock.patch.object(mgr, "host_kubectl", return_value=fake_proc):
                chosen = mgr._resolve_cluster_registry_port(check_cluster=True)
                self.assertEqual(chosen, 30501)
                self.assertEqual(mgr.registry_port, 30501)

            # Now simulate kueue-optimizer-chamber-b already owning NodePort 30555 -> idempotent reuse
            fake_svcs["items"].append(
                {
                    "metadata": {"namespace": "kueue-optimizer-chamber-b", "name": "kwok-registry"},
                    "spec": {"ports": [{"port": 5000, "nodePort": 30555}]},
                }
            )
            fake_proc_own = subprocess.CompletedProcess(
                args=["kubectl"], returncode=0, stdout=json.dumps(fake_svcs), stderr=""
            )
            with mock.patch.object(mgr, "host_kubectl", return_value=fake_proc_own):
                reused = mgr._resolve_cluster_registry_port(check_cluster=True)
                self.assertEqual(reused, 30555)
        finally:
            mgr.close()

    def test_kustomize_renders_custom_registry_port_memory_and_kueue_image(self) -> None:
        custom_img = "localhost:30612/kueue:jit-test@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        mgr = ChamberManager(
            chamber_namespace="kueue-optimizer-chamber-unit",
            apiserver_port=16555,
            registry_port=30612,
            registry_memory_limit="8Gi",
            kueue_image=custom_img,
        )
        try:
            base_dst = mgr.prepare_tmp_deploy_dir(prefer_cluster_pki=False, check_cluster_ports=False)
            res = subprocess.run(
                ["kubectl", "--kubeconfig=/dev/null", "kustomize", str(base_dst)],
                check=True,
                text=True,
                capture_output=True,
            )
            rendered = res.stdout
            reg_patched = (base_dst / "registry.yaml").read_text(encoding="utf-8")
            self.assertIn("nodePort: 30612", rendered)
            self.assertIn("sizeLimit: 8Gi", rendered)
            self.assertIn("memory: 8Gi", rendered)
            self.assertIn("memory: 691Mi", reg_patched)
            self.assertNotIn("11059Mi", reg_patched)
            self.assertIn(f"image: {custom_img}", rendered)
            self.assertIn("imagePullPolicy: Always", rendered)
        finally:
            mgr.close()


if __name__ == "__main__":
    unittest.main()
