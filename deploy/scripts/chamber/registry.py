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

"""Per-chamber in-memory OCI registry NodePort allocation, staging, push/build, and GC helpers."""

from __future__ import annotations

import json
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import time
from typing import Any
import urllib.request

from chamber.constants import DEPLOY_DIR, NODEPORT_MAX, NODEPORT_MIN
from chamber.parsers import docker_image_exists_locally, parse_image_ref


class ChamberRegistryMixin:
    """Mixin providing per-chamber in-memory kwok-registry staging, image push/build, and GC."""

    def _resolve_cluster_registry_port(self, check_cluster: bool = True) -> int:
        """Resolve per-chamber NodePort, reusing existing Service/kwok-registry port or avoiding collisions."""
        if not check_cluster:
            return self.registry_port
        try:
            proc = self.host_kubectl(["get", "svc", "-A", "-o", "json"], check=False, timeout=15.0)
            if proc.returncode != 0 or not proc.stdout:
                return self.registry_port
            data = json.loads(proc.stdout)
        except Exception:
            return self.registry_port

        other_used_nodeports: dict[int, str] = {}
        for item in data.get("items") or []:
            meta = item.get("metadata") or {}
            ns = str(meta.get("namespace") or "")
            name = str(meta.get("name") or "")
            for p in (item.get("spec") or {}).get("ports") or []:
                np = p.get("nodePort")
                if not np:
                    continue
                np_int = int(np)
                if ns == self.chamber_namespace and name == "kwok-registry":
                    self.registry_port = np_int
                    return np_int
                if ns != self.chamber_namespace:
                    other_used_nodeports[np_int] = f"{ns}/{name}"

        if self.registry_port not in other_used_nodeports:
            return self.registry_port

        if self._explicit_registry_port is not None:
            holder = other_used_nodeports[self.registry_port]
            raise RuntimeError(
                f"Requested --registry-port {self.registry_port} is already allocated by Service '{holder}'. "
                f"Choose an unused NodePort in {NODEPORT_MIN}..{NODEPORT_MAX} or tear down that chamber first."
            )

        prev_port = self.registry_port
        holder = other_used_nodeports[prev_port]
        for offset in range(1, NODEPORT_MAX - NODEPORT_MIN + 1):
            cand = NODEPORT_MIN + ((prev_port - NODEPORT_MIN + offset) % (NODEPORT_MAX - NODEPORT_MIN + 1))
            if cand not in other_used_nodeports:
                self.registry_port = cand
                print(
                    f"    Note: NodePort {prev_port} is in use by '{holder}'; "
                    f"selected free per-chamber registry NodePort {cand} for '{self.chamber_namespace}'.",
                    flush=True,
                )
                return cand
        raise RuntimeError(f"No free Kubernetes NodePorts available in {NODEPORT_MIN}..{NODEPORT_MAX}.")

    def stage_registry_only(self, timeout_sec: float = 180.0) -> str:
        """Deploy Namespace + per-chamber in-memory kwok-registry before full chamber rollout and wait until ready."""
        self._ensure_namespace_safe_for_apply(record_creation=False)
        self._resolve_cluster_registry_port(check_cluster=True)
        stage_dir = Path(tempfile.mkdtemp(prefix="kueue-optimizer-registry-stage-"))
        try:
            shutil.copy2(DEPLOY_DIR / "base" / "namespace.yaml", stage_dir / "namespace.yaml")
            shutil.copy2(DEPLOY_DIR / "base" / "registry.yaml", stage_dir / "registry.yaml")
            ns_yaml = stage_dir / "namespace.yaml"
            ns_yaml.write_text(
                re.sub(
                    r"^(\s*name:\s*).*",
                    rf"\g<1>{self.chamber_namespace}",
                    ns_yaml.read_text(encoding="utf-8"),
                    flags=re.MULTILINE,
                ),
                encoding="utf-8",
            )
            self._patch_registry_manifest(stage_dir, self.registry_port, self.registry_memory_limit)
            (stage_dir / "kustomization.yaml").write_text(
                f"apiVersion: kustomize.config.k8s.io/v1beta1\n"
                f"kind: Kustomization\n"
                f"namespace: {self.chamber_namespace}\n"
                f"resources:\n"
                f"- namespace.yaml\n"
                f"- registry.yaml\n",
                encoding="utf-8",
            )
            print(
                f"==> Staging per-chamber in-memory image registry in '{self.chamber_namespace}' "
                f"(NodePort {self.registry_port}, memory limit {self.registry_memory_limit})...",
                flush=True,
            )
            self._ensure_namespace_safe_for_apply(record_creation=True)
            self.host_kubectl(
                ["apply", "--server-side", "--force-conflicts", "-k", str(stage_dir)],
                stream_output=True,
            )
            self.host_kubectl(
                [
                    "-n",
                    self.chamber_namespace,
                    "rollout",
                    "status",
                    f"--timeout={int(timeout_sec)}s",
                    "deployment/kwok-registry",
                ],
                timeout=timeout_sec + 20.0,
                stream_output=True,
            )
        finally:
            shutil.rmtree(stage_dir, ignore_errors=True)
        return self.ensure_registry_connection(timeout_sec=60.0)

    def _probe_registry_ready(self, port: int | None = None, timeout: float = 1.5) -> bool:
        target_port = int(port if port is not None else self.registry_port)
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        try:
            with opener.open(f"http://127.0.0.1:{target_port}/v2/", timeout=timeout) as resp:
                resp.read()
                return 200 <= resp.status < 300
        except Exception:
            return False

    def ensure_registry_connection(self, timeout_sec: float = 45.0) -> str:
        """Ensure local port-forward 127.0.0.1:<registry_port> -> svc/kwok-registry:5000 is active."""
        self._resolve_cluster_registry_port(check_cluster=True)
        with self._pf_lock:
            if self._probe_registry_ready(self.registry_port, timeout=1.5):
                return f"localhost:{self.registry_port}"

            if self._registry_pf_proc is not None:
                try:
                    self._registry_pf_proc.terminate()
                    self._registry_pf_proc.wait(timeout=2)
                except Exception:
                    try:
                        self._registry_pf_proc.kill()
                    except Exception:
                        pass
                self._registry_pf_proc = None

            self._pkill_chamber_port_forward("kwok-registry", f"{self.registry_port}:5000")

            deadline = time.monotonic() + timeout_sec
            while time.monotonic() < deadline:
                if self._registry_pf_proc is None or self._registry_pf_proc.poll() is not None:
                    self._registry_pf_proc = subprocess.Popen(
                        [
                            "kubectl",
                            "-n",
                            self.chamber_namespace,
                            "port-forward",
                            "svc/kwok-registry",
                            f"{self.registry_port}:5000",
                        ],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )
                for _ in range(10):
                    if self._probe_registry_ready(self.registry_port, timeout=1.5):
                        return f"localhost:{self.registry_port}"
                    if self._registry_pf_proc.poll() is not None:
                        break
                    time.sleep(0.3)
                time.sleep(0.5)

        raise RuntimeError(
            f"Timed out waiting to connect to per-chamber registry svc/kwok-registry in namespace "
            f"'{self.chamber_namespace}' on localhost:{self.registry_port}"
        )

    def fetch_registry_manifest_digest(self, repo: str, tag: str) -> str:
        """Query the per-chamber OCI registry HTTP API for Docker-Content-Digest of <repo>:<tag>."""
        self.ensure_registry_connection(timeout_sec=30.0)
        accept = ", ".join(
            [
                "application/vnd.oci.image.index.v1+json",
                "application/vnd.oci.image.manifest.v1+json",
                "application/vnd.docker.distribution.manifest.list.v2+json",
                "application/vnd.docker.distribution.manifest.v2+json",
            ]
        )
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.registry_port}/v2/{repo}/manifests/{tag}",
            method="HEAD",
            headers={"Accept": accept},
        )
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(req, timeout=10.0) as resp:
            digest = resp.headers.get("Docker-Content-Digest") or ""
            return digest.strip()

    def _ensure_registry_staged_or_connected(self, stage_if_needed: bool = True) -> None:
        """Ensure kwok-registry is deployed (when stage_if_needed=True) and local port-forward is connected."""
        if stage_if_needed:
            proc_check = self.host_kubectl(
                ["-n", self.chamber_namespace, "get", "deployment/kwok-registry"],
                check=False,
                timeout=15.0,
            )
            if proc_check.returncode != 0:
                self.stage_registry_only()
                return
        self.ensure_registry_connection()

    def push_image_to_chamber_registry(
        self,
        source_image: str,
        repo: str = "kueue",
        tag: str | None = None,
        stage_if_needed: bool = True,
    ) -> tuple[str, str]:
        """Tag and push a local Docker image into this chamber's in-memory registry.

        Returns (pullable_tag_ref, manifest_sha256_digest), e.g.:
          ('localhost:30500/kueue:adhoc', 'sha256:...')
        """
        self._ensure_registry_staged_or_connected(stage_if_needed=stage_if_needed)

        _, _, parsed_qual = parse_image_ref(source_image)
        if tag:
            clean_tag = re.sub(r"[^A-Za-z0-9_.-]+", "-", tag.strip()).strip(".-") or "adhoc"
        elif parsed_qual and not parsed_qual.startswith("sha256:"):
            clean_tag = re.sub(r"[^A-Za-z0-9_.-]+", "-", parsed_qual).strip(".-") or "adhoc"
        else:
            clean_tag = "adhoc"

        clean_repo = repo.strip("/ ") or "kueue"
        target_ref = f"localhost:{self.registry_port}/{clean_repo}:{clean_tag}"
        if source_image.strip() != target_ref:
            subprocess.run(["docker", "tag", source_image.strip(), target_ref], check=True, timeout=60.0)

        print(
            f"==> Pushing '{source_image}' -> per-chamber in-memory registry '{target_ref}' "
            f"(namespace '{self.chamber_namespace}')...",
            flush=True,
        )
        subprocess.run(["docker", "push", target_ref], check=True, timeout=300.0)
        digest = ""
        try:
            digest = self.fetch_registry_manifest_digest(clean_repo, clean_tag)
        except Exception:
            digest = ""
        digest_note = f" ({digest})" if digest else ""
        print(f"    Stored in chamber RAM registry: {target_ref}{digest_note}", flush=True)
        return target_ref, digest

    def build_and_push_kueue_image(
        self,
        kueue_src_dir: Path | str,
        tag: str = "adhoc",
        dockerfile: str | None = None,
        build_args: list[str] | None = None,
        stage_if_needed: bool = True,
    ) -> tuple[str, str]:
        """Build an ad-hoc Kueue image from source directory and push it into this chamber's in-memory registry."""
        src_path = Path(kueue_src_dir).resolve()
        if not src_path.is_dir():
            raise RuntimeError(f"Kueue build directory does not exist: {src_path}")

        self._ensure_registry_staged_or_connected(stage_if_needed=stage_if_needed)

        clean_tag = re.sub(r"[^A-Za-z0-9_.-]+", "-", (tag or "adhoc").strip()).strip(".-") or "adhoc"
        local_tag = f"localhost:{self.registry_port}/kueue:{clean_tag}"
        cmd = ["docker", "build", "-t", local_tag]
        if dockerfile:
            cmd.extend(["-f", dockerfile])
        elif (src_path / "Dockerfile").is_file():
            cmd.extend(["-f", str(src_path / "Dockerfile")])
        for ba in build_args or ():
            cmd.extend(["--build-arg", ba])
        cmd.append(str(src_path))

        print(
            f"==> Building ad-hoc Kueue image from '{src_path}' -> '{local_tag}'...",
            flush=True,
        )
        subprocess.run(cmd, check=True, timeout=900.0)
        return self.push_image_to_chamber_registry(
            source_image=local_tag,
            repo="kueue",
            tag=clean_tag,
            stage_if_needed=False,
        )

    def _prepare_adhoc_kueue_image_before_deploy(self) -> None:
        """If kueue_build_dir or a local Docker kueue_image is configured, stage registry & push before deploy."""
        if self.kueue_build_dir is not None:
            target_ref, digest = self.build_and_push_kueue_image(
                kueue_src_dir=self.kueue_build_dir,
                tag="adhoc",
                stage_if_needed=True,
            )
            self._resolved_kueue_image = f"{target_ref}@{digest}" if digest else target_ref
            return

        if not self.kueue_image:
            self._resolved_kueue_image = None
            return

        raw_img = self.kueue_image.strip()
        reg_host, repo_path, qual = parse_image_ref(raw_img)
        # If the image exists in the local workstation Docker daemon, push it into THIS chamber's in-memory registry
        # so every GKE worker node can pull it over loopback NodePort (localhost:<registry_port>/...).
        if docker_image_exists_locally(raw_img):
            leaf_repo = repo_path.split("/")[-1] if repo_path else "kueue"
            target_tag = qual if (qual and not qual.startswith("sha256:")) else "adhoc"
            target_ref, digest = self.push_image_to_chamber_registry(
                source_image=raw_img,
                repo=leaf_repo,
                tag=target_tag,
                stage_if_needed=True,
            )
            self._resolved_kueue_image = f"{target_ref}@{digest}" if digest else target_ref
            return

        # Otherwise treat as an already-pushed chamber registry reference or external remote registry image
        if reg_host.startswith("localhost:"):
            self._resolved_kueue_image = raw_img
        else:
            self._resolved_kueue_image = raw_img

    def update_kueue_image(
        self,
        kueue_image: str,
        tag: str | None = None,
        timeout_sec: float = 180.0,
        push_if_local: bool = True,
    ) -> str:
        """Push (if local) and hot-swap deployment/kueue-controller-manager in a running chamber."""
        self.kueue_image = kueue_image.strip()
        if push_if_local and docker_image_exists_locally(self.kueue_image):
            _, repo_path, qual = parse_image_ref(self.kueue_image)
            leaf_repo = repo_path.split("/")[-1] if repo_path else "kueue"
            target_tag = tag or (qual if (qual and not qual.startswith("sha256:")) else "adhoc")
            target_ref, digest = self.push_image_to_chamber_registry(
                source_image=self.kueue_image,
                repo=leaf_repo,
                tag=target_tag,
                stage_if_needed=True,
            )
            resolved = f"{target_ref}@{digest}" if digest else target_ref
        else:
            resolved = self.kueue_image

        self._resolved_kueue_image = resolved
        patch_json = json.dumps(
            {
                "spec": {
                    "template": {
                        "metadata": {
                            "annotations": {
                                "kueue-optimizer.gke.io/kueue-image-updated-at": f"{time.time():.6f}",
                            }
                        },
                        "spec": {
                            "containers": [
                                {
                                    "name": "manager",
                                    "image": resolved,
                                    "imagePullPolicy": "Always",
                                }
                            ]
                        },
                    }
                }
            }
        )
        print(
            f"==> Updating deployment/kueue-controller-manager in '{self.chamber_namespace}' -> {resolved}",
            flush=True,
        )
        self.host_kubectl(
            [
                "-n",
                self.chamber_namespace,
                "patch",
                "deployment",
                "kueue-controller-manager",
                "--type=strategic",
                "-p",
                patch_json,
            ],
            stream_output=True,
        )
        self.host_kubectl(
            [
                "-n",
                self.chamber_namespace,
                "rollout",
                "status",
                f"--timeout={int(timeout_sec)}s",
                "deployment/kueue-controller-manager",
            ],
            timeout=timeout_sec + 20.0,
            stream_output=True,
        )
        return resolved

    def list_chamber_registry_images(self) -> list[dict[str, Any]]:
        """Return a list of {repository, tag, digest, image_ref} entries stored in this chamber's in-memory registry."""
        self.ensure_registry_connection(timeout_sec=30.0)
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(f"http://127.0.0.1:{self.registry_port}/v2/_catalog", timeout=10.0) as resp:
            catalog = json.loads(resp.read().decode("utf-8"))
        repos = catalog.get("repositories") or []
        entries: list[dict[str, Any]] = []
        for repo in repos:
            try:
                with opener.open(f"http://127.0.0.1:{self.registry_port}/v2/{repo}/tags/list", timeout=10.0) as r_tags:
                    tags_data = json.loads(r_tags.read().decode("utf-8"))
                tags = tags_data.get("tags") or []
            except Exception:
                tags = []
            for t in sorted(tags):
                digest = ""
                try:
                    digest = self.fetch_registry_manifest_digest(repo, t)
                except Exception:
                    digest = ""
                entries.append(
                    {
                        "repository": repo,
                        "tag": t,
                        "digest": digest,
                        "image_ref": f"localhost:{self.registry_port}/{repo}:{t}",
                    }
                )
        return entries

    def get_chamber_registry_memory_usage(self) -> dict[str, Any]:
        """Inspect tmpfs (/var/lib/registry) memory utilization inside deployment/kwok-registry."""
        proc = self.host_kubectl(
            [
                "-n",
                self.chamber_namespace,
                "exec",
                "deployment/kwok-registry",
                "--",
                "df",
                "-k",
                "/var/lib/registry",
            ],
            check=False,
            timeout=15.0,
        )
        if proc.returncode != 0 or not proc.stdout:
            return {}
        lines = [ln.strip() for ln in proc.stdout.strip().splitlines() if ln.strip()]
        if len(lines) < 2:
            return {}
        parts = lines[-1].split()
        if len(parts) < 6:
            return {}
        try:
            total_kib = int(parts[1])
            used_kib = int(parts[2])
            avail_kib = int(parts[3])
            return {
                "filesystem": parts[0],
                "total_mib": round(total_kib / 1024.0, 2),
                "used_mib": round(used_kib / 1024.0, 2),
                "available_mib": round(avail_kib / 1024.0, 2),
                "use_percent": parts[4],
                "mountpoint": parts[5],
            }
        except Exception:
            return {}

    def garbage_collect_chamber_registry(self, delete_untagged: bool = True) -> str:
        """Run OCI registry garbage-collect inside deployment/kwok-registry to free tmpfs RAM."""
        args = [
            "-n",
            self.chamber_namespace,
            "exec",
            "deployment/kwok-registry",
            "--",
            "registry",
            "garbage-collect",
        ]
        if delete_untagged:
            args.append("--delete-untagged=true")
        args.append("/etc/docker/registry/config.yml")
        proc = self.host_kubectl(args, check=True, timeout=60.0)
        return (proc.stdout or "") + (proc.stderr or "")
