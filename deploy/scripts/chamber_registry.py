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

"""Per-chamber in-memory OCI container image registry CLI for kueue-optimizer evaluation chambers.

Each chamber namespace (`kueue-optimizer-chamber-*`) runs an isolated `kwok-registry` Deployment
backed by a kernel `tmpfs` (`emptyDir: { medium: Memory }`) volume exposed via `Service/kwok-registry`
(`type: NodePort`, default `30500 + (port - 16443)`).

Subcommands:
  status        Show chamber registry pod status, NodePort, RAM (`tmpfs`) usage, and pushed images/digests
  stage         Stage only the chamber namespace + in-memory `kwok-registry` (before full chamber deploy)
  push          Tag and push a local Docker image into a chamber's in-memory registry (optional `--set-kueue`)
  build-kueue   Build Kueue from a source directory, push into the chamber's in-memory registry (optional `--set-kueue`)
  delete        Delete an image manifest from a chamber's in-memory registry and optionally run garbage collection
  gc            Run `registry garbage-collect` inside the chamber registry pod to reclaim `tmpfs` RAM
  port-forward  Keep a foreground `kubectl port-forward` open on `localhost:<registry-port>`
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys
import urllib.request

from chamber_common import (
    DEFAULT_APISERVER_PORT,
    DEFAULT_CHAMBER_NAMESPACE,
    DEFAULT_REGISTRY_MEMORY_LIMIT,
    ChamberManager,
    env_int,
    env_str,
    parse_image_ref,
)


def _add_target_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--namespace",
        "-n",
        default=env_str("NAMESPACE", DEFAULT_CHAMBER_NAMESPACE),
        help=f"Target chamber namespace in the host cluster (default: {DEFAULT_CHAMBER_NAMESPACE})",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=env_int("LOCAL_APISERVER_PORT", DEFAULT_APISERVER_PORT),
        help=f"Chamber apiserver port offset used to derive default registry NodePort (default: {DEFAULT_APISERVER_PORT})",
    )
    parser.add_argument(
        "--registry-port",
        type=int,
        default=None,
        help="Explicit Kubernetes NodePort (30000..32767) & local port-forward port for the chamber registry",
    )
    parser.add_argument(
        "--registry-memory-limit",
        default=env_str("REGISTRY_MEMORY_LIMIT", DEFAULT_REGISTRY_MEMORY_LIMIT),
        help=f"RAM limit for tmpfs & container memory (default: {DEFAULT_REGISTRY_MEMORY_LIMIT})",
    )


def _cmd_status(mgr: ChamberManager, as_json: bool = False) -> int:
    mgr._resolve_cluster_registry_port(check_cluster=True)
    proc = mgr.host_kubectl(
        ["-n", mgr.chamber_namespace, "get", "deployment/kwok-registry", "-o", "json"],
        check=False,
        timeout=15.0,
    )
    if proc.returncode != 0 or not proc.stdout:
        msg = f"No per-chamber registry found in namespace '{mgr.chamber_namespace}'."
        if as_json:
            print(json.dumps({"namespace": mgr.chamber_namespace, "running": False, "message": msg}, indent=2))
        else:
            print(msg)
        return 1

    dep = json.loads(proc.stdout)
    ready_replicas = int((dep.get("status") or {}).get("readyReplicas") or 0)
    mem_usage = mgr.get_chamber_registry_memory_usage()
    images = mgr.list_chamber_registry_images() if ready_replicas > 0 else []

    payload = {
        "namespace": mgr.chamber_namespace,
        "running": ready_replicas > 0,
        "ready_replicas": ready_replicas,
        "endpoint": f"localhost:{mgr.registry_port}",
        "node_port": mgr.registry_port,
        "memory_usage": mem_usage,
        "images": images,
    }
    if as_json:
        print(json.dumps(payload, indent=2))
        return 0

    print("================================================================================")
    print("  KUEUE-OPTIMIZER PER-CHAMBER IN-MEMORY IMAGE REGISTRY")
    print("================================================================================")
    print(f"  Chamber Namespace  : {mgr.chamber_namespace}")
    print(f"  Status             : {'Ready' if ready_replicas > 0 else 'NotReady'} (readyReplicas={ready_replicas})")
    print(f"  Endpoint / NodePort: localhost:{mgr.registry_port} (NodePort {mgr.registry_port} -> 5000)")
    if mem_usage:
        print(
            f"  RAM (tmpfs) Usage  : {mem_usage['used_mib']:.2f} MiB / {mem_usage['total_mib']:.2f} MiB "
            f"({mem_usage['use_percent']}, avail {mem_usage['available_mib']:.2f} MiB on {mem_usage['mountpoint']})"
        )
    print(f"  Stored Images ({len(images)}):")
    if not images:
        print("    (empty)")
    else:
        for img in images:
            dig_str = f"  [{img['digest']}]" if img.get("digest") else ""
            print(f"    - {img['image_ref']}{dig_str}")
    print("================================================================================")
    return 0


def _cmd_delete(mgr: ChamberManager, image_target: str, run_gc: bool = True) -> int:
    _, repo_path, qual = parse_image_ref(image_target)
    repo = repo_path.strip("/")
    if not repo:
        raise RuntimeError(f"Could not parse repository from '{image_target}'. Expected '<repo>:<tag>'.")
    tag = qual or "latest"
    digest = tag if tag.startswith("sha256:") else mgr.fetch_registry_manifest_digest(repo, tag)
    if not digest:
        raise RuntimeError(f"Manifest not found in chamber registry for '{repo}:{tag}'.")

    req = urllib.request.Request(
        f"http://127.0.0.1:{mgr.registry_port}/v2/{repo}/manifests/{digest}",
        method="DELETE",
    )
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(req, timeout=15.0) as resp:
        resp.read()
    print(f"==> Deleted manifest {repo}@{digest} from chamber '{mgr.chamber_namespace}'.", flush=True)
    if run_gc:
        out = mgr.garbage_collect_chamber_registry(delete_untagged=True)
        if out.strip():
            print(out.strip(), flush=True)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Manage the isolated per-chamber in-memory Docker/OCI image registry (`kwok-registry`)."
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_status = sub.add_parser("status", help="Show chamber registry status, RAM usage, and stored images")
    _add_target_args(p_status)
    p_status.add_argument("--json", action="store_true", default=False, help="Emit machine-readable JSON output")

    p_stage = sub.add_parser("stage", help="Stage only Namespace + in-memory kwok-registry in target chamber")
    _add_target_args(p_stage)

    p_push = sub.add_parser("push", help="Tag and push a local Docker image into the chamber's in-memory registry")
    _add_target_args(p_push)
    p_push.add_argument("source_image", help="Local Docker image reference to push (e.g. 'kueue-dev:latest')")
    p_push.add_argument("--repo", default="kueue", help="Target repository name in chamber registry (default: kueue)")
    p_push.add_argument("--tag", default=None, help="Target tag in chamber registry (default: source tag or 'adhoc')")
    p_push.add_argument(
        "--set-kueue",
        action="store_true",
        default=False,
        help="Immediately patch and roll out deployment/kueue-controller-manager in this chamber to use the pushed image",
    )

    p_build = sub.add_parser(
        "build-kueue",
        help="Build Kueue controller image from a source directory and push into the chamber's in-memory registry",
    )
    _add_target_args(p_build)
    p_build.add_argument("kueue_dir", type=Path, help="Path to local Kueue source checkout")
    p_build.add_argument("--tag", default="adhoc", help="Target tag in chamber registry (default: adhoc)")
    p_build.add_argument("--dockerfile", default=None, help="Optional path to Dockerfile (defaults to <kueue_dir>/Dockerfile)")
    p_build.add_argument(
        "--build-arg",
        action="append",
        default=[],
        help="Optional Docker --build-arg KEY=VAL (may be specified multiple times)",
    )
    p_build.add_argument(
        "--set-kueue",
        action="store_true",
        default=False,
        help="Immediately patch and roll out deployment/kueue-controller-manager in this chamber to use the built image",
    )

    p_del = sub.add_parser("delete", help="Delete an image manifest from the chamber registry and reclaim RAM")
    _add_target_args(p_del)
    p_del.add_argument("image_ref", help="Repository/tag to delete (e.g. 'kueue:adhoc')")
    p_del.add_argument(
        "--gc",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Run registry garbage-collect after manifest deletion (default: true)",
    )

    p_gc = sub.add_parser("gc", help="Run registry garbage-collect inside the chamber registry to free tmpfs RAM")
    _add_target_args(p_gc)

    p_pf = sub.add_parser("port-forward", help="Run foreground kubectl port-forward to svc/kwok-registry")
    _add_target_args(p_pf)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    mgr = ChamberManager(
        chamber_namespace=args.namespace,
        apiserver_port=args.port,
        registry_port=args.registry_port,
        registry_memory_limit=args.registry_memory_limit,
    )
    try:
        if args.cmd == "status":
            return _cmd_status(mgr, as_json=args.json)

        if args.cmd == "stage":
            endpoint = mgr.stage_registry_only()
            print(f"Per-chamber registry ready at {endpoint} (namespace '{mgr.chamber_namespace}')")
            return 0

        if args.cmd == "push":
            ref, digest = mgr.push_image_to_chamber_registry(
                source_image=args.source_image,
                repo=args.repo,
                tag=args.tag,
                stage_if_needed=True,
            )
            full_ref = f"{ref}@{digest}" if digest else ref
            if args.set_kueue:
                mgr.update_kueue_image(full_ref, tag=args.tag, push_if_local=False)
            print(f"Ready image reference: {full_ref}")
            return 0

        if args.cmd == "build-kueue":
            ref, digest = mgr.build_and_push_kueue_image(
                kueue_src_dir=args.kueue_dir,
                tag=args.tag,
                dockerfile=args.dockerfile,
                build_args=args.build_arg,
                stage_if_needed=True,
            )
            full_ref = f"{ref}@{digest}" if digest else ref
            if args.set_kueue:
                mgr.update_kueue_image(full_ref, tag=args.tag, push_if_local=False)
            print(f"Ready image reference: {full_ref}")
            return 0

        if args.cmd == "delete":
            return _cmd_delete(mgr, args.image_ref, run_gc=args.gc)

        if args.cmd == "gc":
            out = mgr.garbage_collect_chamber_registry(delete_untagged=True)
            if out.strip():
                print(out.strip())
            return 0

        if args.cmd == "port-forward":
            mgr._resolve_cluster_registry_port(check_cluster=True)
            print(
                f"==> Port-forwarding per-chamber registry in '{mgr.chamber_namespace}' "
                f"on http://localhost:{mgr.registry_port} -> svc/kwok-registry:5000 (Ctrl-C to stop)...",
                flush=True,
            )
            return subprocess.run(
                [
                    "kubectl",
                    "-n",
                    mgr.chamber_namespace,
                    "port-forward",
                    "svc/kwok-registry",
                    f"{mgr.registry_port}:5000",
                ],
                check=False,
            ).returncode

        return 1
    finally:
        mgr.close()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except BrokenPipeError:
        sys.exit(0)
    except subprocess.CalledProcessError as exc:
        sys.exit(exc.returncode or 1)
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr, flush=True)
        sys.exit(1)
