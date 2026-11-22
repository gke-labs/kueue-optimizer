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

"""Shared CLI flags, Fair Sharing resolution, and managed chamber lifecycle context manager."""

from __future__ import annotations

import argparse
from collections.abc import Iterator
from contextlib import contextmanager
import os
from pathlib import Path
import sys
from typing import Any

from chamber.constants import (
    DEFAULT_APISERVER_PORT,
    DEFAULT_CHAMBER_NAMESPACE,
    DEFAULT_PAUSE_IMAGE,
    DEFAULT_REGISTRY_BASE_PORT,
    DEFAULT_REGISTRY_MEMORY_LIMIT,
    DEFAULT_SCRAPE_INTERVAL,
    NODEPORT_MAX,
    NODEPORT_MIN,
)
from chamber.lifecycle import ChamberManager
from chamber.parsers import env_bool, env_int, env_str


def add_common_chamber_args(
    parser: argparse.ArgumentParser,
    default_workload_namespace: str,
    default_queueing_mode: str,
    default_pause_image: str | None = DEFAULT_PAUSE_IMAGE,
) -> None:
    """Register CLI flags shared across chamber runner scripts (testrunner.py and demo.py)."""
    default_ns = env_str("NAMESPACE", DEFAULT_CHAMBER_NAMESPACE)
    default_wl_ns = env_str("WORKLOAD_NAMESPACE", default_workload_namespace)
    default_q_mode = env_str("QUEUEING_MODE", env_str("QUEUEING_STRATEGY", default_queueing_mode))
    default_port = env_int("LOCAL_APISERVER_PORT", DEFAULT_APISERVER_PORT)
    default_scrape = env_str("PROMETHEUS_SCRAPE_INTERVAL", DEFAULT_SCRAPE_INTERVAL)

    parser.add_argument(
        "--namespace",
        default=default_ns,
        help=f"Host cluster namespace for the kueue-optimizer chamber (default: {default_ns})",
    )
    parser.add_argument(
        "--workload-namespace",
        default=default_wl_ns,
        help=f"Namespace inside the KWOK cluster for queues and workloads (default: {default_wl_ns})",
    )
    parser.add_argument(
        "--queueing-mode",
        "--queueing-strategy",
        "--mode",
        "--strategy",
        dest="queueing_mode",
        default=default_q_mode,
        help=f"ClusterQueue queueing strategy: StrictFIFO (strict-fifo) or BestEffortFIFO (best-effort) (default: {default_q_mode})",
    )
    parser.add_argument(
        "--strict-fifo",
        action="store_const",
        const="StrictFIFO",
        dest="queueing_mode",
        help="Shorthand for --queueing-mode StrictFIFO",
    )
    parser.add_argument(
        "--best-effort",
        "--best-effort-fifo",
        action="store_const",
        const="BestEffortFIFO",
        dest="queueing_mode",
        help="Shorthand for --queueing-mode BestEffortFIFO",
    )
    parser.add_argument(
        "--fair-sharing",
        dest="fair_sharing",
        action=argparse.BooleanOptionalAction,
        default=None,
        help=(
            "Enable Preemption-Based Fair Sharing (fairSharing.preemptionStrategies: "
            "[LessThanOrEqualToFinalShare, LessThanInitialShare]) in Kueue controller configuration "
            "(default: enabled; pass --no-fair-sharing for Classical Preemption; incompatible with --skip-chamber-deploy)"
        ),
    )
    parser.add_argument(
        "--pause-image",
        default=env_str("PAUSE_IMAGE", default_pause_image) if default_pause_image else os.environ.get("PAUSE_IMAGE"),
        help="Container image for simulated KWOK pods",
    )
    parser.add_argument(
        "--scrape-interval",
        default=default_scrape,
        help=f"Prometheus scrape interval for Kueue metrics (default: {default_scrape})",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=default_port,
        help=f"Local port for port-forwarding the KWOK kube-apiserver (default: {default_port})",
    )
    parser.add_argument(
        "--prometheus-port",
        type=int,
        default=None,
        help="Local port for port-forwarding chamber Prometheus (default: 19090 + port - 16443)",
    )
    parser.add_argument(
        "--registry-port",
        type=int,
        default=None,
        help=(
            f"Kubernetes NodePort ({NODEPORT_MIN}..{NODEPORT_MAX}) & local port-forward port for the "
            f"per-chamber in-memory image registry (default: {DEFAULT_REGISTRY_BASE_PORT} + port - {DEFAULT_APISERVER_PORT})"
        ),
    )
    parser.add_argument(
        "--registry-memory-limit",
        default=env_str("REGISTRY_MEMORY_LIMIT", DEFAULT_REGISTRY_MEMORY_LIMIT),
        help=(
            "RAM limit (tmpfs sizeLimit & container memory limit, e.g. '4Gi', '8Gi') for the per-chamber "
            f"in-memory image registry (default: {DEFAULT_REGISTRY_MEMORY_LIMIT})"
        ),
    )
    parser.add_argument(
        "--kueue-image",
        default=os.environ.get("KUEUE_IMAGE"),
        help=(
            "Override image for kueue-controller-manager (accepts a local Docker image tag — automatically "
            "pushed into this chamber's in-memory registry — or a localhost:<NodePort>/... or remote registry ref)"
        ),
    )
    parser.add_argument(
        "--kueue-build-dir",
        type=Path,
        default=Path(os.environ["KUEUE_BUILD_DIR"]) if os.environ.get("KUEUE_BUILD_DIR") else None,
        help=(
            "Optional path to a local Kueue source checkout to docker-build and push into this chamber's "
            "in-memory registry before deploying kueue-controller-manager"
        ),
    )
    parser.add_argument(
        "--kueue-feature-gates",
        default=os.environ.get("KUEUE_FEATURE_GATES"),
        help=(
            "Optional comma-separated Kueue feature gate overrides rendered into controller_manager_config.yaml "
            "(e.g. 'TASReplaceMultipleFailedNodes=true')"
        ),
    )
    parser.add_argument(
        "--skip-chamber-deploy",
        "--reuse-chamber",
        action="store_true",
        default=False,
        help="Skip deploying the chamber into the host cluster and connect to an already-running chamber in --namespace",
    )
    parser.add_argument(
        "--cleanup",
        "--clean",
        action="store_true",
        default=env_bool("CLEANUP", False),
        help="Tear down an existing chamber in host namespace --namespace and exit",
    )
    parser.add_argument(
        "--cleanup-workloads",
        "--clean-workloads",
        action="store_true",
        default=env_bool("CLEANUP_WORKLOADS", False),
        help="Delete workloads and Kueue queues in the KWOK cluster and exit",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=env_bool("DRY_RUN", False),
        help="Print generated manifests and exit without applying to a cluster",
    )


def resolve_fair_sharing_arg(
    cli_fair_sharing: bool | None,
    skip_chamber_deploy: bool,
    parser: argparse.ArgumentParser,
) -> bool:
    """Resolve effective --fair-sharing boolean and reject overriding it alongside --skip-chamber-deploy."""
    if skip_chamber_deploy and cli_fair_sharing is not None:
        parser.error(
            "--fair-sharing / --no-fair-sharing cannot be combined with --skip-chamber-deploy "
            "because Kueue's controller configuration is only applied when deploying a chamber."
        )
    if cli_fair_sharing is not None:
        return bool(cli_fair_sharing)
    return env_bool("FAIR_SHARING", True)


@contextmanager
def managed_chamber(
    mgr: ChamberManager,
    *,
    skip_deploy: bool,
    should_cleanup: bool,
    initial_nodes: list[dict[str, Any]] | None = None,
) -> Iterator[ChamberManager]:
    """Deploy (unless skip_deploy) and guarantee safe teardown matching testrunner/demo semantics.

    On normal exit: calls `mgr.cleanup_chamber()` if `should_cleanup` is True, otherwise `mgr.close()`.
    On exception: calls `mgr.cleanup_chamber()` only if `should_cleanup` is True AND this run created the
    namespace (`mgr.created_namespace_in_this_run`), preserving pre-existing chambers and namespaces.
    """
    run_succeeded = False
    try:
        if not skip_deploy:
            mgr.deploy_chamber(initial_nodes=initial_nodes)
        yield mgr
        run_succeeded = True
    finally:
        if should_cleanup and run_succeeded:
            print("", flush=True)
            mgr.cleanup_chamber()
        elif should_cleanup and mgr.created_namespace_in_this_run:
            print("", flush=True)
            try:
                mgr.cleanup_chamber()
            except Exception as cleanup_exc:
                print(
                    f"WARNING: Chamber cleanup after error failed for '{mgr.chamber_namespace}': {cleanup_exc}",
                    file=sys.stderr,
                    flush=True,
                )
        else:
            mgr.close()
