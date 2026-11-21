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

"""Backwards-compatible re-export facade and Makefile CLI entrypoint for the `chamber` package.

Used by:
  - Makefile (`render`, `apply`, `delete`, `local-kubeconfig`)
  - deploy/scripts/testrunner.py
  - deploy/scripts/chamber_registry.py
  - deploy/scripts/demo.py
  - deploy/scripts/test_*.py

`kueue_metrics_report.py`, `generate_scenario_readme.py`, and the `metrics`, `runner`, and `scenario_readme`
packages import the `chamber` sub-modules directly.
"""

from __future__ import annotations

import argparse
import subprocess
import sys

from chamber import (  # noqa: F401
    BINARY_SI_FACTORS,
    CA_BUNDLE_MANIFEST_FILES,
    CA_BUNDLE_PLACEHOLDER,
    CHAMBER_DEPLOYMENTS,
    CHAMBER_NAMESPACE_LABEL_KEY,
    CHAMBER_NAMESPACE_LABEL_VALUE,
    CHAMBER_PART_OF_LABEL_VALUE,
    DECIMAL_SI_FACTORS,
    DEFAULT_APISERVER_PORT,
    DEFAULT_CHAMBER_NAMESPACE,
    DEFAULT_FAIR_SHARING_PREEMPTION_STRATEGIES,
    DEFAULT_FALLBACK_EXEC_SEC,
    DEFAULT_FLAVOR_NAME,
    DEFAULT_INFRA_PODS_NAMESPACE,
    DEFAULT_KUEUE_IMAGE,
    DEFAULT_PAUSE_IMAGE,
    DEFAULT_PROMETHEUS_BASE_PORT,
    DEFAULT_REGISTRY_BASE_PORT,
    DEFAULT_REGISTRY_MEMORY_LIMIT,
    DEFAULT_SCRAPE_INTERVAL,
    DEFAULT_TOPOLOGY_NAME,
    DEPLOY_DIR,
    DNS_LABEL_RE,
    K8S_META_KEY_ORDER,
    K8S_TOP_KEY_ORDER,
    METRICS_TOKEN_CSV,
    NODEPORT_MAX,
    NODEPORT_MIN,
    PKI_REQUIRED_KEYS,
    PKI_SECRET_NAME,
    REQUIRED_KWOK_CRDS,
    RESERVED_POD_LABELS,
    ROOT_DIR,
    SCRIPT_DIR,
    SCRIPT_PATH,
    YAML_SAFE_UNQUOTED_RE,
    YAML_SPECIAL_STRINGS,
    ChamberManager,
    ClusterStatusSnapshot,
    ExecutionTimeIndex,
    WorkloadExecutionInfo,
    add_common_chamber_args,
    build_cluster_queue_manifest,
    build_cohort_manifest,
    build_kwok_node_manifest,
    build_local_queue_manifest,
    build_namespace_manifest,
    build_priority_class_manifest,
    build_resource_flavor_manifest,
    build_topology_assignment,
    calc_summary_stats,
    collect_known_parent_workflows,
    collect_not_admitted_workflow_admit_epochs,
    compute_percentile,
    compute_prometheus_port,
    compute_registry_port,
    describe_k8s_obj_header,
    docker_image_exists_locally,
    docs_to_pretty_yaml,
    ensure_pki_dir,
    env_bool,
    env_int,
    env_str,
    extract_pod_spec_resources,
    extract_workflow_pods_and_requests,
    fetch_pki_from_cluster,
    filter_initial_running_pods,
    format_compact_si,
    format_duration_hms,
    format_duration_human,
    format_duration_short,
    format_epoch_iso,
    format_kwok_pod_duration,
    format_num,
    format_pct,
    format_pod_execution_seconds,
    format_resource_quantity,
    generate_adhoc_pki,
    index_execution_times,
    inject_ca_bundle_into_manifests,
    is_child_workflow_owned_by_tracked_parent,
    load_jsonl_events,
    managed_chamber,
    multiply_k8s_quantity_exact,
    normalize_queueing_strategy,
    normalize_scrape_interval,
    obj_to_pretty_yaml,
    parse_duration_seconds,
    parse_image_ref,
    parse_k8s_quantity,
    parse_positive_duration_annotation,
    parse_rfc3339_epoch,
    query_cluster_status,
    resolve_fair_sharing_arg,
    resolve_kubeconfig_path,
    sanitize_k8s_name,
    summarize_samples,
    to_clean_yaml,
    validate_dns_label,
)


def _cli_main() -> int:
    """CLI entrypoint for Makefile targets (render, apply, delete, local-kubeconfig)."""
    parser = argparse.ArgumentParser(
        description="Chamber PKI & Kustomize staging utility (generates PKI ad-hoc or picks from cluster)."
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    for cmd_name in ("render", "apply"):
        p = sub.add_parser(cmd_name)
        p.add_argument("--namespace", default=env_str("NAMESPACE", DEFAULT_CHAMBER_NAMESPACE))
        p.add_argument("--scrape-interval", default=env_str("PROMETHEUS_SCRAPE_INTERVAL", DEFAULT_SCRAPE_INTERVAL))
        p.add_argument("--with-sample-queues", action="store_true", default=False)
        p.add_argument("--adhoc-only", action="store_true", default=False)
        p.add_argument(
            "--fair-sharing",
            dest="fair_sharing",
            action=argparse.BooleanOptionalAction,
            default=env_bool("FAIR_SHARING", True),
        )

    p_del = sub.add_parser("delete", aliases=["cleanup"])
    p_del.add_argument("--namespace", default=env_str("NAMESPACE", DEFAULT_CHAMBER_NAMESPACE))

    p_kc = sub.add_parser("local-kubeconfig")
    p_kc.add_argument("--namespace", default=env_str("NAMESPACE", DEFAULT_CHAMBER_NAMESPACE))
    p_kc.add_argument("--port", type=int, default=env_int("LOCAL_APISERVER_PORT", DEFAULT_APISERVER_PORT))
    p_kc.add_argument("--out", default=None)

    args = parser.parse_args()

    if args.cmd in ("delete", "cleanup"):
        mgr = ChamberManager(chamber_namespace=args.namespace)
        mgr.cleanup_chamber()
        return 0

    if args.cmd == "local-kubeconfig":
        mgr = ChamberManager(
            chamber_namespace=args.namespace,
            apiserver_port=args.port,
            kubeconfig_out=args.out,
        )
        try:
            kc_path = mgr.write_local_kubeconfig(prefer_cluster=True)
            print(
                f"Wrote {kc_path} (PKI source: {mgr._pki_source}, server: https://127.0.0.1:{args.port})"
            )
        finally:
            mgr.close()
        return 0

    mgr = ChamberManager(
        chamber_namespace=args.namespace,
        scrape_interval=args.scrape_interval,
        enable_fair_sharing=args.fair_sharing,
    )
    try:
        if args.cmd == "apply":
            mgr._ensure_namespace_safe_for_apply(record_creation=False)
        base_dst = mgr.prepare_tmp_deploy_dir(
            initial_nodes=None,
            include_overlays=args.with_sample_queues,
            prefer_cluster_pki=not args.adhoc_only,
        )
        target_k_dir = base_dst.parent if args.with_sample_queues else base_dst
        if args.cmd == "render":
            res = subprocess.run(
                ["kubectl", "--kubeconfig=/dev/null", "kustomize", str(target_k_dir)],
                check=True,
                text=True,
                capture_output=True,
            )
            sys.stdout.write(res.stdout)
        elif args.cmd == "apply":
            mgr._ensure_namespace_safe_for_apply(record_creation=True)
            mgr.host_kubectl(
                ["apply", "--server-side", "--force-conflicts", "-k", str(target_k_dir)],
                stream_output=True,
            )
    finally:
        mgr.close()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(_cli_main())
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr, flush=True)
        sys.exit(1)
