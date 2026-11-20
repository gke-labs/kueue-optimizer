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

"""Kueue Prometheus Metrics Catalog & Demo Summary Reporter CLI.

Reference documentation:
  https://kueue.sigs.k8s.io/docs/reference/metrics/
  (Local source in repo: kueue/site/content/en/docs/reference/metrics.md)

Core implementation lives in the `metrics` package (`deploy/scripts/metrics/`).
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import sys

from chamber.parsers import (  # noqa: F401
    compute_percentile,
    format_compact_si as _fmt_compact_si,
    format_num,
    format_pct,
    format_resource_quantity as _fmt_cap_qty,
    parse_k8s_quantity as _parse_k8s_qty,
    parse_positive_duration_annotation,
    parse_rfc3339_epoch as _parse_rfc3339_ts,
    summarize_samples,
)
from chamber.scenario import extract_pod_spec_resources as _extract_pod_spec_resources  # noqa: F401
from metrics import (  # noqa: F401
    IGNORED_LABELS,
    KUEUE_METRICS_CATALOG,
    MAX_TERMINAL_WIDTH,
    MetricSpec,
    MetricType,
    PrometheusClient,
    SAT_SYSTEM_NAMESPACES as _SAT_SYSTEM_NAMESPACES,
    build_monotonic_counter_points as _build_monotonic_counter_points,
    completed_effective_duration as _completed_effective_duration,
    counter_series_total_increase,
    extract_workload_resource_size_and_cfg_duration as _extract_workload_resource_size_and_cfg_duration,
    format_labels,
    histogram_quantile_from_buckets,
    load_or_fetch_cluster_snapshot,
    print_table,
    priority_sort_key as _priority_sort_key,
    raw_series_identity as _raw_series_identity,
    report_aggregated_evictions_and_reclaims,
    report_aggregated_quota_utilization,
    report_aggregated_wait_time_by_priority,
    report_counter,
    report_effective_uninterrupted_saturation,
    report_gauge,
    report_histogram,
    wrap_cell,
)


def _parse_pos_float_ann(anns: dict | None, key: str) -> float | None:
    return parse_positive_duration_annotation(anns, key)


def main() -> int:
    parser = argparse.ArgumentParser(description="Print Demo Summary and Kueue Prometheus Metrics Report")
    parser.add_argument("--namespace", required=True, help="Host cluster namespace for the chamber")
    parser.add_argument("--workload-namespace", default="ai-lab", help="Workload namespace in KWOK cluster")
    parser.add_argument(
        "--queueing-strategy",
        default="StrictFIFO",
        help="ClusterQueue queueingStrategy (StrictFIFO or BestEffortFIFO)",
    )
    parser.add_argument("--prometheus-url", default="", help="Optional local Prometheus HTTP URL (e.g. http://127.0.0.1:19090)")
    parser.add_argument("--start-epoch", type=float, required=True, help="Demo start timestamp (epoch seconds when JobSets were created)")
    parser.add_argument("--end-epoch", type=float, required=True, help="Demo end timestamp (epoch seconds when all JobSets completed)")
    parser.add_argument("--query-end-epoch", type=float, default=0.0, help="Timestamp for final Prometheus query (after final scrape)")
    parser.add_argument("--jobset-count", type=int, required=True, help="Total number of JobSets / Kueue Workloads")
    parser.add_argument("--total-pods", type=int, required=True, help="Total number of Pods across all JobSets")
    parser.add_argument("--step", type=float, default=1.0, help="Query range resolution step in seconds (default: 1.0)")
    parser.add_argument(
        "--cluster-snapshot-json",
        default="",
        help="Optional path to test-end cluster snapshot JSON (nodes, Kueue workloads, pods) for Section 5.4",
    )
    parser.add_argument(
        "--kubeconfig",
        default="",
        help=(
            "Optional path to chamber kubeconfig for live cluster snapshot fallback "
            "(defaults to ./kwok-<namespace>.kubeconfig when omitted)"
        ),
    )
    args = parser.parse_args()

    start_dt = datetime.fromtimestamp(args.start_epoch, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(args.end_epoch, tz=timezone.utc)
    duration_sec = max(0.0, args.end_epoch - args.start_epoch)
    mins, secs = divmod(int(round(duration_sec)), 60)
    query_end = args.query_end_epoch if args.query_end_epoch >= args.end_epoch else args.end_epoch

    print("")
    print("================================================================================")
    print("                        KUEUE-OPTIMIZER CHAMBER SUMMARY                         ")
    print("================================================================================")
    print(f"  Chamber Namespace       : {args.namespace}")
    print(f"  Workload Namespace      : {args.workload_namespace}")
    print(f"  Queueing Strategy       : {args.queueing_strategy}")
    print(f"  Start Time (JobSets)    : {start_dt.strftime('%Y-%m-%dT%H:%M:%SZ')} (epoch: {int(args.start_epoch)})")
    print(f"  End Time (Completed)    : {end_dt.strftime('%Y-%m-%dT%H:%M:%SZ')} (epoch: {int(args.end_epoch)})")
    print(f"  Total Duration          : {duration_sec:.1f}s ({mins}m {secs:02d}s)")
    print(f"  Total Workloads/JobSets : {args.jobset_count}")
    print(f"  Total Pods Executed     : {args.total_pods}")
    print("================================================================================", flush=True)

    print("  [1/4] Connecting to Prometheus in namespace " f"'{args.namespace}'...", flush=True)
    client = PrometheusClient(
        prometheus_url=args.prometheus_url if args.prometheus_url else None,
        namespace=args.namespace,
    )

    try:
        # Batch-fetch all Kueue & MultiKueue Gauge/Counter range series and Histogram
        # instant series in just 3 Prometheus API requests instead of 150+ requests.
        window_sec = 30.0
        range_start = max(0.0, args.start_epoch - window_sec)
        baseline_time = max(0.0, args.start_epoch - 1.0)

        print("  [2/4] Fetching Gauge & Counter time-series from Prometheus...", flush=True)
        all_range_series = client.query_range(
            '{__name__=~"kueue_.*|multikueue_.*",__name__!~".*_(bucket|sum|count)"}',
            range_start,
            query_end,
            step=args.step,
        )
        range_by_name: dict[str, list[dict]] = {}
        for s in all_range_series:
            mname = s.get("metric", {}).get("__name__", "")
            if mname:
                range_by_name.setdefault(mname, []).append(s)

        print("  [3/4] Fetching Histogram buckets & counts from Prometheus...", flush=True)
        end_hist_series = client.query_instant(
            '{__name__=~"kueue_.*_(bucket|sum|count)"}',
            query_end,
        )
        end_hist_by_name: dict[str, list[dict]] = {}
        for s in end_hist_series:
            mname = s.get("metric", {}).get("__name__", "")
            if mname:
                end_hist_by_name.setdefault(mname, []).append(s)

        start_hist_series = client.query_instant(
            '{__name__=~"kueue_.*_(bucket|sum|count)"}',
            baseline_time,
        )
        start_hist_by_name: dict[str, list[dict]] = {}
        for s in start_hist_series:
            mname = s.get("metric", {}).get("__name__", "")
            if mname:
                start_hist_by_name.setdefault(mname, []).append(s)
    finally:
        client.close()

    print(
        f"  [4/4] Computing P50 / P90 / P99 / AVG / MAX across {len(KUEUE_METRICS_CATALOG)} catalog metrics...",
        flush=True,
    )

    gauge_rows: list[list[str]] = []
    counter_rows: list[list[str]] = []
    histogram_rows: list[list[str]] = []
    unrecorded_metrics: list[tuple[str, str, str]] = []

    for spec in KUEUE_METRICS_CATALOG:
        if spec.metric_type == MetricType.GAUGE:
            res = report_gauge(range_by_name.get(spec.name, []), args.start_epoch)
            if not res:
                unrecorded_metrics.append((spec.section, spec.name, spec.metric_type.value))
            for lbl_str, p50, p90, p99, avg in res:
                gauge_rows.append([spec.section, spec.name, lbl_str, p50, p90, p99, avg])

        elif spec.metric_type == MetricType.COUNTER:
            res = report_counter(
                range_by_name.get(spec.name, []),
                args.start_epoch,
                query_end,
                step=args.step,
                window_sec=window_sec,
            )
            if not res:
                unrecorded_metrics.append((spec.section, spec.name, spec.metric_type.value))
            for lbl_str, p50, p90, p99, avg, total_inc in res:
                counter_rows.append([spec.section, spec.name, lbl_str, p50, p90, p99, avg, total_inc])

        elif spec.metric_type == MetricType.HISTOGRAM:
            res = report_histogram(end_hist_by_name, start_hist_by_name, spec)
            if not res:
                unrecorded_metrics.append((spec.section, spec.name, spec.metric_type.value))
            for lbl_str, p50, p90, hmax, avg, cnt, buckets in res:
                histogram_rows.append([spec.section, spec.name, lbl_str, p50, p90, hmax, avg, cnt, buckets])

    print("")
    print("=== 1. Kueue Gauge Metrics (P50, P90, P99, AVG over demo run) ===")
    print("    Ref: https://kueue.sigs.k8s.io/docs/reference/metrics/")
    print("")
    if gauge_rows:
        print_table(
            ["SECTION", "METRIC (GAUGE)", "LABELS", "P50", "P90", "P99", "AVG"],
            gauge_rows,
        )
    else:
        print("  (No gauge samples recorded)")

    print("")
    print("=== 2. Kueue Counter Metrics (P50, P90, P99, AVG of 30s-window increase) ===")
    print("    Ref: https://kueue.sigs.k8s.io/docs/reference/metrics/")
    print("")
    if counter_rows:
        print_table(
            ["SECTION", "METRIC (COUNTER)", "LABELS", "P50(30s)", "P90(30s)", "P99(30s)", "AVG(30s)", "TOTAL_INC"],
            counter_rows,
        )
    else:
        print("  (No counter samples recorded)")

    print("")
    print("=== 3. Kueue Histogram Metrics (P50, P90, MAX, AVG, COUNT, Bucket Distribution) ===")
    print("    Ref: https://kueue.sigs.k8s.io/docs/reference/metrics/")
    print("")
    if histogram_rows:
        print_table(
            ["SECTION", "METRIC (HISTOGRAM)", "LABELS", "P50", "P90", "MAX", "AVG", "COUNT", "POPULATED_BUCKETS"],
            histogram_rows,
        )
    else:
        print("  (No histogram observations recorded)")

    if unrecorded_metrics:
        print("")
        print("=== 4. Catalog Metrics Not Emitted in This Run (Inactive Features / Zero Events) ===")
        print_table(
            ["SECTION", "METRIC", "TYPE"],
            [[sec, name, mtype] for sec, name, mtype in unrecorded_metrics],
        )

    quota_util_rows = report_aggregated_quota_utilization(range_by_name, args.start_epoch)
    wait_time_prio_rows = report_aggregated_wait_time_by_priority(end_hist_by_name, start_hist_by_name)
    evict_summary_rows, evict_breakdown_rows = report_aggregated_evictions_and_reclaims(
        range_by_name,
        args.start_epoch,
    )
    cluster_snapshot = load_or_fetch_cluster_snapshot(
        args.cluster_snapshot_json,
        args.namespace,
        kubeconfig_path=args.kubeconfig,
    )
    eff_sat_rows, eff_sat_subtitle = report_effective_uninterrupted_saturation(
        cluster_snapshot,
        args.start_epoch,
        args.end_epoch,
    )

    print("")
    print("================================================================================")
    print("                        AGGREGATED KUEUE METRICS SUMMARY                        ")
    print("================================================================================")

    print("")
    print("=== 5.1 ClusterQueue & Cohort Quota Utilization per Resource Type (P50, P90, P99, AVG) ===")
    print("")
    if quota_util_rows:
        print_table(
            ["QUOTA SCOPE", "RESOURCE", "P50", "P90", "P99", "AVG", "AVG USED / QUOTA"],
            quota_util_rows,
        )
    else:
        print("  (No ClusterQueue or Cohort quota series recorded)")

    print("")
    print("=== 5.2 Wait Time Across All ClusterQueues per Workload Priority (P50, P90, P99, AVG) ===")
    print("")
    if wait_time_prio_rows:
        print_table(
            ["PRIORITY CLASS", "P50", "P90", "P99", "AVG", "WORKLOADS"],
            wait_time_prio_rows,
        )
    else:
        print("  (No admission wait time observations recorded)")

    print("")
    print("=== 5.3 Total Evictions and Reclaims ===")
    print("")
    print_table(
        ["CATEGORY", "METRIC", "TOTAL", "DETAILS"],
        evict_summary_rows,
    )
    if evict_breakdown_rows:
        print("")
        print_table(
            ["BREAKDOWN TYPE", "LABELS", "COUNT"],
            evict_breakdown_rows,
        )

    print("")
    print("=== 5.4 Effective Uninterrupted Cluster Saturation ===")
    if eff_sat_subtitle:
        print(eff_sat_subtitle)
    print("")
    if eff_sat_rows:
        print_table(
            [
                "RESOURCE",
                "CLUSTER CAP",
                "EFFECTIVE SAT",
                "COMPLETED ONLY",
                "ACTIVE AT END",
                "FULL-DUR SAT",
                "EFFECTIVE / EVAL CAP",
            ],
            eff_sat_rows,
        )
    else:
        print("  (No live cluster Node/Workload snapshot available to compute effective saturation)")

    print("================================================================================")
    return 0


if __name__ == "__main__":
    sys.exit(main())
