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

"""Comprehensive unit test suite for deploy/scripts/kueue_metrics_report.py.

Covers:
  1. Catalog integrity, label normalization, and priority class ordering.
  2. Statistical primitives (linear percentiles, sample summaries, NaN/Inf filtering).
  3. Kubernetes resource.Quantity binary/decimal SI parsing, RFC3339 timestamps, and number/table formatting.
  4. Section 1 (Gauges), Section 2 (Counters & 30s sliding-window increases), and Section 3 (Histograms & quantiles).
  5. Section 5.1 (ClusterQueue & hierarchical Cohort quota utilization).
  6. Section 5.2 (Admission & quota-reserved wait time histograms across priority classes).
  7. Section 5.3 (Total evictions, unique evictions, reclaims, and preemptions).
  8. Section 5.4 (Effective uninterrupted cluster saturation: multi-pod TPU/GPU/CPU/memory accounting,
     limits-only extended resources, multi-container & initContainer semantics, pod overhead,
     multi-podSetAssignments, active-at-end vs completed vs preempted/evicted/failed workloads,
     pre-existing workloads, system namespace exclusion, and JobSet/Job deduplication).
  9. Executable reproduction tests for all 10 discovered discrepancies (D1-D10).

Run with:
  python3 deploy/scripts/test_kueue_metrics_report.py -v
"""

from __future__ import annotations

import io
import math
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import kueue_metrics_report as kmr  # noqa: E402


def _make_nodes_2x4_tpu() -> list[dict]:
    """Two identical 4-TPU, 180-core, 720-GiB, 110-pod nodes (cluster total: 8 TPUs, 360 cores, 1440 GiB)."""
    return [
        {
            "metadata": {"name": "kwok-tpu-node-0"},
            "status": {
                "allocatable": {
                    "google.com/tpu": "4",
                    "cpu": "180",
                    "memory": "720Gi",
                    "pods": "110",
                }
            },
        },
        {
            "metadata": {"name": "kwok-tpu-node-1"},
            "status": {
                "allocatable": {
                    "google.com/tpu": "4",
                    "cpu": "180",
                    "memory": "720Gi",
                    "pods": "110",
                }
            },
        },
    ]


def _rows_by_key(rows: list[list[str]], key_col: int = 0) -> dict[str, list[str]]:
    return {r[key_col]: r for r in rows}


class TestCatalogAndLabelHelpers(unittest.TestCase):
    """Sanity checks on KUEUE_METRICS_CATALOG, format_labels, and priority sorting."""

    def test_catalog_entries_unique_and_well_formed(self) -> None:
        self.assertGreaterEqual(len(kmr.KUEUE_METRICS_CATALOG), 50)
        seen_names: set[str] = set()
        for spec in kmr.KUEUE_METRICS_CATALOG:
            self.assertIsInstance(spec, kmr.MetricSpec)
            self.assertTrue(spec.section.strip())
            self.assertTrue(spec.name.strip())
            self.assertNotIn(spec.name, seen_names, f"Duplicate catalog entry: {spec.name}")
            seen_names.add(spec.name)
            self.assertIn(
                spec.metric_type,
                (kmr.MetricType.GAUGE, kmr.MetricType.COUNTER, kmr.MetricType.HISTOGRAM),
            )
            self.assertFalse(spec.name.endswith(("_bucket", "_sum", "_count")))

    def test_format_labels_excludes_ignored_and_empty_values(self) -> None:
        raw = {
            "__name__": "kueue_pending_workloads",
            "instance": "10.0.0.1:8080",
            "job": "kueue-metrics",
            "component": "controller",
            "replica_role": "leader",
            "cluster_queue": "cq-main",
            "status": "active",
            "underlying_cause": "",
        }
        self.assertEqual(kmr.format_labels(raw), "cluster_queue=cq-main\nstatus=active")
        self.assertEqual(
            kmr.format_labels(raw, extra_exclude={"status"}),
            "cluster_queue=cq-main",
        )
        self.assertEqual(kmr.format_labels({"__name__": "foo", "instance": "x"}), "default")

    def test_priority_sort_key_orders_highest_to_lowest(self) -> None:
        raw = ["low-prio", "custom-tier", "high-prio", "mid-prio", "hero-prod", "bg-spot"]
        ordered = sorted(raw, key=kmr._priority_sort_key)
        self.assertEqual(
            ordered,
            ["hero-prod", "high-prio", "mid-prio", "bg-spot", "low-prio", "custom-tier"],
        )


class TestPrimitivesParsersAndFormatters(unittest.TestCase):
    """Unit tests for statistical helpers, Kubernetes quantity parsers, timestamps, and table wrapping."""

    def test_compute_percentile_and_summarize_samples(self) -> None:
        self.assertTrue(math.isnan(kmr.compute_percentile([], 0.5)))
        self.assertAlmostEqual(kmr.compute_percentile([42.0], 0.5), 42.0)
        self.assertAlmostEqual(kmr.compute_percentile([42.0], 0.99), 42.0)

        # 5-element sequence [10, 20, 30, 40, 50]: P50 = 30, P90 = 46, AVG = 30
        vals = [50.0, 10.0, float("nan"), 30.0, 20.0, float("inf"), 40.0]
        summary = kmr.summarize_samples(vals)
        self.assertIsNotNone(summary)
        assert summary is not None
        p50, p90, p99, avg = summary
        self.assertAlmostEqual(p50, 30.0)
        self.assertAlmostEqual(p90, 46.0)
        self.assertAlmostEqual(p99, 49.6)
        self.assertAlmostEqual(avg, 30.0)
        self.assertLessEqual(p50, p90)
        self.assertLessEqual(p90, p99)

        self.assertIsNone(kmr.summarize_samples([float("nan"), float("inf"), float("-inf")]))

    def test_parse_k8s_qty_binary_and_decimal_units(self) -> None:
        # CPU: millicores & whole cores
        self.assertAlmostEqual(kmr._parse_k8s_qty("500m", "cpu"), 0.5)
        self.assertAlmostEqual(kmr._parse_k8s_qty("112", "cpu"), 112.0)
        self.assertAlmostEqual(kmr._parse_k8s_qty("2k", "cpu"), 2000.0)

        # Memory: always returned in GiB
        self.assertAlmostEqual(kmr._parse_k8s_qty("1Gi", "memory"), 1.0)
        self.assertAlmostEqual(kmr._parse_k8s_qty("512Mi", "memory"), 0.5)
        self.assertAlmostEqual(kmr._parse_k8s_qty("1048576Ki", "memory"), 1.0)
        self.assertAlmostEqual(kmr._parse_k8s_qty("2Ti", "memory"), 2048.0)
        self.assertAlmostEqual(kmr._parse_k8s_qty(1024**3, "memory"), 1.0)

        # Accelerators / scalar quantities
        self.assertAlmostEqual(kmr._parse_k8s_qty("4", "google.com/tpu"), 4.0)
        self.assertAlmostEqual(kmr._parse_k8s_qty(8, "nvidia.com/gpu"), 8.0)
        self.assertAlmostEqual(kmr._parse_k8s_qty(None, "cpu"), 0.0)
        self.assertAlmostEqual(kmr._parse_k8s_qty("", "cpu"), 0.0)
        self.assertAlmostEqual(kmr._parse_k8s_qty("invalid", "cpu"), 0.0)

    def test_parse_rfc3339_ts_formats(self) -> None:
        ts_z = kmr._parse_rfc3339_ts("2026-01-01T00:00:00Z")
        ts_nano = kmr._parse_rfc3339_ts("2026-01-01T00:00:00.500000000Z")
        ts_offset = kmr._parse_rfc3339_ts("2026-01-01T01:00:00+01:00")
        assert ts_z is not None and ts_nano is not None and ts_offset is not None
        self.assertAlmostEqual(ts_z, ts_offset)
        self.assertAlmostEqual(ts_nano - ts_z, 0.5, places=5)
        self.assertIsNone(kmr._parse_rfc3339_ts(None))
        self.assertIsNone(kmr._parse_rfc3339_ts(""))
        self.assertIsNone(kmr._parse_rfc3339_ts("not-a-timestamp"))

    def test_wrap_cell_and_print_table_respects_max_width(self) -> None:
        buf = io.StringIO()
        with patch("sys.stdout", buf):
            kmr.print_table(
                ["SECTION", "METRIC", "LABELS", "P50", "P90", "P99", "AVG"],
                [
                    [
                        "ClusterQueue Resources (Optional)",
                        "kueue_cluster_queue_resource_usage",
                        "cluster_queue=very-long-production-queue-name, flavor=tpu-slice, resource=google.com/tpu",
                        "50.00",
                        "90.00",
                        "99.00",
                        "64.25",
                    ]
                ],
                max_width=100,
            )
        rendered_lines = buf.getvalue().splitlines()
        self.assertGreater(len(rendered_lines), 2)
        for line in rendered_lines:
            self.assertLessEqual(len(line), 100, f"Line exceeded max_width=100: {line!r}")


class TestSections1To3GaugeCounterHistogram(unittest.TestCase):
    """Tests for Section 1 (Gauges), Section 2 (Counters), and Section 3 (Histograms)."""

    def test_report_gauge_filters_pre_start_samples(self) -> None:
        series = [
            {
                "metric": {"__name__": "kueue_pending_workloads", "cluster_queue": "cq-a", "status": "active"},
                "values": [
                    [990.0, "999"],   # Before start=1000.0 -> excluded
                    [1000.0, "10"],
                    [1010.0, "20"],
                    [1020.0, "30"],
                ],
            }
        ]
        rows = kmr.report_gauge(series, start=1000.0)
        self.assertEqual(len(rows), 1)
        lbl, p50, p90, p99, avg = rows[0]
        self.assertEqual(lbl, "cluster_queue=cq-a\nstatus=active")
        self.assertEqual(p50, "20.00")
        self.assertEqual(p90, "28.00")
        self.assertEqual(p99, "29.80")
        self.assertEqual(avg, "20.00")

    def test_report_counter_30s_window_and_total_increase(self) -> None:
        # Counter starts at baseline 100 at t=999 (before start=1000), increases linearly by 1/s up to t=1060 (value 160)
        values = [[999.0, "100"]] + [[1000.0 + i, str(100 + i)] for i in range(61)]
        series = [
            {
                "metric": {"__name__": "kueue_admitted_workloads_total", "cluster_queue": "cq-a"},
                "values": values,
            }
        ]
        rows = kmr.report_counter(series, start=1000.0, end=1060.0, step=1.0, window_sec=30.0)
        self.assertEqual(len(rows), 1)
        lbl, p50, p90, p99, _avg, total_inc = rows[0]
        self.assertEqual(lbl, "cluster_queue=cq-a")
        # For t in [1030..1060] (31 of 61 samples), 30s window increase is exact 30.00 -> P50 = 30.00, P90 = 30.00
        self.assertEqual(p50, "30.00")
        self.assertEqual(p90, "30.00")
        self.assertEqual(p99, "30.00")
        self.assertEqual(total_inc, "60.00")

    def test_report_histogram_baseline_subtraction_and_single_observation_max(self) -> None:
        spec = kmr.MetricSpec(
            section="ClusterQueue Status",
            name="kueue_admission_wait_time_seconds",
            metric_type=kmr.MetricType.HISTOGRAM,
            labels=("cluster_queue", "priority_class"),
            description="Wait time",
        )
        m_base = {"cluster_queue": "cq-a", "priority_class": "high"}
        # Before test start: 2 old observations in <=10s bucket (sum=12.0, count=2)
        start_by_name = {
            "kueue_admission_wait_time_seconds_bucket": [
                {"metric": {**m_base, "le": "1"}, "value": [999, "0"]},
                {"metric": {**m_base, "le": "10"}, "value": [999, "2"]},
                {"metric": {**m_base, "le": "+Inf"}, "value": [999, "2"]},
            ],
            "kueue_admission_wait_time_seconds_sum": [{"metric": m_base, "value": [999, "12.0"]}],
            "kueue_admission_wait_time_seconds_count": [{"metric": m_base, "value": [999, "2"]}],
        }
        # At test end: 1 new observation of exact 4.25s in <=10s bucket (end sum=16.25, end count=3)
        end_by_name = {
            "kueue_admission_wait_time_seconds_bucket": [
                {"metric": {**m_base, "le": "1"}, "value": [1100, "0"]},
                {"metric": {**m_base, "le": "10"}, "value": [1100, "3"]},
                {"metric": {**m_base, "le": "+Inf"}, "value": [1100, "3"]},
            ],
            "kueue_admission_wait_time_seconds_sum": [{"metric": m_base, "value": [1100, "16.25"]}],
            "kueue_admission_wait_time_seconds_count": [{"metric": m_base, "value": [1100, "3"]}],
        }
        rows = kmr.report_histogram(end_by_name, start_by_name, spec)
        self.assertEqual(len(rows), 1)
        _gkey, p50, p90, hmax, avg, cnt, buckets = rows[0]
        self.assertEqual(cnt, "1")
        self.assertEqual(avg, "4.25s")
        # For count == 1, MAX is exact observation sum (4.25s)
        self.assertEqual(hmax, "4.25s")
        # Linear interpolation on [1, 10] with 1 observation: P50 = 1 + 0.5*(10-1) = 5.50s, P90 = 1 + 0.9*9 = 9.10s
        self.assertEqual(p50, "5.50s")
        self.assertEqual(p90, "9.10s")
        self.assertEqual(buckets, "<=10s:1")


class TestSection51to53Aggregations(unittest.TestCase):
    """Tests for Section 5.1 (Quota Utilization), Section 5.2 (Wait Time), and Section 5.3 (Evictions & Reclaims)."""

    def test_section_5_1_cluster_queue_and_hierarchical_cohort_deduplication(self) -> None:
        # Two ClusterQueues (cq-a, cq-b) each with nominal quota 4 TPUs (total 8 TPUs),
        # inside child cohorts under root-cohort (hierarchical subtree).
        # Verify root-cohort quota is NOT double-counted with child cohort quotas!
        range_by_name = {
            "kueue_cluster_queue_nominal_quota": [
                {
                    "metric": {"cluster_queue": "cq-a", "cohort": "child-a", "flavor": "tpu-pool", "resource": "google.com/tpu"},
                    "values": [[1000.0, "4"], [1010.0, "4"]],
                },
                {
                    "metric": {"cluster_queue": "cq-b", "cohort": "child-b", "flavor": "tpu-pool", "resource": "google.com/tpu"},
                    "values": [[1000.0, "4"], [1010.0, "4"]],
                },
            ],
            "kueue_cluster_queue_resource_usage": [
                {
                    "metric": {"cluster_queue": "cq-a", "cohort": "child-a", "flavor": "tpu-pool", "resource": "google.com/tpu"},
                    "values": [[1000.0, "4"], [1010.0, "4"]],
                },
                {
                    "metric": {"cluster_queue": "cq-b", "cohort": "child-b", "flavor": "tpu-pool", "resource": "google.com/tpu"},
                    "values": [[1000.0, "0"], [1010.0, "4"]],
                },
            ],
            "kueue_cohort_info": [
                {"metric": {"cohort": "root", "parent_cohort": ""}, "values": [[1000.0, "1"]]},
                {"metric": {"cohort": "child-a", "parent_cohort": "root"}, "values": [[1000.0, "1"]]},
                {"metric": {"cohort": "child-b", "parent_cohort": "root"}, "values": [[1000.0, "1"]]},
            ],
            "kueue_cohort_subtree_quota": [
                {"metric": {"cohort": "root", "flavor": "tpu-pool", "resource": "google.com/tpu"}, "values": [[1000.0, "8"], [1010.0, "8"]]},
                {"metric": {"cohort": "child-a", "flavor": "tpu-pool", "resource": "google.com/tpu"}, "values": [[1000.0, "4"], [1010.0, "4"]]},
                {"metric": {"cohort": "child-b", "flavor": "tpu-pool", "resource": "google.com/tpu"}, "values": [[1000.0, "4"], [1010.0, "4"]]},
            ],
            "kueue_cohort_subtree_resource_reservations": [
                {"metric": {"cohort": "root", "flavor": "tpu-pool", "resource": "google.com/tpu"}, "values": [[1000.0, "4"], [1010.0, "8"]]},
                {"metric": {"cohort": "child-a", "flavor": "tpu-pool", "resource": "google.com/tpu"}, "values": [[1000.0, "4"], [1010.0, "4"]]},
                {"metric": {"cohort": "child-b", "flavor": "tpu-pool", "resource": "google.com/tpu"}, "values": [[1000.0, "0"], [1010.0, "4"]]},
            ],
        }
        rows = kmr.report_aggregated_quota_utilization(range_by_name, start=1000.0)
        by_scope = _rows_by_key(rows, 0)
        # At t=1000: total used = 4/8 = 50%; at t=1010: total used = 8/8 = 100% -> AVG = 75.00%, avg used/quota = 6.00 / 8.00
        self.assertEqual(by_scope["ClusterQueue Quotas (All)"][5], "75.00%")
        self.assertEqual(by_scope["ClusterQueue Quotas (All)"][6], "6.00 / 8.00")
        self.assertEqual(by_scope["Cohort Quotas (All)"][5], "75.00%")
        self.assertEqual(by_scope["Cohort Quotas (All)"][6], "6.00 / 8.00")

    def test_section_5_2_wait_time_across_priorities_and_fallback_metric(self) -> None:
        # Test fallback to kueue_quota_reserved_wait_time_seconds when admission_wait_time_seconds is absent
        end_by_name = {
            "kueue_quota_reserved_wait_time_seconds_bucket": [
                {"metric": {"cluster_queue": "cq-1", "priority_class": "high", "le": "10"}, "value": [1100, "2"]},
                {"metric": {"cluster_queue": "cq-1", "priority_class": "high", "le": "+Inf"}, "value": [1100, "2"]},
                {"metric": {"cluster_queue": "cq-1", "priority_class": "low", "le": "10"}, "value": [1100, "0"]},
                {"metric": {"cluster_queue": "cq-1", "priority_class": "low", "le": "100"}, "value": [1100, "2"]},
                {"metric": {"cluster_queue": "cq-1", "priority_class": "low", "le": "+Inf"}, "value": [1100, "2"]},
            ],
            "kueue_quota_reserved_wait_time_seconds_sum": [
                {"metric": {"cluster_queue": "cq-1", "priority_class": "high"}, "value": [1100, "8.0"]},
                {"metric": {"cluster_queue": "cq-1", "priority_class": "low"}, "value": [1100, "120.0"]},
            ],
            "kueue_quota_reserved_wait_time_seconds_count": [
                {"metric": {"cluster_queue": "cq-1", "priority_class": "high"}, "value": [1100, "2"]},
                {"metric": {"cluster_queue": "cq-1", "priority_class": "low"}, "value": [1100, "2"]},
            ],
        }
        rows = kmr.report_aggregated_wait_time_by_priority(end_by_name, {})
        self.assertEqual([r[0] for r in rows], ["high", "low", "ALL (Across All Priorities)"])
        self.assertEqual(rows[0][4], "4.00s")   # 8.0 / 2
        self.assertEqual(rows[1][4], "60.00s")  # 120.0 / 2
        self.assertEqual(rows[2][4], "32.00s")  # 128.0 / 4
        self.assertEqual(rows[2][5], "4")

    def test_section_5_3_evictions_reclaims_and_preemptions(self) -> None:
        range_by_name = {
            "kueue_evicted_workloads_total": [
                {
                    "metric": {"cluster_queue": "cq-a", "reason": "Preempted", "underlying_cause": "InCohortReclamation", "priority_class": "low"},
                    "values": [[1000.0, "0"], [1050.0, "3"]],
                },
                {
                    "metric": {"cluster_queue": "cq-a", "reason": "Preempted", "underlying_cause": "InClusterQueue", "priority_class": "low"},
                    "values": [[1000.0, "0"], [1050.0, "2"]],
                },
            ],
            "kueue_evicted_workloads_once_total": [
                {
                    "metric": {"cluster_queue": "cq-a", "reason": "Preempted", "underlying_cause": "", "priority_class": "low"},
                    "values": [[1000.0, "0"], [1050.0, "4"]],
                }
            ],
            "kueue_preempted_workloads_total": [
                {
                    "metric": {"preempting_cluster_queue": "cq-b", "reason": "InCohortReclamation"},
                    "values": [[1000.0, "0"], [1050.0, "3"]],
                },
                {
                    "metric": {"preempting_cluster_queue": "cq-a", "reason": "InClusterQueue"},
                    "values": [[1000.0, "0"], [1050.0, "2"]],
                },
            ],
        }
        summary_rows, breakdown_rows = kmr.report_aggregated_evictions_and_reclaims(range_by_name, start=1000.0)
        by_cat = _rows_by_key(summary_rows, 0)
        self.assertEqual(by_cat["Total Evictions"][2], "5")
        self.assertEqual(by_cat["Total Reclaims"][2], "3")
        self.assertEqual(by_cat["Total Preemptions"][2], "5")
        self.assertEqual(len(breakdown_rows), 4)


class TestSection54EffectiveUninterruptedSaturation(unittest.TestCase):
    """Comprehensive tests for Section 5.4 (Effective Uninterrupted Cluster Saturation)."""

    T0 = 1700000000.0
    T1 = 1700003600.0  # Exact 1-hour (3600s) evaluation window
    TS0 = "2023-11-14T22:13:20Z"
    TS1 = "2023-11-14T23:13:20Z"

    def test_2_pods_x_4_tpus_on_2_x_4_tpu_nodes_for_1h_gives_100pct(self) -> None:
        """Canonical prompt scenario: 2-pod workload x 4 TPUs/pod on 2x4-TPU nodes for 1h = 100.00%."""
        for mode in ("requests", "limits_only", "multi_podset_assignments", "pod_fallback"):
            with self.subTest(mode=mode):
                if mode == "requests":
                    c_res = {"requests": {"google.com/tpu": "4", "cpu": "180", "memory": "720Gi"}}
                    psa = []
                    pods = []
                elif mode == "limits_only":
                    c_res = {"requests": {"cpu": "180", "memory": "720Gi"}, "limits": {"google.com/tpu": "4"}}
                    psa = []
                    pods = []
                elif mode == "multi_podset_assignments":
                    c_res = {"requests": {"cpu": "180", "memory": "720Gi"}}
                    psa = [
                        {"name": "leader", "count": 1, "resourceUsage": {"google.com/tpu": "4"}},
                        {"name": "worker", "count": 1, "resourceUsage": {"google.com/tpu": "4"}},
                    ]
                    pods = []
                else:  # pod_fallback
                    c_res = {"requests": {"cpu": "180", "memory": "720Gi"}}
                    psa = []
                    pods = [
                        {
                            "metadata": {"name": f"job-pod-{idx}", "namespace": "default", "labels": {"jobset.sigs.k8s.io/jobset-name": "jobset-2x4"}},
                            "spec": {"containers": [{"name": "main", "resources": {"limits": {"google.com/tpu": "4"}}}]},
                            "status": {"phase": "Succeeded", "startTime": self.TS0},
                        }
                        for idx in range(2)
                    ]

                snap = {
                    "nodes": _make_nodes_2x4_tpu(),
                    "workloads": [
                        {
                            "metadata": {
                                "name": "jobset-2x4-wl",
                                "namespace": "default",
                                "ownerReferences": [{"kind": "JobSet", "name": "jobset-2x4"}],
                            },
                            "spec": {
                                "podSets": [
                                    {
                                        "name": "worker",
                                        "count": 2,
                                        "template": {
                                            "metadata": {"annotations": {"kueue-optimizer.gke.io/pod-execution-time-seconds": "3600"}},
                                            "spec": {"containers": [{"name": "trainer", "resources": c_res}]},
                                        },
                                    }
                                ]
                            },
                            "status": {
                                "admission": {"podSetAssignments": psa} if psa else {},
                                "conditions": [
                                    {"type": "Admitted", "status": "True", "lastTransitionTime": self.TS0},
                                    {"type": "Finished", "status": "True", "reason": "Succeeded", "lastTransitionTime": self.TS1},
                                ],
                            },
                        }
                    ],
                    "pods": pods,
                }
                rows, subtitle = kmr.report_effective_uninterrupted_saturation(snap, self.T0, self.T1)
                by_res = _rows_by_key(rows, 0)
                self.assertEqual(by_res["google.com/tpu"][2], "100.00%")
                self.assertEqual(by_res["cpu"][2], "100.00%")
                self.assertEqual(by_res["memory"][2], "100.00%")
                self.assertIn("completed=1 [2 pods]", subtitle)

    def test_multi_container_init_container_and_overhead_pod_math(self) -> None:
        # 2 regular containers: c1 (2 TPU, 40 cpu) + c2 (2 TPU, 60 cpu) => 4 TPU, 100 cpu
        # initContainer requests 120 cpu (larger than 100 cpu) and 1 TPU (smaller than 4 TPU) => max gives 4 TPU, 120 cpu
        # pod overhead adds 10 cpu => 130 cpu per pod
        p_spec = {
            "overhead": {"cpu": "10"},
            "containers": [
                {"name": "c1", "resources": {"requests": {"cpu": "40"}, "limits": {"google.com/tpu": "2"}}},
                {"name": "c2", "resources": {"requests": {"cpu": "60"}, "limits": {"google.com/tpu": "2"}}},
            ],
            "initContainers": [
                {"name": "init", "resources": {"requests": {"cpu": "120", "google.com/tpu": "1"}}},
            ],
        }
        per_pod = kmr._extract_pod_spec_resources(p_spec)
        self.assertAlmostEqual(per_pod["google.com/tpu"], 4.0)
        self.assertAlmostEqual(per_pod["cpu"], 130.0)

    def test_pod_level_limit_estimate_reconciles_partial_admission(self) -> None:
        # Each Pod requests 1 CPU, but Kueue reserves the pod-level limit of 4 for the one admitted Pod
        # (kubernetes-sigs/kueue#15702), so the admitted resourceUsage exceeds the template total of 2.
        wl = {
            "spec": {
                "podSets": [
                    {
                        "name": "main",
                        "count": 2,
                        "minCount": 1,
                        "template": {
                            "spec": {
                                "resources": {"limits": {"cpu": "4"}},
                                "containers": [{"name": "main", "resources": {"requests": {"cpu": "1"}}}],
                            }
                        },
                    }
                ]
            },
            "status": {"admission": {"podSetAssignments": [{"name": "main", "count": 1, "resourceUsage": {"cpu": "4"}}]}},
        }
        totals, _ = kmr._extract_workload_resource_size_and_cfg_duration(wl)
        self.assertEqual(totals["cpu"], 4.0)

    def test_active_at_end_partial_vs_full_duration_and_interrupted_exclusion(self) -> None:
        # 1h eval window [T0, T0+3600]:
        # - wl-active: 8 TPUs admitted at T0 + 1800s (halfway through), configured for 3600s, still running at T1
        #   -> contributes 1800s * 8 = 14,400 TPU-s = 50.00% EFFECTIVE SAT, 100.00% FULL-DUR SAT.
        # - wl-preempted: 8 TPUs ran for 1800s then Evicted=True (reason=Preempted)
        #   -> contributes exact 0.00%, counted under Excluded (preempted/reclaimed/failed): 1 [2 pods].
        ts_mid = "2023-11-14T22:43:20Z"
        snap = {
            "nodes": _make_nodes_2x4_tpu(),
            "workloads": [
                {
                    "metadata": {"name": "wl-active", "namespace": "default"},
                    "spec": {
                        "podSets": [
                            {
                                "count": 2,
                                "template": {
                                    "metadata": {"annotations": {"kueue-optimizer.gke.io/pod-execution-time-seconds": "3600"}},
                                    "spec": {"containers": [{"name": "c", "resources": {"limits": {"google.com/tpu": "4"}}}]},
                                },
                            }
                        ]
                    },
                    "status": {"conditions": [{"type": "Admitted", "status": "True", "lastTransitionTime": ts_mid}]},
                },
                {
                    "metadata": {"name": "wl-preempted", "namespace": "default"},
                    "spec": {
                        "podSets": [
                            {
                                "count": 2,
                                "template": {
                                    "metadata": {"annotations": {"kueue-optimizer.gke.io/pod-execution-time-seconds": "3600"}},
                                    "spec": {"containers": [{"name": "c", "resources": {"limits": {"google.com/tpu": "4"}}}]},
                                },
                            }
                        ]
                    },
                    "status": {
                        "conditions": [
                            {"type": "Admitted", "status": "False", "reason": "Evicted", "lastTransitionTime": ts_mid},
                            {"type": "Evicted", "status": "True", "reason": "Preempted", "lastTransitionTime": ts_mid},
                        ]
                    },
                },
            ],
            "pods": [],
        }
        rows, subtitle = kmr.report_effective_uninterrupted_saturation(snap, self.T0, self.T1)
        by_res = _rows_by_key(rows, 0)
        self.assertEqual(by_res["google.com/tpu"][2], "50.00%")   # EFFECTIVE SAT
        self.assertEqual(by_res["google.com/tpu"][3], "0.00%")    # COMPLETED ONLY
        self.assertEqual(by_res["google.com/tpu"][4], "50.00%")   # ACTIVE AT END
        self.assertEqual(by_res["google.com/tpu"][5], "100.00%")  # FULL-DUR SAT
        self.assertIn("active_at_end=1 [2 pods]", subtitle)
        self.assertIn("Excluded (preempted/reclaimed/failed): 1 [2 pods]", subtitle)

    def test_jobset_parent_deduplicates_child_job_workload(self) -> None:
        # Both parent JobSet Workload AND child batch/v1 Job Workload exist in snapshot["workloads"].
        # Must count 100.00% (1 workload), NOT 200.00% (2 workloads)!
        snap = {
            "nodes": _make_nodes_2x4_tpu(),
            "workloads": [
                {
                    "metadata": {
                        "name": "jobset-train-parent",
                        "namespace": "default",
                        "ownerReferences": [{"kind": "JobSet", "name": "train-js"}],
                    },
                    "spec": {
                        "podSets": [
                            {
                                "count": 2,
                                "template": {
                                    "metadata": {"annotations": {"kueue-optimizer.gke.io/pod-execution-time-seconds": "3600"}},
                                    "spec": {"containers": [{"name": "c", "resources": {"limits": {"google.com/tpu": "4"}}}]},
                                },
                            }
                        ]
                    },
                    "status": {
                        "conditions": [
                            {"type": "Admitted", "status": "True", "lastTransitionTime": self.TS0},
                            {"type": "Finished", "status": "True", "reason": "Succeeded", "lastTransitionTime": self.TS1},
                        ]
                    },
                },
                {
                    "metadata": {
                        "name": "job-train-js-worker-0-child",
                        "namespace": "default",
                        "ownerReferences": [{"kind": "Job", "name": "train-js-worker-0"}],
                    },
                    "spec": {
                        "podSets": [
                            {
                                "count": 2,
                                "template": {
                                    "metadata": {
                                        "labels": {"jobset.sigs.k8s.io/jobset-name": "train-js"},
                                        "annotations": {"kueue-optimizer.gke.io/pod-execution-time-seconds": "3600"},
                                    },
                                    "spec": {"containers": [{"name": "c", "resources": {"limits": {"google.com/tpu": "4"}}}]},
                                },
                            }
                        ]
                    },
                    "status": {
                        "conditions": [
                            {"type": "Admitted", "status": "True", "lastTransitionTime": self.TS0},
                            {"type": "Finished", "status": "True", "reason": "Succeeded", "lastTransitionTime": self.TS1},
                        ]
                    },
                },
            ],
            "pods": [],
        }
        rows, subtitle = kmr.report_effective_uninterrupted_saturation(snap, self.T0, self.T1)
        by_res = _rows_by_key(rows, 0)
        self.assertEqual(by_res["google.com/tpu"][2], "100.00%")
        self.assertIn("completed=1 [2 pods]", subtitle)


class TestRegressionFixesForDiscrepanciesD1ToD10(unittest.TestCase):
    """Regression tests verifying all 10 fixes (D1-D10) across live code paths and edge cases."""

    def test_D1_live_snapshot_fallback_handles_missing_kubeconfig_without_name_error(self) -> None:
        """D1 fix: load_or_fetch_cluster_snapshot('', ns) returns None cleanly without NameError on os."""
        with patch.dict(os.environ, {"KUBECONFIG": "/nonexistent-kubeconfig-path"}, clear=False):
            result = kmr.load_or_fetch_cluster_snapshot("", "nonexistent-chamber-ns")
        self.assertIsNone(result)

    def test_D2_counter_reset_on_controller_restart_preserves_full_monotonic_increase(self) -> None:
        """D2 fix: Counter reset (10 -> 25 -> 0 -> 8) accumulates exact total increase = (25 - 10) + 8 = 23.0."""
        series = {
            "metric": {"__name__": "kueue_evicted_workloads_total", "cluster_queue": "cq-a", "reason": "Preempted"},
            "values": [
                [999.0, "10"],   # Pre-start baseline = 10
                [1010.0, "25"],  # +15 before controller restart
                [1020.0, "0"],   # Controller pod restarted -> counter reset to 0
                [1030.0, "8"],   # +8 after restart => true total increase is 15 + 8 = 23
            ],
        }
        actual = kmr.counter_series_total_increase(series, start=1000.0)
        self.assertAlmostEqual(actual, 23.0)

        rows = kmr.report_counter([series], start=1000.0, end=1030.0, step=10.0, window_sec=30.0)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][5], "23.00")

    def test_D3_ha_replica_follower_never_overwrites_leader_histogram_or_quota(self) -> None:
        """D3 fix: In HA deployments (leader + follower), per-replica deltas are summed without overwrite."""
        spec = kmr.MetricSpec(
            section="ClusterQueue Status",
            name="kueue_admission_wait_time_seconds",
            metric_type=kmr.MetricType.HISTOGRAM,
            labels=("cluster_queue", "priority_class", "replica_role"),
            description="Wait time",
        )
        # Leader pod recorded count=5, sum=50.0s; follower pod recorded count=0, sum=0.0s (listed second)
        end_by_name = {
            "kueue_admission_wait_time_seconds_bucket": [
                {"metric": {"cluster_queue": "cq-a", "priority_class": "high", "replica_role": "leader", "instance": "pod-0", "le": "10"}, "value": [1100, "5"]},
                {"metric": {"cluster_queue": "cq-a", "priority_class": "high", "replica_role": "leader", "instance": "pod-0", "le": "+Inf"}, "value": [1100, "5"]},
                {"metric": {"cluster_queue": "cq-a", "priority_class": "high", "replica_role": "follower", "instance": "pod-1", "le": "10"}, "value": [1100, "0"]},
                {"metric": {"cluster_queue": "cq-a", "priority_class": "high", "replica_role": "follower", "instance": "pod-1", "le": "+Inf"}, "value": [1100, "0"]},
            ],
            "kueue_admission_wait_time_seconds_sum": [
                {"metric": {"cluster_queue": "cq-a", "priority_class": "high", "replica_role": "leader", "instance": "pod-0"}, "value": [1100, "50.0"]},
                {"metric": {"cluster_queue": "cq-a", "priority_class": "high", "replica_role": "follower", "instance": "pod-1"}, "value": [1100, "0.0"]},
            ],
            "kueue_admission_wait_time_seconds_count": [
                {"metric": {"cluster_queue": "cq-a", "priority_class": "high", "replica_role": "leader", "instance": "pod-0"}, "value": [1100, "5"]},
                {"metric": {"cluster_queue": "cq-a", "priority_class": "high", "replica_role": "follower", "instance": "pod-1"}, "value": [1100, "0"]},
            ],
        }
        rows = kmr.report_histogram(end_by_name, {}, spec)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][4], "10.00s")  # 50.0 / 5 = 10.00s
        self.assertEqual(rows[0][5], "5")

    def test_D4_section_5_4_inflated_cfg_dur_capped_by_actual_in_window_runtime(self) -> None:
        """D4 fix: Two sequential 1800s workloads carrying cfg_dur=3600s on an 8-TPU cluster report exact 100.00%."""
        t0 = 1700000000.0
        t1 = t0 + 3600.0
        ts0 = "2023-11-14T22:13:20Z"
        ts_mid = "2023-11-14T22:43:20Z"
        ts1 = "2023-11-14T23:13:20Z"
        snap = {
            "nodes": _make_nodes_2x4_tpu(),
            "workloads": [
                {
                    "metadata": {"name": "wl-1", "namespace": "default"},
                    "spec": {"podSets": [{"count": 2, "template": {"metadata": {"annotations": {"kueue-optimizer.gke.io/pod-execution-time-seconds": "3600"}}, "spec": {"containers": [{"name": "c", "resources": {"limits": {"google.com/tpu": "4"}}}]}}}]},
                    "status": {"conditions": [{"type": "Admitted", "status": "True", "lastTransitionTime": ts0}, {"type": "Finished", "status": "True", "reason": "Succeeded", "lastTransitionTime": ts_mid}]},
                },
                {
                    "metadata": {"name": "wl-2", "namespace": "default"},
                    "spec": {"podSets": [{"count": 2, "template": {"metadata": {"annotations": {"kueue-optimizer.gke.io/pod-execution-time-seconds": "3600"}}, "spec": {"containers": [{"name": "c", "resources": {"limits": {"google.com/tpu": "4"}}}]}}}]},
                    "status": {"conditions": [{"type": "Admitted", "status": "True", "lastTransitionTime": ts_mid}, {"type": "Finished", "status": "True", "reason": "Succeeded", "lastTransitionTime": ts1}]},
                },
            ],
            "pods": [],
        }
        rows, _ = kmr.report_effective_uninterrupted_saturation(snap, t0, t1)
        by_res = _rows_by_key(rows, 0)
        # 1800s + 1800s = 3600s across all 8 TPUs -> exact 100.00% effective saturation!
        self.assertEqual(by_res["google.com/tpu"][2], "100.00%")

    def test_D5_section_5_4_requeued_preempted_workload_credits_only_post_eviction_uninterrupted_window(self) -> None:
        """D5 fix: Preempted-and-requeued workload (requeueState.count=1) credits only post-eviction runtime (600s/3600s = 16.67%)."""
        t0 = 1700000000.0
        t1 = t0 + 3600.0
        ts0 = "2023-11-14T22:13:20Z"         # Initial creation at t0
        ts_readmit = "2023-11-14T23:03:20Z"  # Evicted/re-admitted 600s before t1 (600 / 3600 = 16.67%)
        snap = {
            "nodes": _make_nodes_2x4_tpu(),
            "workloads": [
                {
                    "metadata": {"name": "wl-requeued", "namespace": "default", "creationTimestamp": ts0},
                    "spec": {"podSets": [{"count": 2, "template": {"spec": {"containers": [{"name": "c", "resources": {"limits": {"google.com/tpu": "4"}}}]}}}]},
                    "status": {
                        "requeueState": {"count": 1},
                        "conditions": [
                            {"type": "Admitted", "status": "True"},
                            {"type": "Evicted", "status": "False", "reason": "Preempted", "lastTransitionTime": ts_readmit},
                        ],
                    },
                }
            ],
            "pods": [
                {
                    "metadata": {"name": "stale-pod-0", "namespace": "default", "labels": {"kueue.x-k8s.io/prebuilt-workload-name": "wl-requeued"}},
                    "spec": {"containers": [{"name": "c", "resources": {"limits": {"google.com/tpu": "4"}}}]},
                    "status": {"phase": "Running", "startTime": ts0},
                }
            ],
        }
        rows, _ = kmr.report_effective_uninterrupted_saturation(snap, t0, t1)
        by_res = _rows_by_key(rows, 0)
        self.assertEqual(by_res["google.com/tpu"][2], "16.67%")

    def test_D6_section_5_4_stale_workload_finished_before_start_epoch_is_ignored(self) -> None:
        """D6 fix: Stale pre-test workload finished strictly before start_epoch contributes 0% and 0 completed count."""
        t0 = 1700000000.0
        t1 = t0 + 3600.0
        ts_before_admit = "2023-11-14T21:00:00Z"   # 4400s before t0
        ts_before_finish = "2023-11-14T22:00:00Z"  # 800s before t0
        snap = {
            "nodes": _make_nodes_2x4_tpu(),
            "workloads": [
                {
                    "metadata": {"name": "stale-pre-test-wl", "namespace": "default"},
                    "spec": {"podSets": [{"count": 2, "template": {"metadata": {"annotations": {"kueue-optimizer.gke.io/pod-execution-time-seconds": "3600"}}, "spec": {"containers": [{"name": "c", "resources": {"limits": {"google.com/tpu": "4"}}}]}}}]},
                    "status": {"conditions": [
                        {"type": "Admitted", "status": "True", "lastTransitionTime": ts_before_admit},
                        {"type": "Finished", "status": "True", "reason": "Succeeded", "lastTransitionTime": ts_before_finish},
                    ]},
                }
            ],
            "pods": [],
        }
        rows, subtitle = kmr.report_effective_uninterrupted_saturation(snap, t0, t1)
        by_res = _rows_by_key(rows, 0)
        self.assertIn("completed=0 [0 pods]", subtitle)
        self.assertEqual(by_res["google.com/tpu"][2], "0.00%")
        self.assertEqual(by_res["google.com/tpu"][5], "0.00%")

    def test_D7_report_histogram_enforces_monotonic_cumulative_bucket_counts(self) -> None:
        """D7 fix: Non-monotonic raw bucket deltas are clamped monotonically so differential bins equal total count."""
        spec = kmr.MetricSpec(
            section="ClusterQueue Status",
            name="kueue_admission_wait_time_seconds",
            metric_type=kmr.MetricType.HISTOGRAM,
            labels=("cluster_queue",),
            description="Wait time",
        )
        end_by_name = {
            "kueue_admission_wait_time_seconds_bucket": [
                {"metric": {"cluster_queue": "cq-a", "le": "10"}, "value": [1100, "5"]},
                {"metric": {"cluster_queue": "cq-a", "le": "20"}, "value": [1100, "3"]},
                {"metric": {"cluster_queue": "cq-a", "le": "+Inf"}, "value": [1100, "5"]},
            ],
            "kueue_admission_wait_time_seconds_sum": [{"metric": {"cluster_queue": "cq-a"}, "value": [1100, "25.0"]}],
            "kueue_admission_wait_time_seconds_count": [{"metric": {"cluster_queue": "cq-a"}, "value": [1100, "5"]}],
        }
        rows = kmr.report_histogram(end_by_name, {}, spec)
        self.assertEqual(rows[0][6], "<=10s:5")

    def test_D8_format_num_formats_memory_with_binary_units_and_scalars_with_decimal_si(self) -> None:
        """D8 fix: Memory uses consistent binary Mi/Gi/Ti units; dimensionless counters use decimal M/B."""
        self.assertEqual(kmr.format_num(512 * 1024 * 1024, is_bytes=True), "512.00Mi")
        self.assertEqual(kmr.format_num(1024 * 1024 * 1024, is_bytes=True), "1.00Gi")
        self.assertEqual(kmr.format_num(2_000_000_000.0, is_bytes=False), "2.00B")

    def test_D9_section_5_1_per_cq_label_counts_only_queues_with_positive_nominal_quota(self) -> None:
        """D9 fix: Per-CQ row is omitted when only 1 queue has positive nominal quota (never mislabeled n=2)."""
        range_by_name = {
            "kueue_cluster_queue_nominal_quota": [
                {"metric": {"cluster_queue": "cq-guaranteed", "flavor": "tpu-pool", "resource": "google.com/tpu"}, "values": [[1000.0, "8"]]},
                {"metric": {"cluster_queue": "cq-spot-borrower", "flavor": "tpu-pool", "resource": "google.com/tpu"}, "values": [[1000.0, "0"]]},
            ],
            "kueue_cluster_queue_resource_usage": [
                {"metric": {"cluster_queue": "cq-guaranteed", "flavor": "tpu-pool", "resource": "google.com/tpu"}, "values": [[1000.0, "4"]]},
                {"metric": {"cluster_queue": "cq-spot-borrower", "flavor": "tpu-pool", "resource": "google.com/tpu"}, "values": [[1000.0, "4"]]},
            ],
        }
        rows = kmr.report_aggregated_quota_utilization(range_by_name, start=1000.0)
        by_scope = _rows_by_key(rows, 0)
        # Total usage across both queues (4 + 4 = 8 / 8 = 100.00%) is captured in ClusterQueue Quotas (All),
        # while no misleading 'Per-CQ, n=2' row is emitted when only 1 queue has nominal quota > 0.
        self.assertEqual(by_scope["ClusterQueue Quotas (All)"][5], "100.00%")
        self.assertNotIn("ClusterQueue Quotas (Per-CQ, n=2)", by_scope)

    def test_D10_section_5_3_reclaims_fallback_populates_matching_breakdown_details(self) -> None:
        """D10 fix: Reclaims from kueue_evicted_workloads_total populate both TOTAL='4' and InCohortReclamation=4."""
        range_by_name = {
            "kueue_evicted_workloads_total": [
                {
                    "metric": {
                        "cluster_queue": "cq-a",
                        "reason": "Preempted",
                        "underlying_cause": "InCohortReclamation",
                        "priority_class": "low",
                    },
                    "values": [[1000.0, "0"], [1050.0, "4"]],
                }
            ],
            "kueue_preempted_workloads_total": [],
        }
        summary_rows, _ = kmr.report_aggregated_evictions_and_reclaims(range_by_name, start=1000.0)
        by_cat = _rows_by_key(summary_rows, 0)
        self.assertEqual(by_cat["Total Reclaims"][2], "4")
        self.assertEqual(
            by_cat["Total Reclaims"][3],
            "InCohortReclamation=4, InCohortReclaimWhileBorrowing=0",
        )

    def test_section_5_1_named_per_cq_and_per_cohort_breakdown_rows(self) -> None:
        """Named per-ClusterQueue and per-Cohort rows surface asymmetric multi-queue utilization clearly."""
        range_by_name = {
            "kueue_cluster_queue_nominal_quota": [
                {"metric": {"cluster_queue": "cq-alpha", "flavor": "tpu-pool", "resource": "google.com/tpu"}, "values": [[1000.0, "16"], [1010.0, "16"]]},
                {"metric": {"cluster_queue": "cq-beta", "flavor": "tpu-pool", "resource": "google.com/tpu"}, "values": [[1000.0, "16"], [1010.0, "16"]]},
            ],
            "kueue_cluster_queue_resource_usage": [
                {"metric": {"cluster_queue": "cq-alpha", "flavor": "tpu-pool", "resource": "google.com/tpu"}, "values": [[1000.0, "16"], [1010.0, "16"]]},
                {"metric": {"cluster_queue": "cq-beta", "flavor": "tpu-pool", "resource": "google.com/tpu"}, "values": [[1000.0, "0"], [1010.0, "0"]]},
            ],
        }
        rows = kmr.report_aggregated_quota_utilization(range_by_name, start=1000.0)
        by_scope = _rows_by_key(rows, 0)
        self.assertEqual(by_scope["ClusterQueue Quotas (All)"][5], "50.00%")
        self.assertEqual(by_scope["ClusterQueue Quotas (All)"][6], "16.00 / 32.00")
        self.assertIn("ClusterQueue Quotas (Per-CQ, n=2)", by_scope)
        self.assertEqual(by_scope["ClusterQueue: cq-alpha"][2:7], ["100.00%", "100.00%", "100.00%", "100.00%", "16.00 / 16.00"])
        self.assertEqual(by_scope["ClusterQueue: cq-beta"][2:7], ["0.00%", "0.00%", "0.00%", "0.00%", "0.00 / 16.00"])

    def test_section_5_1_zero_nominal_quota_cq_reports_cohort_share_percentage(self) -> None:
        """Pure-borrower CQs with 0 nominal quota inside a Cohort display % of parent Cohort quota."""
        range_by_name = {
            "kueue_cluster_queue_nominal_quota": [
                {
                    "metric": {
                        "cluster_queue": "cq-team-a",
                        "cohort": "cohort-shared",
                        "flavor": "tpu-pool",
                        "resource": "google.com/tpu",
                    },
                    "values": [[1000.0, "0"], [1010.0, "0"]],
                },
                {
                    "metric": {
                        "cluster_queue": "cq-team-b",
                        "cohort": "cohort-shared",
                        "flavor": "tpu-pool",
                        "resource": "google.com/tpu",
                    },
                    "values": [[1000.0, "0"], [1010.0, "0"]],
                },
            ],
            "kueue_cluster_queue_resource_usage": [
                {
                    "metric": {
                        "cluster_queue": "cq-team-a",
                        "cohort": "cohort-shared",
                        "flavor": "tpu-pool",
                        "resource": "google.com/tpu",
                    },
                    "values": [[1000.0, "16"], [1010.0, "16"]],
                },
                {
                    "metric": {
                        "cluster_queue": "cq-team-b",
                        "cohort": "cohort-shared",
                        "flavor": "tpu-pool",
                        "resource": "google.com/tpu",
                    },
                    "values": [[1000.0, "16"], [1010.0, "16"]],
                },
            ],
            "kueue_cohort_subtree_quota": [
                {
                    "metric": {"cohort": "cohort-shared", "flavor": "tpu-pool", "resource": "google.com/tpu"},
                    "values": [[1000.0, "32"], [1010.0, "32"]],
                }
            ],
            "kueue_cohort_subtree_resource_reservations": [
                {
                    "metric": {"cohort": "cohort-shared", "flavor": "tpu-pool", "resource": "google.com/tpu"},
                    "values": [[1000.0, "32"], [1010.0, "32"]],
                }
            ],
        }
        rows = kmr.report_aggregated_quota_utilization(range_by_name, start=1000.0)
        by_scope = _rows_by_key(rows, 0)
        self.assertEqual(
            by_scope["ClusterQueue: cq-team-a"][2:7],
            ["50.00% (cohort)", "50.00% (cohort)", "50.00% (cohort)", "50.00% (cohort)", "16.00 / 0.00 (borrowed)"],
        )
        self.assertEqual(
            by_scope["ClusterQueue: cq-team-b"][2:7],
            ["50.00% (cohort)", "50.00% (cohort)", "50.00% (cohort)", "50.00% (cohort)", "16.00 / 0.00 (borrowed)"],
        )

    def test_section_5_4_includes_initial_infra_pods_and_unadmitted_inadmissible_workloads(self) -> None:
        """Section 5.4 includes pre-existing infra-pods and classifies Inadmissible jobs as Unadmitted."""
        t0 = 1000.0
        t1 = 1100.0
        snap = {
            "nodes": [
                {"status": {"allocatable": {"google.com/tpu": "4", "cpu": "16", "memory": "64Gi", "pods": "110"}}},
                {"status": {"allocatable": {"google.com/tpu": "4", "cpu": "16", "memory": "64Gi", "pods": "110"}}},
            ],
            "workloads": [
                {
                    "metadata": {"name": "wl-kueue-ok", "namespace": "default"},
                    "spec": {
                        "podSets": [
                            {
                                "count": 1,
                                "template": {
                                    "metadata": {"annotations": {"kueue-optimizer.gke.io/pod-execution-time-seconds": "100"}},
                                    "spec": {"containers": [{"name": "c", "resources": {"requests": {"google.com/tpu": "4"}}}]},
                                },
                            }
                        ]
                    },
                    "status": {
                        "conditions": [
                            {"type": "Admitted", "status": "True", "lastTransitionTime": "1970-01-01T00:16:40Z"},
                            {"type": "Finished", "status": "True", "reason": "Succeeded", "lastTransitionTime": "1970-01-01T00:18:20Z"},
                        ]
                    },
                },
                {
                    "metadata": {"name": "wl-inadmissible", "namespace": "default"},
                    "spec": {
                        "podSets": [
                            {
                                "count": 16,
                                "template": {
                                    "spec": {"containers": [{"name": "c", "resources": {"requests": {"google.com/tpu": "4"}}}]},
                                },
                            }
                        ]
                    },
                    "status": {
                        "conditions": [
                            {"type": "QuotaReserved", "status": "False", "reason": "Inadmissible"},
                            {"type": "Admitted", "status": "False", "reason": "Unadmitted"},
                        ]
                    },
                },
            ],
            "pods": [
                {
                    "metadata": {
                        "name": "legacy-pod-0",
                        "namespace": "infra-pods",
                        "annotations": {"kueue-optimizer.gke.io/pod-execution-time-seconds": "100"},
                    },
                    "spec": {"containers": [{"name": "p", "resources": {"requests": {"google.com/tpu": "4"}}}]},
                    "status": {
                        "phase": "Succeeded",
                        "startTime": "1970-01-01T00:16:40Z",
                        "containerStatuses": [{"state": {"terminated": {"finishedAt": "1970-01-01T00:18:20Z"}}}],
                    },
                }
            ],
        }
        rows, subtitle = kmr.report_effective_uninterrupted_saturation(snap, t0, t1)
        by_res = _rows_by_key(rows, 0)
        self.assertEqual(by_res["google.com/tpu"][2], "100.00%")
        self.assertIn("Initial Infra Pods: 1 (completed=1, active_at_end=0)", subtitle)
        self.assertIn("Excluded (preempted/reclaimed/failed): 0 [0 pods]", subtitle)
        self.assertIn("Unadmitted: 1 [16 pods]", subtitle)

    def test_completed_effective_duration_and_out_of_window_filtering(self) -> None:
        """Completed duration helper caps sub-second runs at 1s and skips out-of-window completions."""
        t0 = 1000.0
        t1 = 1100.0
        # 1. Direct unit checks on _completed_effective_duration
        self.assertEqual(kmr._completed_effective_duration(1101.0, 1101.0, t0, t1, 3600.0), (0.0, 3600.0))
        self.assertEqual(kmr._completed_effective_duration(900.0, 1000.0, t0, t1, 3600.0), (0.0, 3600.0))
        # Same-second completion inside window: capped at min(max_possible, 1.0), NOT max_possible (100.0)
        self.assertEqual(kmr._completed_effective_duration(1050.0, 1050.0, t0, t1, 3600.0), (1.0, 3600.0))
        self.assertEqual(kmr._completed_effective_duration(1050.0, 1050.0, t0, t1, 0.25), (0.25, 0.25))
        # Boundary-straddling completion does NOT snap 1s jitter across start_epoch
        self.assertEqual(kmr._completed_effective_duration(990.0, 1029.0, t0, t1, 40.0), (29.0, 40.0))
        # Fully in-window completion with 1s RFC3339 jitter snaps to configured duration
        self.assertEqual(kmr._completed_effective_duration(1010.0, 1039.0, t0, t1, 30.0), (30.0, 30.0))

        # 2. End-to-end Section 5.4 check: post-window workload (t0+101s) and boundary workload (finished at t0)
        #    must NOT increment completed counts or full_dur_res_sec_qual, and sub-second workload gets 1s credit.
        snap = {
            "nodes": [
                {"status": {"allocatable": {"google.com/tpu": "8", "cpu": "16", "memory": "64Gi", "pods": "110"}}},
            ],
            "workloads": [
                {
                    "metadata": {"name": "wl-post-window", "namespace": "default"},
                    "spec": {
                        "podSets": [
                            {
                                "count": 1,
                                "template": {
                                    "metadata": {"annotations": {"kueue-optimizer.gke.io/pod-execution-time-seconds": "3600"}},
                                    "spec": {"containers": [{"name": "c", "resources": {"requests": {"google.com/tpu": "8"}}}]},
                                },
                            }
                        ]
                    },
                    "status": {
                        "conditions": [
                            {"type": "Admitted", "status": "True", "lastTransitionTime": "1970-01-01T00:18:21Z"},
                            {"type": "Finished", "status": "True", "reason": "Succeeded", "lastTransitionTime": "1970-01-01T00:18:21Z"},
                        ]
                    },
                },
                {
                    "metadata": {"name": "wl-same-second", "namespace": "default"},
                    "spec": {
                        "podSets": [
                            {
                                "count": 1,
                                "template": {
                                    "metadata": {"annotations": {"kueue-optimizer.gke.io/pod-execution-time-seconds": "100"}},
                                    "spec": {"containers": [{"name": "c", "resources": {"requests": {"google.com/tpu": "8"}}}]},
                                },
                            }
                        ]
                    },
                    "status": {
                        "conditions": [
                            {"type": "Admitted", "status": "True", "lastTransitionTime": "1970-01-01T00:17:30Z"},
                            {"type": "Finished", "status": "True", "reason": "Succeeded", "lastTransitionTime": "1970-01-01T00:17:30Z"},
                        ]
                    },
                },
            ],
            "pods": [
                {
                    "metadata": {
                        "name": "infra-post-window",
                        "namespace": "infra-pods",
                        "annotations": {"kueue-optimizer.gke.io/pod-execution-time-seconds": "3600"},
                    },
                    "spec": {"containers": [{"name": "p", "resources": {"requests": {"google.com/tpu": "8"}}}]},
                    "status": {
                        "phase": "Succeeded",
                        "startTime": "1970-01-01T00:18:21Z",
                        "containerStatuses": [{"state": {"terminated": {"finishedAt": "1970-01-01T00:18:21Z"}}}],
                    },
                },
                {
                    "metadata": {
                        "name": "infra-at-start-boundary",
                        "namespace": "infra-pods",
                        "annotations": {"kueue-optimizer.gke.io/pod-execution-time-seconds": "3600"},
                    },
                    "spec": {"containers": [{"name": "p", "resources": {"requests": {"google.com/tpu": "8"}}}]},
                    "status": {
                        "phase": "Succeeded",
                        "startTime": "1970-01-01T00:15:00Z",
                        "containerStatuses": [{"state": {"terminated": {"finishedAt": "1970-01-01T00:16:40Z"}}}],
                    },
                },
            ],
        }
        rows, subtitle = kmr.report_effective_uninterrupted_saturation(snap, t0, t1)
        by_res = _rows_by_key(rows, 0)
        # Only wl-same-second is counted (1s out of 100s = 1.00% EFFECTIVE SAT, 100s = 100.00% FULL-DUR SAT)
        self.assertIn("completed=1 [1 pods]", subtitle)
        self.assertNotIn("Initial Infra Pods:", subtitle)
        self.assertEqual(by_res["google.com/tpu"][2], "1.00%")
        self.assertEqual(by_res["google.com/tpu"][5], "100.00%")

    def test_load_or_fetch_cluster_snapshot_ignores_env_and_tmp_kubeconfigs(self) -> None:
        """load_or_fetch_cluster_snapshot never reads os.environ['KUBECONFIG'] or /tmp/kueue-optimizer-*.kubeconfig."""
        from unittest import mock
        import subprocess

        ns = "kueue-optimizer-chamber-kc-unit-test"
        with tempfile.TemporaryDirectory() as tmp:
            host_kc = Path(tmp) / "host-cluster.kubeconfig"
            host_kc.write_text("apiVersion: v1\nkind: Config\n", encoding="utf-8")
            explicit_kc = Path(tmp) / "explicit-chamber.kubeconfig"
            explicit_kc.write_text("apiVersion: v1\nkind: Config\n", encoding="utf-8")
            tmp_planted = Path(f"/tmp/kueue-optimizer-{ns}.kubeconfig")
            tmp_planted.write_text("apiVersion: v1\nkind: Config\n", encoding="utf-8")
            try:
                with (
                    mock.patch.dict("os.environ", {"KUBECONFIG": str(host_kc)}, clear=False),
                    mock.patch("metrics.saturation.subprocess.run") as m_run,
                ):
                    # 1. Without kwok-<ns>.kubeconfig in repo_root or --kubeconfig, must return None without running kubectl
                    res = kmr.load_or_fetch_cluster_snapshot("", ns)
                    self.assertIsNone(res)
                    m_run.assert_not_called()

                    # 2. With explicit kubeconfig_path, uses ONLY explicit_kc (never host_kc or /tmp)
                    fake_ok = subprocess.CompletedProcess(
                        args=["kubectl"], returncode=0, stdout='{"items": []}', stderr=""
                    )
                    m_run.return_value = fake_ok
                    res2 = kmr.load_or_fetch_cluster_snapshot("", ns, kubeconfig_path=str(explicit_kc))
                    self.assertEqual(res2, {"nodes": [], "workloads": [], "pods": []})
                    for call in m_run.call_args_list:
                        self.assertEqual(call.kwargs["env"]["KUBECONFIG"], str(explicit_kc))
            finally:
                tmp_planted.unlink(missing_ok=True)

    def test_report_gauge_and_section_5_1_clip_post_end_epoch_zero_scrapes_with_short_run_fallback(self) -> None:
        """Gauge & Sec 5.1 exclude post-end_epoch 0% flush scrapes, with fallback if [start, end] had 0 scrapes."""
        gauge_series = [
            {
                "metric": {"cluster_queue": "cq-full"},
                "values": [[1000.0, "8"], [1010.0, "8"], [1020.0, "8"], [1030.0, "8"], [1040.0, "0"]],
            }
        ]
        clipped_g = kmr.report_gauge(gauge_series, start=1000.0, end=1032.0)
        self.assertEqual(clipped_g[0][4], "8.00")

        # Ultra-short <10s run where only post-end scrape at 1008 exists for [1000, 1004]: falls back instead of empty
        short_g = kmr.report_gauge(
            [{"metric": {"cluster_queue": "cq-short"}, "values": [[1008.0, "4"]]}],
            start=1000.0,
            end=1004.0,
        )
        self.assertEqual(short_g[0][4], "4.00")

        range_by_name = {
            "kueue_cluster_queue_nominal_quota": [
                {
                    "metric": {"cluster_queue": "cq-full", "flavor": "tpu-pool", "resource": "google.com/tpu"},
                    "values": [[1000.0, "32"], [1010.0, "32"], [1020.0, "32"], [1030.0, "32"], [1040.0, "32"]],
                }
            ],
            "kueue_cluster_queue_resource_usage": [
                {
                    "metric": {"cluster_queue": "cq-full", "flavor": "tpu-pool", "resource": "google.com/tpu"},
                    "values": [[1000.0, "32"], [1010.0, "32"], [1020.0, "32"], [1030.0, "32"], [1040.0, "0"]],
                }
            ],
        }
        rows = kmr.report_aggregated_quota_utilization(range_by_name, start=1000.0, end=1032.0)
        by_scope = _rows_by_key(rows, 0)
        self.assertEqual(by_scope["ClusterQueue: cq-full"][5], "100.00%")
        self.assertEqual(by_scope["ClusterQueue: cq-full"][6], "32.00 / 32.00")

    def test_section_5_4_preempted_t0_resubmitted_pending_workload_classified_as_excluded(self) -> None:
        """Pending Workload from a preempted-and-resubmitted t0 PodGroup counts as Excluded rather than Unadmitted."""
        t0 = 1700000000.0
        t1 = t0 + 38.0
        snap = {
            "nodes": _make_nodes_2x4_tpu(),
            "workloads": [
                {
                    "metadata": {"name": "jobset-t0-batch-0-abcde", "namespace": "st-workloads"},
                    "spec": {
                        "podSets": [
                            {
                                "count": 2,
                                "template": {
                                    "metadata": {
                                        "annotations": {
                                            "kueue-optimizer.gke.io/pod-execution-time-seconds": "32",
                                            "kueue-optimizer.gke.io/preempted-t0-resubmit": "true",
                                        }
                                    },
                                    "spec": {"containers": [{"name": "c", "resources": {"limits": {"google.com/tpu": "4"}}}]},
                                },
                            }
                        ]
                    },
                    "status": {"conditions": [{"type": "QuotaReserved", "status": "False", "reason": "Pending"}]},
                }
            ],
            "pods": [],
        }
        _rows, subtitle = kmr.report_effective_uninterrupted_saturation(snap, t0, t1)
        self.assertIn("Excluded (preempted/reclaimed/failed): 1 [2 pods]", subtitle)
        self.assertIn("Unadmitted: 0", subtitle)


class TestSection54SaturationFixesM1ToM4(unittest.TestCase):
    """Table-driven tests for Section 5.4 fixes M1-M4, count=0 PodSets, and dynamic capacity integration."""

    T0 = 1700000000.0
    TPU = "google.com/tpu"
    ANN_DUR = "kueue-optimizer.gke.io/pod-execution-time-seconds"
    ANN_INF = "kueue-optimizer.gke.io/infinite-duration"

    @classmethod
    def _ts(cls, offset_sec: float) -> str:
        from datetime import datetime, timezone

        return datetime.fromtimestamp(cls.T0 + offset_sec, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    @classmethod
    def _node(
        cls,
        name: str,
        alloc: dict[str, str],
        *,
        created_off: float | None = None,
        deleted_off: float | None = None,
    ) -> dict:
        meta: dict = {"name": name}
        if created_off is not None:
            meta["creationTimestamp"] = cls._ts(created_off)
        if deleted_off is not None:
            meta["deletionTimestamp"] = cls._ts(deleted_off)
        return {"metadata": meta, "status": {"allocatable": dict(alloc)}}

    @classmethod
    def _podset(
        cls,
        name: str,
        count: int,
        res: dict[str, str],
        *,
        dur: float | None = None,
        infinite: bool = False,
        min_count: int | None = None,
    ) -> dict:
        tpl: dict = {"spec": {"containers": [{"name": "c", "resources": {"requests": dict(res), "limits": dict(res)}}]}}
        anns: dict[str, str] = {}
        if dur is not None:
            anns[cls.ANN_DUR] = str(dur)
        if infinite:
            anns[cls.ANN_INF] = "true"
        if anns:
            tpl["metadata"] = {"annotations": anns}
        ps: dict = {"name": name, "count": count, "template": tpl}
        if min_count is not None:
            ps["minCount"] = min_count
        return ps

    @classmethod
    def _cond(cls, ctype: str, status: str, offset_sec: float, reason: str = "") -> dict:
        return {
            "type": ctype,
            "status": status,
            "reason": reason,
            "lastTransitionTime": cls._ts(offset_sec),
        }

    @classmethod
    def _wl(
        cls,
        name: str,
        podsets: list[dict],
        conds: list[dict],
        *,
        admission: dict | None = None,
        ns: str = "ai-lab",
    ) -> dict:
        st: dict = {"conditions": conds}
        if admission is not None:
            st["admission"] = admission
        return {
            "metadata": {"name": name, "namespace": ns, "creationTimestamp": cls._ts(0)},
            "spec": {"podSets": podsets},
            "status": st,
        }

    def test_M1_per_podset_durations_completed_active_and_infinite(self) -> None:
        """M1: Each PodSet uses its own configured duration instead of smearing the max duration across all PodSets."""
        nodes_16_tpu = [self._node(f"n{i}", {self.TPU: "4"}) for i in range(4)]
        cases = [
            (
                "completed_heterogeneous_podsets",
                self._wl(
                    "pg-completed",
                    [
                        self._podset("short", 2, {self.TPU: "4"}, dur=100),
                        self._podset("long", 2, {self.TPU: "4"}, dur=1000),
                    ],
                    [
                        self._cond("QuotaReserved", "True", 0),
                        self._cond("Admitted", "True", 0),
                        self._cond("Finished", "True", 1000, "Succeeded"),
                    ],
                ),
                [],
                "55.00%",
                "55.00%",
                "0.00%",
                "55.00%",
            ),
            (
                "active_at_end_heterogeneous_podsets",
                self._wl(
                    "pg-active",
                    [
                        self._podset("short", 2, {self.TPU: "4"}, dur=100),
                        self._podset("long", 2, {self.TPU: "4"}, dur=5000),
                    ],
                    [
                        self._cond("QuotaReserved", "True", 0),
                        self._cond("Admitted", "True", 0),
                    ],
                ),
                [],
                "55.00%",
                "0.00%",
                "55.00%",
                "255.00%",
            ),
            (
                "per_podset_infinite_annotation",
                self._wl(
                    "pg-mixed-inf",
                    [
                        self._podset("short", 2, {self.TPU: "4"}, dur=100),
                        self._podset("long", 2, {self.TPU: "4"}, infinite=True),
                    ],
                    [
                        self._cond("QuotaReserved", "True", 0),
                        self._cond("Admitted", "True", 0),
                    ],
                ),
                [],
                "55.00%",
                "0.00%",
                "55.00%",
                "55.00%",
            ),
            (
                "per_podset_duration_from_assoc_pods",
                self._wl(
                    "pg-pod-durs",
                    [
                        self._podset("short", 2, {self.TPU: "4"}),
                        self._podset("long", 2, {self.TPU: "4"}),
                    ],
                    [
                        self._cond("QuotaReserved", "True", 0),
                        self._cond("Admitted", "True", 0),
                        self._cond("Finished", "True", 1000, "Succeeded"),
                    ],
                ),
                [
                    {
                        "metadata": {
                            "name": f"short-{i}",
                            "namespace": "ai-lab",
                            "labels": {"kueue.x-k8s.io/prebuilt-workload-name": "pg-pod-durs", "kueue.x-k8s.io/podset": "short"},
                            "annotations": {self.ANN_DUR: "100"},
                        },
                        "status": {"phase": "Succeeded", "startTime": self._ts(0)},
                    }
                    for i in range(2)
                ]
                + [
                    {
                        "metadata": {
                            "name": f"long-{i}",
                            "namespace": "ai-lab",
                            "labels": {"kueue.x-k8s.io/prebuilt-workload-name": "pg-pod-durs", "kueue.x-k8s.io/podset": "long"},
                            "annotations": {self.ANN_DUR: "1000"},
                        },
                        "status": {"phase": "Succeeded", "startTime": self._ts(0)},
                    }
                    for i in range(2)
                ],
                "55.00%",
                "55.00%",
                "0.00%",
                "55.00%",
            ),
        ]
        for label, wl, pods, exp_eff, exp_comp, exp_act, exp_full in cases:
            with self.subTest(case=label):
                snap = {"nodes": nodes_16_tpu, "workloads": [wl], "pods": pods}
                rows, _ = kmr.report_effective_uninterrupted_saturation(snap, self.T0, self.T0 + 1000.0)
                by_res = _rows_by_key(rows, 0)
                self.assertEqual(by_res[self.TPU][2], exp_eff)
                self.assertEqual(by_res[self.TPU][3], exp_comp)
                self.assertEqual(by_res[self.TPU][4], exp_act)
                self.assertEqual(by_res[self.TPU][5], exp_full)

        # Sibling PodSet pod-level annotations must NOT leak onto an infinite-duration or unannotated PodSet.
        for long_anns, exp_long_inf in (
            ({self.ANN_INF: "true"}, True),
            ({}, False),
        ):
            with self.subTest(case=f"no_sibling_dur_leak_inf_{exp_long_inf}"):
                wl_leak = self._wl(
                    "pg-no-leak",
                    [
                        self._podset("short", 2, {self.TPU: "4"}),
                        self._podset("long", 2, {self.TPU: "4"}),
                    ],
                    [self._cond("QuotaReserved", "True", 0), self._cond("Admitted", "True", 0)],
                )
                pods_leak = [
                    {
                        "metadata": {
                            "name": f"short-{i}",
                            "namespace": "ai-lab",
                            "labels": {"kueue.x-k8s.io/prebuilt-workload-name": "pg-no-leak", "kueue.x-k8s.io/podset": "short"},
                            "annotations": {self.ANN_DUR: "100"},
                        },
                        "status": {"phase": "Running", "startTime": self._ts(0)},
                    }
                    for i in range(2)
                ] + [
                    {
                        "metadata": {
                            "name": f"long-{i}",
                            "namespace": "ai-lab",
                            "labels": {"kueue.x-k8s.io/prebuilt-workload-name": "pg-no-leak", "kueue.x-k8s.io/podset": "long"},
                            "annotations": dict(long_anns),
                        },
                        "status": {"phase": "Running", "startTime": self._ts(0)},
                    }
                    for i in range(2)
                ]
                slices_leak = kmr.extract_workload_podset_slices(wl_leak, assoc_pods=pods_leak)
                self.assertEqual(slices_leak[0].cfg_dur_sec, 100.0)
                self.assertIsNone(slices_leak[1].cfg_dur_sec)
                self.assertEqual(slices_leak[1].is_infinite, exp_long_inf)
                rows_leak, _ = kmr.report_effective_uninterrupted_saturation(
                    {"nodes": nodes_16_tpu, "workloads": [wl_leak], "pods": pods_leak},
                    self.T0,
                    self.T0 + 1000.0,
                )
                # short: 8 TPU * 100s = 800; long: 8 TPU * 1000s = 8000 -> 8800 / 16000 = 55.00%
                self.assertEqual(_rows_by_key(rows_leak, 0)[self.TPU][2], "55.00%")

        # Empty/invalid template duration annotation must NOT clear Workload-level infinite-duration.
        wl_inf_bad_tpl = self._wl(
            "pg-inf-invalid-tpl",
            [self._podset("main", 2, {self.TPU: "4"}, dur=None)],
            [self._cond("Admitted", "True", 0)],
        )
        wl_inf_bad_tpl["metadata"]["annotations"] = {self.ANN_INF: "true"}
        wl_inf_bad_tpl["spec"]["podSets"][0]["template"]["metadata"] = {"annotations": {self.ANN_DUR: "0"}}
        inf_slices = kmr.extract_workload_podset_slices(wl_inf_bad_tpl)
        self.assertTrue(inf_slices[0].is_infinite)
        self.assertIsNone(inf_slices[0].cfg_dur_sec)

    def test_M2_dynamic_cluster_capacity_timeline_and_timestamp_fallback(self) -> None:
        """M2: Cluster capacity integrates time-varying nodeCapacityTimeline and creation/deletion timestamps."""
        cases = [
            (
                "timeline_node_deleted_mid_run",
                {
                    "nodes": [self._node("n-a", {self.TPU: "4"})],
                    "nodeCapacityTimeline": [
                        {"epoch": self.T0, "capacity": {self.TPU: 8.0}},
                        {"epoch": self.T0 + 1800.0, "capacity": {self.TPU: 4.0}},
                    ],
                    "workloads": [
                        self._wl(
                            "w1",
                            [self._podset("m", 2, {self.TPU: "4"}, dur=1800)],
                            [self._cond("Admitted", "True", 0), self._cond("Finished", "True", 1800, "Succeeded")],
                        ),
                        self._wl(
                            "w2",
                            [self._podset("m", 1, {self.TPU: "4"}, dur=1800)],
                            [self._cond("Admitted", "True", 1800), self._cond("Finished", "True", 3600, "Succeeded")],
                        ),
                    ],
                    "pods": [],
                },
                "100.00%",
                "6",
            ),
            (
                "timeline_node_added_late",
                {
                    "nodes": [self._node("n-a", {self.TPU: "4"}), self._node("n-late", {self.TPU: "4"})],
                    "nodeCapacityTimeline": [
                        {"epoch": self.T0, "capacity": {self.TPU: 4.0}},
                        {"epoch": self.T0 + 3540.0, "capacity": {self.TPU: 8.0}},
                    ],
                    "workloads": [
                        self._wl(
                            "w1",
                            [self._podset("m", 1, {self.TPU: "4"}, dur=3600)],
                            [self._cond("Admitted", "True", 0), self._cond("Finished", "True", 3600, "Succeeded")],
                        ),
                    ],
                    "pods": [],
                },
                "98.36%",
                "4.07",
            ),
            (
                "timeline_ignores_post_end_and_uses_last_same_epoch_entry",
                {
                    "nodes": [],
                    "nodeCapacityTimeline": [
                        {"epoch": self.T0 - 10.0, "capacity": {self.TPU: 12.0}},
                        {"epoch": self.T0, "capacity": {self.TPU: 8.0}},
                        {"epoch": self.T0 + 1800.0, "capacity": {self.TPU: 6.0}},
                        {"epoch": self.T0 + 1800.0, "capacity": {self.TPU: 4.0}},
                        {"epoch": self.T0 + 3605.0, "capacity": {}},
                    ],
                    "workloads": [
                        self._wl(
                            "w1",
                            [self._podset("m", 2, {self.TPU: "4"}, dur=1800)],
                            [self._cond("Admitted", "True", 0), self._cond("Finished", "True", 1800, "Succeeded")],
                        ),
                        self._wl(
                            "w2",
                            [self._podset("m", 1, {self.TPU: "4"}, dur=1800)],
                            [self._cond("Admitted", "True", 1800), self._cond("Finished", "True", 3600, "Succeeded")],
                        ),
                    ],
                    "pods": [],
                },
                "100.00%",
                "6",
            ),
            (
                "timeline_initial_entry_within_1ms_rounding_of_start_epoch",
                {
                    "nodes": [self._node("n-a", {self.TPU: "4"}), self._node("n-b", {self.TPU: "4"})],
                    "nodeCapacityTimeline": [
                        {"epoch": self.T0 + 0.0004, "capacity": {self.TPU: 8.0}},
                        {"epoch": self.T0 + 1800.0, "capacity": {self.TPU: 4.0}},
                    ],
                    "workloads": [
                        self._wl(
                            "w1",
                            [self._podset("m", 2, {self.TPU: "4"}, dur=1800)],
                            [self._cond("Admitted", "True", 0), self._cond("Finished", "True", 1800, "Succeeded")],
                        ),
                        self._wl(
                            "w2",
                            [self._podset("m", 1, {self.TPU: "4"}, dur=1800)],
                            [self._cond("Admitted", "True", 1800), self._cond("Finished", "True", 3600, "Succeeded")],
                        ),
                    ],
                    "pods": [],
                },
                "100.00%",
                "6",
            ),
            (
                "timeline_zero_capacity_at_t0_then_added",
                {
                    "nodes": [self._node("n-added", {self.TPU: "4"})],
                    "nodeCapacityTimeline": [
                        {"epoch": self.T0, "capacity": {}},
                        {"epoch": self.T0 + 1800.0, "capacity": {self.TPU: 4.0}},
                    ],
                    "workloads": [
                        self._wl(
                            "w2",
                            [self._podset("m", 1, {self.TPU: "4"}, dur=1800)],
                            [self._cond("Admitted", "True", 1800), self._cond("Finished", "True", 3600, "Succeeded")],
                        ),
                    ],
                    "pods": [],
                },
                "100.00%",
                "2",
            ),
            (
                "nodes_creation_timestamp_fallback_without_timeline",
                {
                    "nodes": [
                        self._node("n-a", {self.TPU: "4"}, created_off=-60.0),
                        self._node("n-late", {self.TPU: "4"}, created_off=3540.0),
                        self._node("n-after-end", {self.TPU: "4"}, created_off=3610.0),
                    ],
                    "workloads": [
                        self._wl(
                            "w1",
                            [self._podset("m", 1, {self.TPU: "4"}, dur=3600)],
                            [self._cond("Admitted", "True", 0), self._cond("Finished", "True", 3600, "Succeeded")],
                        ),
                    ],
                    "pods": [],
                },
                "98.36%",
                "4.07",
            ),
            (
                "nodes_deletion_timestamp_fallback_without_timeline",
                {
                    "nodes": [
                        self._node("n-a", {self.TPU: "4"}, created_off=-60.0),
                        self._node("n-b", {self.TPU: "4"}, created_off=-60.0, deleted_off=1800.0),
                    ],
                    "workloads": [
                        self._wl(
                            "w1",
                            [self._podset("m", 2, {self.TPU: "4"}, dur=1800)],
                            [self._cond("Admitted", "True", 0), self._cond("Finished", "True", 1800, "Succeeded")],
                        ),
                        self._wl(
                            "w2",
                            [self._podset("m", 1, {self.TPU: "4"}, dur=1800)],
                            [self._cond("Admitted", "True", 1800), self._cond("Finished", "True", 3600, "Succeeded")],
                        ),
                    ],
                    "pods": [],
                },
                "100.00%",
                "6",
            ),
        ]
        for label, snap, exp_eff, exp_avg_cap in cases:
            with self.subTest(case=label):
                rows, _ = kmr.report_effective_uninterrupted_saturation(snap, self.T0, self.T0 + 3600.0)
                by_res = _rows_by_key(rows, 0)
                self.assertEqual(by_res[self.TPU][2], exp_eff)
                self.assertEqual(by_res[self.TPU][1], exp_avg_cap)

    def test_M3_partial_admission_and_zero_count_podsets(self) -> None:
        """M3 & S4d: Partial admission uses authoritative podSetAssignments count/usage; count=0 PodSets contribute 0."""
        cases = [
            (
                "single_podset_partial_admission_with_resource_usage",
                [self._node("n0", {"cpu": "10", "pods": "110"})],
                self._wl(
                    "partial-1",
                    [self._podset("main", 10, {"cpu": "1"}, dur=1000, min_count=2)],
                    [
                        self._cond("QuotaReserved", "True", 0),
                        self._cond("Admitted", "True", 0),
                        self._cond("Finished", "True", 1000, "Succeeded"),
                    ],
                    admission={
                        "clusterQueue": "cq",
                        "podSetAssignments": [{"name": "main", "count": 4, "resourceUsage": {"cpu": "4"}}],
                    },
                ),
                {"cpu": "40.00%"},
                "completed=1 [4 pods]",
            ),
            (
                "single_podset_partial_admission_without_resource_usage_scales_template",
                [self._node("n0", {"cpu": "10", "pods": "110"})],
                self._wl(
                    "partial-no-usage",
                    [self._podset("main", 10, {"cpu": "1"}, dur=1000, min_count=2)],
                    [
                        self._cond("QuotaReserved", "True", 0),
                        self._cond("Admitted", "True", 0),
                        self._cond("Finished", "True", 1000, "Succeeded"),
                    ],
                    admission={
                        "clusterQueue": "cq",
                        "podSetAssignments": [{"name": "main", "count": 4}],
                    },
                ),
                {"cpu": "40.00%"},
                "completed=1 [4 pods]",
            ),
            (
                "multi_podset_partial_admission",
                [self._node("n0", {"cpu": "10", self.TPU: "32", "pods": "110"})],
                self._wl(
                    "partial-multi",
                    [
                        self._podset("driver", 1, {"cpu": "2"}, dur=1000),
                        self._podset("worker", 10, {"cpu": "1", self.TPU: "4"}, dur=1000, min_count=2),
                    ],
                    [
                        self._cond("QuotaReserved", "True", 0),
                        self._cond("Admitted", "True", 0),
                        self._cond("Finished", "True", 1000, "Succeeded"),
                    ],
                    admission={
                        "clusterQueue": "cq",
                        "podSetAssignments": [
                            {"name": "driver", "count": 1, "resourceUsage": {"cpu": "2"}},
                            {"name": "worker", "count": 4, "resourceUsage": {"cpu": "4", self.TPU: "16"}},
                        ],
                    },
                ),
                {"cpu": "60.00%", self.TPU: "50.00%"},
                "completed=1 [5 pods]",
            ),
            (
                "zero_count_podset_contributes_zero_pods_and_zero_tpus",
                [self._node("n0", {"cpu": "2", self.TPU: "8", "pods": "110"})],
                self._wl(
                    "zero-ps",
                    [
                        self._podset("driver", 1, {"cpu": "1"}, dur=1000),
                        self._podset("workers", 0, {self.TPU: "4"}, dur=1000),
                    ],
                    [
                        self._cond("Admitted", "True", 0),
                        self._cond("Finished", "True", 1000, "Succeeded"),
                    ],
                ),
                {"cpu": "50.00%", self.TPU: "0.00%"},
                "completed=1 [1 pods]",
            ),
        ]
        for label, nodes, wl, exp_pcts, exp_sub in cases:
            with self.subTest(case=label):
                snap = {"nodes": nodes, "workloads": [wl], "pods": []}
                rows, subtitle = kmr.report_effective_uninterrupted_saturation(snap, self.T0, self.T0 + 1000.0)
                by_res = _rows_by_key(rows, 0)
                for rk, exp_pct in exp_pcts.items():
                    self.assertEqual(by_res[rk][2], exp_pct)
                self.assertIn(exp_sub, subtitle)

        # Direct check on extract_workload_podset_slices for count=0 PodSet
        zero_slices = kmr.extract_workload_podset_slices(
            self._wl(
                "zero-direct",
                [self._podset("driver", 1, {"cpu": "1"}, dur=100), self._podset("empty", 0, {self.TPU: "4"}, dur=100)],
                [],
            )
        )
        self.assertEqual([(s.name, s.pod_count, s.resources) for s in zero_slices], [
            ("driver", 1, {"cpu": 1.0, "pods": 1.0}),
            ("empty", 0, {}),
        ])

    def test_M3_multi_wave_partial_admission_and_indexed_hotswap_dedup(self) -> None:
        """Multi-wave partially admitted Jobs scale per-pod duration by distinct waves without double-counting Indexed retries."""
        nodes = [self._node("n0", {self.TPU: "4"}), self._node("n1", {self.TPU: "4"})]  # 8 TPUs
        # 1. Partial admission Job: parallelism=4, completions=4, min=2 -> admitted count=2 (8 TPU), ran 2 waves of 12s = 24s.
        wl_2wave = self._wl(
            "job-partial-2wave",
            [self._podset("main", 4, {self.TPU: "4"}, dur=12, min_count=2)],
            [
                self._cond("QuotaReserved", "True", 0),
                self._cond("Admitted", "True", 0),
                self._cond("Finished", "True", 24, "Succeeded"),
            ],
            admission={
                "clusterQueue": "cq",
                "podSetAssignments": [{"name": "main", "count": 2, "resourceUsage": {self.TPU: "8"}}],
            },
        )
        wl_2wave["metadata"]["ownerReferences"] = [{"kind": "Job", "name": "partial-2wave"}]
        four_nonindexed_pods = [
            {
                "metadata": {
                    "name": f"partial-2wave-{i}",
                    "uid": f"uid-{i}",
                    "namespace": "ai-lab",
                    "labels": {"batch.kubernetes.io/job-name": "partial-2wave", "kueue.x-k8s.io/podset": "main"},
                },
                "status": {"phase": "Succeeded", "startTime": self._ts(0 if i < 2 else 12)},
            }
            for i in range(4)
        ]
        # Completed after 2 waves (24s): 8 TPU * 24s = 192 / (8 * 24) = 100.00%
        rows_done, sub_done = kmr.report_effective_uninterrupted_saturation(
            {"nodes": nodes, "workloads": [wl_2wave], "pods": four_nonindexed_pods},
            self.T0,
            self.T0 + 24.0,
        )
        self.assertEqual(_rows_by_key(rows_done, 0)[self.TPU][2], "100.00%")
        self.assertIn("completed=1 [2 pods]", sub_done)

        # Active during wave 1 (8s elapsed, only 2 Running pods): 8 TPU * 8s = 64 / (8 * 8) = 100.00% (NOT 200%!)
        wl_wave1 = self._wl(
            "job-partial-wave1",
            [self._podset("main", 4, {self.TPU: "4"}, dur=12, min_count=2)],
            [self._cond("QuotaReserved", "True", 0), self._cond("Admitted", "True", 0)],
            admission={
                "clusterQueue": "cq",
                "podSetAssignments": [{"name": "main", "count": 2, "resourceUsage": {self.TPU: "8"}}],
            },
        )
        wl_wave1["metadata"]["ownerReferences"] = [{"kind": "Job", "name": "partial-wave1"}]
        two_running_pods = [
            {
                "metadata": {
                    "name": f"partial-wave1-{i}",
                    "uid": f"run-{i}",
                    "namespace": "ai-lab",
                    "labels": {"batch.kubernetes.io/job-name": "partial-wave1", "kueue.x-k8s.io/podset": "main"},
                },
                "status": {"phase": "Running", "startTime": self._ts(0)},
            }
            for i in range(2)
        ]
        rows_w1, _ = kmr.report_effective_uninterrupted_saturation(
            {"nodes": nodes, "workloads": [wl_wave1], "pods": two_running_pods},
            self.T0,
            self.T0 + 8.0,
        )
        self.assertEqual(_rows_by_key(rows_w1, 0)[self.TPU][2], "100.00%")

        # 2. Indexed JobSet with hot-swapped replacement pod sharing job-completion-index="0": must NOT inflate duration.
        wl_hotswap = self._wl(
            "jobset-hs-wl",
            [self._podset("workers", 2, {self.TPU: "4"}, dur=12)],
            [
                self._cond("Admitted", "True", 0),
                self._cond("Finished", "True", 24, "Succeeded"),
            ],
            admission={
                "clusterQueue": "cq",
                "podSetAssignments": [{"name": "workers", "count": 2, "resourceUsage": {self.TPU: "8"}}],
            },
        )
        wl_hotswap["metadata"]["ownerReferences"] = [{"kind": "JobSet", "name": "hs"}]
        hs_pods = [
            {
                "metadata": {
                    "name": f"hs-workers-0-{idx}-{suffix}",
                    "uid": f"hs-{idx}-{suffix}",
                    "namespace": "ai-lab",
                    "labels": {
                        "jobset.sigs.k8s.io/jobset-name": "hs",
                        "jobset.sigs.k8s.io/replicatedjob-name": "workers",
                        "jobset.sigs.k8s.io/job-index": "0",
                        "batch.kubernetes.io/job-completion-index": str(idx),
                        "kueue.x-k8s.io/podset": "workers",
                    },
                },
                "status": {"phase": "Succeeded", "startTime": self._ts(0)},
            }
            for idx, suffix in ((0, "old"), (0, "new"), (1, "orig"))
        ]
        rows_hs, _ = kmr.report_effective_uninterrupted_saturation(
            {"nodes": nodes, "workloads": [wl_hotswap], "pods": hs_pods},
            self.T0,
            self.T0 + 24.0,
        )
        # Only 2 distinct indices (0 and 1) -> stays at 12s / 24s = 50.00%, NOT inflated to 75.00%!
        self.assertEqual(_rows_by_key(rows_hs, 0)[self.TPU][2], "50.00%")

        # 3. Annotation-only batch.kubernetes.io/job-completion-index also deduplicates hot-swap retries.
        hs_ann_pods = [
            {
                "metadata": {
                    "name": f"hs-ann-{idx}-{suffix}",
                    "uid": f"hs-ann-{idx}-{suffix}",
                    "namespace": "ai-lab",
                    "labels": {"jobset.sigs.k8s.io/jobset-name": "hs", "kueue.x-k8s.io/podset": "workers"},
                    "annotations": {"batch.kubernetes.io/job-completion-index": str(idx)},
                },
                "status": {"phase": "Succeeded", "startTime": self._ts(0)},
            }
            for idx, suffix in ((0, "old"), (0, "new"), (1, "orig"))
        ]
        rows_hs_ann, _ = kmr.report_effective_uninterrupted_saturation(
            {"nodes": nodes, "workloads": [wl_hotswap], "pods": hs_ann_pods},
            self.T0,
            self.T0 + 24.0,
        )
        self.assertEqual(_rows_by_key(rows_hs_ann, 0)[self.TPU][2], "50.00%")

        # 4. Active-at-end uneven Wave 2 & inter-wave gap: parallelism=4, completions=3, min=2 -> admitted 2 (8 TPU),
        #    per_pod_dur=12s. Wave 1: 2 Succeeded pods [0..12s] = 96 TPU-s; Wave 2: 1 Running pod [12..18s] = 24 TPU-s
        #    -> exact 120.0 TPU-s (83.33%), NOT 8 * 18 = 144 TPU-s (100.00%).
        wl_uneven = self._wl(
            "job-partial-uneven",
            [self._podset("main", 4, {self.TPU: "4"}, dur=12, min_count=2)],
            [self._cond("QuotaReserved", "True", 0), self._cond("Admitted", "True", 0)],
            admission={
                "clusterQueue": "cq",
                "podSetAssignments": [{"name": "main", "count": 2, "resourceUsage": {self.TPU: "8"}}],
            },
        )
        wl_uneven["metadata"]["ownerReferences"] = [{"kind": "Job", "name": "partial-uneven"}]
        for label_w2, w2_start, exp_eff_tpu, exp_eff_pct in (
            ("uneven_wave2_no_gap", 12, 120.0, "83.33%"),
            ("uneven_wave2_with_2s_gap", 14, 112.0, "77.78%"),
        ):
            with self.subTest(case=label_w2):
                w2_pods = [
                    {
                        "metadata": {
                            "name": f"partial-uneven-w1-{i}",
                            "uid": f"u-w1-{i}",
                            "namespace": "ai-lab",
                            "labels": {"batch.kubernetes.io/job-name": "partial-uneven", "kueue.x-k8s.io/podset": "main"},
                        },
                        "status": {
                            "phase": "Succeeded",
                            "startTime": self._ts(0),
                            "containerStatuses": [{"state": {"terminated": {"finishedAt": self._ts(12)}}}],
                        },
                    }
                    for i in range(2)
                ] + [
                    {
                        "metadata": {
                            "name": "partial-uneven-w2-0",
                            "uid": "u-w2-0",
                            "namespace": "ai-lab",
                            "labels": {"batch.kubernetes.io/job-name": "partial-uneven", "kueue.x-k8s.io/podset": "main"},
                        },
                        "status": {"phase": "Running", "startTime": self._ts(w2_start)},
                    }
                ]
                cls_w2 = kmr.classify_workload_for_saturation(wl_uneven, w2_pods, self.T0, self.T0 + 18.0)
                self.assertEqual(cls_w2.outcome, "active_at_end")
                self.assertAlmostEqual(cls_w2.eff_res_sec[self.TPU], exp_eff_tpu)
                self.assertAlmostEqual(cls_w2.full_res_sec[self.TPU], 144.0)
                rows_w2, _ = kmr.report_effective_uninterrupted_saturation(
                    {"nodes": nodes, "workloads": [wl_uneven], "pods": w2_pods},
                    self.T0,
                    self.T0 + 18.0,
                )
                self.assertEqual(_rows_by_key(rows_w2, 0)[self.TPU][2], exp_eff_pct)

    def test_M4_and_S4_classification_seam_table(self) -> None:
        """M4 & S4a-S4c: Pure classify_workload_for_saturation seam handles AdmissionChecks, Pending pods, and re-admission."""
        nodes = [self._node("n0", {self.TPU: "4"}), self._node("n1", {self.TPU: "4"})]
        pending_pods = [
            {
                "metadata": {"name": f"p{i}", "namespace": "ai-lab", "labels": {"kueue.x-k8s.io/pod-group-name": "stuck"}},
                "spec": {"containers": [{"name": "c", "resources": {"requests": {self.TPU: "4"}}}]},
                "status": {"phase": "Pending"},
            }
            for i in range(2)
        ]
        cases = [
            (
                "quota_reserved_only_admitted_false",
                self._wl(
                    "waiting-ac",
                    [self._podset("m", 2, {self.TPU: "4"}, dur=5000)],
                    [
                        self._cond("QuotaReserved", "True", 0),
                        self._cond("Admitted", "False", 0, "UnsatisfiedAdmissionChecks"),
                    ],
                    admission={"clusterQueue": "cq", "podSetAssignments": [{"name": "m", "count": 2}]},
                ),
                [],
                "pending",
                "0.00%",
                "active_at_end=0 [0 pods]",
                "Excluded (preempted/reclaimed/failed): 0 [0 pods] | Unadmitted: 1 [2 pods]",
            ),
            (
                "quota_reserved_only_no_admitted_condition",
                self._wl(
                    "waiting-ac-no-admitted",
                    [self._podset("m", 2, {self.TPU: "4"}, dur=5000)],
                    [self._cond("QuotaReserved", "True", 0)],
                ),
                [],
                "pending",
                "0.00%",
                "active_at_end=0 [0 pods]",
                "Excluded (preempted/reclaimed/failed): 0 [0 pods] | Unadmitted: 1 [2 pods]",
            ),
            (
                "admitted_but_pods_pending_never_interrupted",
                self._wl(
                    "stuck",
                    [self._podset("m", 2, {self.TPU: "4"}, dur=600)],
                    [self._cond("QuotaReserved", "True", 100), self._cond("Admitted", "True", 100)],
                    admission={"clusterQueue": "cq", "podSetAssignments": [{"name": "m", "count": 2}]},
                ),
                pending_pods,
                "admitted_pending_pods",
                "0.00%",
                "active_at_end=0 [0 pods]",
                "Excluded (preempted/reclaimed/failed): 0 [0 pods] | Admitted (pending pods): 1 [2 pods] | Unadmitted: 0",
            ),
            (
                "all_pods_succeeded_ignores_late_admitted_false_timestamp",
                self._wl(
                    "all-succ-late-admit-false",
                    [self._podset("m", 2, {self.TPU: "4"}, dur=400)],
                    [
                        self._cond("Admitted", "False", 2000, "Finished"),
                        self._cond("Finished", "True", 400, "Succeeded"),
                    ],
                ),
                [
                    {
                        "metadata": {
                            "name": f"succ-{i}",
                            "namespace": "ai-lab",
                            "labels": {"kueue.x-k8s.io/pod-group-name": "all-succ-late-admit-false"},
                        },
                        "spec": {"containers": [{"name": "c", "resources": {"requests": {self.TPU: "4"}}}]},
                        "status": {"phase": "Succeeded", "startTime": self._ts(0)},
                    }
                    for i in range(2)
                ],
                "completed",
                "40.00%",
                "completed=1 [2 pods]",
                "Excluded (preempted/reclaimed/failed): 0 [0 pods] | Unadmitted: 0",
            ),
            (
                "evicted_then_readmitted_then_finished",
                self._wl(
                    "readmitted",
                    [self._podset("m", 2, {self.TPU: "4"}, dur=400)],
                    [
                        self._cond("QuotaReserved", "True", 500, "QuotaReserved"),
                        self._cond("Admitted", "True", 500, "Admitted"),
                        self._cond("Evicted", "False", 500, "QuotaReserved"),
                        self._cond("Preempted", "False", 500, "QuotaReserved"),
                        self._cond("Requeued", "True", 300, "Preempted"),
                        self._cond("Finished", "True", 900, "Succeeded"),
                    ],
                ),
                [],
                "completed",
                "40.00%",
                "completed=1 [2 pods]",
                "Excluded (preempted/reclaimed/failed): 0 [0 pods] | Unadmitted: 0",
            ),
            (
                "finished_after_eval_window_counted_active_at_end",
                self._wl(
                    "late-finish",
                    [self._podset("m", 2, {self.TPU: "4"}, dur=1010)],
                    [
                        self._cond("Admitted", "True", 0),
                        self._cond("Finished", "True", 1010, "Succeeded"),
                    ],
                ),
                [],
                "active_at_end",
                "100.00%",
                "active_at_end=1 [2 pods]",
                "Excluded (preempted/reclaimed/failed): 0 [0 pods] | Unadmitted: 0",
            ),
            (
                "evicted_true_classified_as_excluded_interrupted",
                self._wl(
                    "evicted-wl",
                    [self._podset("m", 2, {self.TPU: "4"}, dur=600)],
                    [
                        self._cond("Admitted", "False", 300, "Evicted"),
                        self._cond("Evicted", "True", 300, "Preempted"),
                    ],
                ),
                [],
                "excluded_interrupted",
                "0.00%",
                "active_at_end=0 [0 pods]",
                "Excluded (preempted/reclaimed/failed): 1 [2 pods] | Unadmitted: 0",
            ),
        ]
        for label, wl, pods, exp_outcome, exp_tpu_pct, exp_sub_part1, exp_sub_part2 in cases:
            with self.subTest(case=label):
                cls_res = kmr.classify_workload_for_saturation(wl, pods, self.T0, self.T0 + 1000.0)
                self.assertEqual(cls_res.outcome, exp_outcome)
                snap = {"nodes": nodes, "workloads": [wl], "pods": pods}
                rows, subtitle = kmr.report_effective_uninterrupted_saturation(snap, self.T0, self.T0 + 1000.0)
                by_res = _rows_by_key(rows, 0)
                self.assertEqual(by_res[self.TPU][2], exp_tpu_pct)
                self.assertIn(exp_sub_part1, subtitle)
                self.assertIn(exp_sub_part2, subtitle)

    def test_M2_replay_and_snapshot_capture_records_node_capacity_timeline(self) -> None:
        """run_scenario_replay tracks Node capacity transitions and capture_cluster_saturation_snapshot exports them."""
        import json
        from chamber.lifecycle import ChamberManager, ClusterStatusSnapshot
        from runner.loader import ExecutionTimeIndex, ScenarioBundle, ScheduledInfraEvent
        from runner.replay import run_scenario_replay

        n0 = self._node("n-0", {self.TPU: "4", "cpu": "16"})
        n1 = self._node("n-1", {self.TPU: "4", "cpu": "16"})
        n2 = self._node("n-2", {self.TPU: "8", "cpu": "32"})

        def _make_bundle(infra_events: list[ScheduledInfraEvent]) -> ScenarioBundle:
            return ScenarioBundle(
                scenario_dir=Path("/tmp/fake-scenario"),
                infra_path=Path("/tmp/fake-scenario/infra.json"),
                workflows_path=Path("/tmp/fake-scenario/workflows.json"),
                execution_time_path=Path("/tmp/fake-scenario/execution_time.json"),
                exec_index=ExecutionTimeIndex({}, {}),
                t0_epoch=self.T0,
                t0_iso=self._ts(0),
                effective_start_epoch=self.T0,
                effective_start_iso=self._ts(0),
                initial_jump_raw_sec=0.0,
                initial_jump_wall_sec=0.0,
                expired_initial_pods_count=0,
                t_end_epoch=self.T0 + 5.0,
                t_end_iso=self._ts(5),
                infra_event_count=len(infra_events),
                workflow_event_count=0,
                nodes=[n0, n1],
                namespaces=[],
                prereq_and_queue_docs=[],
                initial_pods=[],
                scheduled_workflows=[],
                skipped_already_admitted_workflows=[],
                skipped_child_workflows=0,
                workload_namespaces=["default"],
                primary_workload_namespace="default",
                infra_pods_namespace="infra-pods",
                cluster_queue_names=[],
                queueing_strategy="BestEffortFIFO",
                scheduled_infra_events=infra_events,
            )

        # 1. Dynamic scenario: Delete n-1, taint-only Update n-0 carrying synthetic fallback allocatable
        #    (cpu=4, mem=16Gi, 0 TPU from _prepare_node_manifest), Add n-2 -> 3 timeline entries (Update ignored!).
        n0_taint_update_synthetic_fallback = self._node("n-0", {"cpu": "4", "memory": "16Gi", "pods": "110"})
        bundle_dyn = _make_bundle(
            [
                ScheduledInfraEvent(
                    target_wall_offset_sec=0.0,
                    seq=1,
                    raw_offset_sec=0.0,
                    raw_offset_from_t0_sec=0.0,
                    event_timestamp=self._ts(0),
                    event_type="Deleted",
                    object_kind="Node",
                    name="n-1",
                    manifest={},
                ),
                ScheduledInfraEvent(
                    target_wall_offset_sec=0.0,
                    seq=2,
                    raw_offset_sec=0.0,
                    raw_offset_from_t0_sec=0.0,
                    event_timestamp=self._ts(0),
                    event_type="Update",
                    object_kind="Node",
                    name="n-0",
                    manifest=n0_taint_update_synthetic_fallback,
                ),
                ScheduledInfraEvent(
                    target_wall_offset_sec=0.0,
                    seq=3,
                    raw_offset_sec=0.0,
                    raw_offset_from_t0_sec=0.0,
                    event_timestamp=self._ts(0),
                    event_type="New",
                    object_kind="Node",
                    name="n-2",
                    manifest=n2,
                ),
            ]
        )

        mgr = ChamberManager(chamber_namespace="kueue-optimizer-chamber-m2-test", apiserver_port=16498)
        try:
            empty_status = ClusterStatusSnapshot(
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
                workloads_query_ok=True,
            )

            def fake_api_request(method: str, path: str, **_kwargs: object) -> tuple[int, bytes]:
                if path == "/api/v1/nodes" and method == "GET":
                    return 200, json.dumps({"items": [n0, n2]}).encode("utf-8")
                if path == "/apis/kueue.x-k8s.io/v1beta2/workloads" and method == "GET":
                    return 200, b'{"items": []}'
                if path == "/api/v1/pods" and method == "GET":
                    return 200, b'{"items": []}'
                return 200, b'{"items": []}'

            with (
                patch.object(mgr, "ensure_kwok_connection", return_value=None),
                patch.object(mgr, "api_request", side_effect=fake_api_request),
                patch.object(mgr, "ssa_apply_doc", return_value=None),
                patch("runner.replay.query_cluster_status", return_value=empty_status),
            ):
                run_scenario_replay(
                    mgr=mgr,
                    bundle=bundle_dyn,
                    speedup=1.0,
                    max_run_seconds=5.0,
                    status_interval_sec=0.5,
                    wait_for_completion=True,
                    initial_state_already_bootstrapped=True,
                )
                snap_dyn = mgr.capture_cluster_saturation_snapshot()

            self.assertIsNotNone(snap_dyn)
            assert snap_dyn is not None
            timeline = snap_dyn.get("nodeCapacityTimeline")
            self.assertIsInstance(timeline, list)
            assert isinstance(timeline, list)
            # Initial (8 TPU) -> after deleting n-1 (4 TPU) -> after adding n-2 (12 TPU); unchanged Update skipped!
            self.assertEqual(len(timeline), 3)
            self.assertEqual([e["capacity"][self.TPU] for e in timeline], [8.0, 4.0, 12.0])

            # 2. Static scenario (no Node capacity changes): nodeCapacityTimeline is omitted from snapshot.
            bundle_static = _make_bundle([])
            with (
                patch.object(mgr, "ensure_kwok_connection", return_value=None),
                patch.object(mgr, "api_request", side_effect=fake_api_request),
                patch("runner.replay.query_cluster_status", return_value=empty_status),
            ):
                run_scenario_replay(
                    mgr=mgr,
                    bundle=bundle_static,
                    speedup=1.0,
                    max_run_seconds=5.0,
                    status_interval_sec=0.5,
                    wait_for_completion=True,
                    initial_state_already_bootstrapped=True,
                )
                snap_static = mgr.capture_cluster_saturation_snapshot()

            self.assertIsNotNone(snap_static)
            assert snap_static is not None
            self.assertNotIn("nodeCapacityTimeline", snap_static)
        finally:
            mgr.close()


if __name__ == "__main__":
    unittest.main()
