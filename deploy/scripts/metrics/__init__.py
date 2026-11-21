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

"""Modular Kueue Prometheus & saturation metrics reporting package."""

from metrics.aggregations import (
    report_aggregated_evictions_and_reclaims,
    report_aggregated_quota_utilization,
    report_aggregated_wait_time_by_priority,
    report_counter,
    report_gauge,
    report_histogram,
)
from metrics.catalog import (
    IGNORED_LABELS,
    KUEUE_METRICS_CATALOG,
    MetricSpec,
    MetricType,
    format_labels,
    priority_sort_key,
    raw_series_identity,
)
from metrics.formatting import MAX_TERMINAL_WIDTH, print_table, wrap_cell
from metrics.prometheus import (
    PrometheusClient,
    build_monotonic_counter_points,
    counter_series_total_increase,
    histogram_quantile_from_buckets,
)
from metrics.saturation import (
    SAT_SYSTEM_NAMESPACES,
    completed_effective_duration,
    extract_workload_resource_size_and_cfg_duration,
    load_or_fetch_cluster_snapshot,
    report_effective_uninterrupted_saturation,
)

__all__ = [
    "IGNORED_LABELS",
    "KUEUE_METRICS_CATALOG",
    "MAX_TERMINAL_WIDTH",
    "MetricSpec",
    "MetricType",
    "PrometheusClient",
    "SAT_SYSTEM_NAMESPACES",
    "build_monotonic_counter_points",
    "completed_effective_duration",
    "counter_series_total_increase",
    "extract_workload_resource_size_and_cfg_duration",
    "format_labels",
    "histogram_quantile_from_buckets",
    "load_or_fetch_cluster_snapshot",
    "print_table",
    "priority_sort_key",
    "raw_series_identity",
    "report_aggregated_evictions_and_reclaims",
    "report_aggregated_quota_utilization",
    "report_aggregated_wait_time_by_priority",
    "report_counter",
    "report_effective_uninterrupted_saturation",
    "report_gauge",
    "report_histogram",
    "wrap_cell",
]
