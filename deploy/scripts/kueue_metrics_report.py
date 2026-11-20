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

"""Kueue Prometheus Metrics Catalog & Demo Summary Reporter.

Reference documentation:
  https://kueue.sigs.k8s.io/docs/reference/metrics/
  (Local source in repo: kueue/site/content/en/docs/reference/metrics.md)

How metrics are reported by type:
  - GAUGE:
      Reported with P50, P90, P99, and AVG across all scraped samples in the
      demo execution window [start_time, end_time].
  - COUNTER:
      Reported with P50, P90, P99, and AVG of the counter increase over a
      30-second sliding window (`increase(<metric>[30s])`) across the demo
      execution window [start_time, end_time], plus total increase.
  - HISTOGRAM:
      Reported with P50, P90, and MAX (upper bound of highest populated bucket /
      histogram_quantile(1.0)), along with sample COUNT, AVG, and bucket
      distribution so the reader can see where P50, P90, and MAX fall.

How to update the metrics catalog in the future:
  1. Check https://kueue.sigs.k8s.io/docs/reference/metrics/ (or run
     `make generate-metrics-tables` in `kueue/` and inspect
     `kueue/site/content/en/docs/reference/metrics.md`).
  2. Add, remove, or update MetricSpec entries in `KUEUE_METRICS_CATALOG` below.
     Each entry specifies:
       - section:     Documentation section name
       - name:        Prometheus metric name (base name without _bucket/_sum/_count)
       - metric_type: MetricType.GAUGE, MetricType.COUNTER, or MetricType.HISTOGRAM
       - labels:      Tuple of key Prometheus labels carried by the metric
       - description: Short summary from the Kueue metrics reference table
"""

from __future__ import annotations

import argparse
import atexit
from bisect import bisect_right
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
import json
import math
import os
import re
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

from chamber_common import (
    compute_percentile,
    extract_pod_spec_resources,
    format_compact_si,
    format_num,
    format_pct,
    format_resource_quantity,
    parse_k8s_quantity,
    parse_positive_duration_annotation,
    parse_rfc3339_epoch,
    summarize_samples,
)


class MetricType(str, Enum):
    """Prometheus metric types exported by Kueue."""

    # Gauge: instantaneous value that can go up or down.
    # Reported with: P50, P90, P99, AVG over the run duration.
    GAUGE = "GAUGE"

    # Counter: monotonically increasing cumulative counter.
    # Reported with: P50, P90, P99, AVG of the 30-second window increase.
    COUNTER = "COUNTER"

    # Histogram: cumulative bucketed observations (<name>_bucket, _sum, _count).
    # Reported with: P50, P90, MAX (and COUNT, AVG, bucket distribution).
    HISTOGRAM = "HISTOGRAM"


@dataclass(frozen=True)
class MetricSpec:
    """Declarative specification of a single Kueue Prometheus metric."""

    section: str
    name: str
    metric_type: MetricType
    labels: tuple[str, ...]
    description: str


# ==============================================================================
# KUEUE METRICS CATALOG
# Source of truth: https://kueue.sigs.k8s.io/docs/reference/metrics/
# Local doc file:  kueue/site/content/en/docs/reference/metrics.md
#
# Keep entries grouped by documentation section and ordered alphabetically by
# metric name within each section so diffs against upstream docs stay trivial.
# ==============================================================================
KUEUE_METRICS_CATALOG: tuple[MetricSpec, ...] = (
    # --------------------------------------------------------------------------
    # Section 1: Kueue health
    # Docs: https://kueue.sigs.k8s.io/docs/reference/metrics/#kueue-health
    # --------------------------------------------------------------------------
    MetricSpec(
        section="Kueue Health",
        name="kueue_admission_attempt_duration_seconds",
        metric_type=MetricType.HISTOGRAM,  # Histogram: latency of admission attempts
        labels=("result", "replica_role"),
        description="Latency of an admission attempt (result: success | inadmissible)",
    ),
    MetricSpec(
        section="Kueue Health",
        name="kueue_admission_attempts_total",
        metric_type=MetricType.COUNTER,  # Counter: total attempts to admit workloads
        labels=("result", "replica_role"),
        description="Total number of attempts to admit workloads",
    ),
    MetricSpec(
        section="Kueue Health",
        name="kueue_pod_scheduling_gate_removal_seconds",
        metric_type=MetricType.HISTOGRAM,  # Histogram: workload admission to pod scheduling gate removal
        labels=("name", "cluster_queue", "is_group", "replica_role"),
        description="Duration from Workload admission to removal of a Pod scheduling gate",
    ),
    MetricSpec(
        section="Kueue Health",
        name="kueue_workload_creation_latency_seconds",
        metric_type=MetricType.HISTOGRAM,  # Histogram: job creation to workload creation latency
        labels=("job_kind", "replica_role"),
        description="Time between job creation and Workload creation per job_kind",
    ),
    MetricSpec(
        section="Kueue Health",
        name="multikueue_cluster_status",
        metric_type=MetricType.GAUGE,  # Gauge: MultiKueue worker cluster active status
        labels=("cluster_queue", "cluster", "active", "replica_role"),
        description="MultiKueue worker cluster active status per manager cluster_queue",
    ),
    MetricSpec(
        section="Kueue Health",
        name="multikueue_workloads_admitted_total",
        metric_type=MetricType.COUNTER,  # Counter: remote workload admissions on worker cluster
        labels=("cluster_queue", "cluster", "replica_role"),
        description="Total remote workload admissions on a MultiKueue worker cluster",
    ),
    MetricSpec(
        section="Kueue Health",
        name="multikueue_workloads_dispatched_total",
        metric_type=MetricType.COUNTER,  # Counter: remote workloads dispatched to worker cluster
        labels=("cluster_queue", "cluster", "replica_role"),
        description="Total remote workloads created by MultiKueue manager on a worker cluster",
    ),
    MetricSpec(
        section="Kueue Health",
        name="multikueue_workloads_evicted_total",
        metric_type=MetricType.COUNTER,  # Counter: remote workload evictions on worker cluster
        labels=("cluster_queue", "cluster", "reason", "replica_role"),
        description="Total remote workload evictions on a MultiKueue worker cluster",
    ),

    # --------------------------------------------------------------------------
    # Section 2: ClusterQueue status
    # Docs: https://kueue.sigs.k8s.io/docs/reference/metrics/#clusterqueue-status
    # --------------------------------------------------------------------------
    MetricSpec(
        section="ClusterQueue Status",
        name="kueue_admission_checks_wait_time_seconds",
        metric_type=MetricType.HISTOGRAM,  # Histogram: quota reservation until admission
        labels=("cluster_queue", "priority_class", "replica_role"),
        description="Time from workload quota reservation until admission per cluster_queue",
    ),
    MetricSpec(
        section="ClusterQueue Status",
        name="kueue_admission_cycle_preemption_skips",
        metric_type=MetricType.GAUGE,  # Gauge: workloads skipped for preemption in cycle
        labels=("cluster_queue", "replica_role"),
        description="Workloads in ClusterQueue that had preemption candidates but were skipped",
    ),
    MetricSpec(
        section="ClusterQueue Status",
        name="kueue_admission_wait_time_seconds",
        metric_type=MetricType.HISTOGRAM,  # Histogram: creation/requeue until admission wait time
        labels=("cluster_queue", "priority_class", "replica_role"),
        description="Time between workload creation/requeue and admission per cluster_queue",
    ),
    MetricSpec(
        section="ClusterQueue Status",
        name="kueue_admitted_active_workloads",
        metric_type=MetricType.GAUGE,  # Gauge: currently active admitted workloads
        labels=("cluster_queue", "replica_role"),
        description="Number of admitted Workloads that are currently active per cluster_queue",
    ),
    MetricSpec(
        section="ClusterQueue Status",
        name="kueue_admitted_workloads_total",
        metric_type=MetricType.COUNTER,  # Counter: cumulative admitted workloads
        labels=("cluster_queue", "priority_class", "replica_role"),
        description="Total number of admitted workloads per cluster_queue",
    ),
    MetricSpec(
        section="ClusterQueue Status",
        name="kueue_build_info",
        metric_type=MetricType.GAUGE,  # Gauge: constant 1 with build metadata labels
        labels=("git_version", "git_commit", "build_date", "go_version", "compiler", "platform"),
        description="Kueue build information (constant 1 labeled with version metadata)",
    ),
    MetricSpec(
        section="ClusterQueue Status",
        name="kueue_cluster_queue_info",
        metric_type=MetricType.GAUGE,  # Gauge: constant 1 with hierarchy labels
        labels=("cluster_queue", "parent_cohort", "root_cohort", "dynamic_quota_orchestrator", "replica_role"),
        description="ClusterQueue hierarchy information (constant 1)",
    ),
    MetricSpec(
        section="ClusterQueue Status",
        name="kueue_cluster_queue_resource_pending",
        metric_type=MetricType.GAUGE,  # Gauge: total pending resource requests
        labels=("cluster_queue", "resource", "replica_role"),
        description="ClusterQueue total pending resource requests per resource",
    ),
    MetricSpec(
        section="ClusterQueue Status",
        name="kueue_cluster_queue_status",
        metric_type=MetricType.GAUGE,  # Gauge: 1 for active status (pending | active | terminated)
        labels=("cluster_queue", "status", "replica_role"),
        description="ClusterQueue status indicator (1 for current status)",
    ),
    MetricSpec(
        section="ClusterQueue Status",
        name="kueue_evicted_workloads_once_total",
        metric_type=MetricType.COUNTER,  # Counter: unique workload evictions per cluster_queue
        labels=("cluster_queue", "reason", "underlying_cause", "priority_class", "replica_role"),
        description="Number of unique workload evictions per cluster_queue",
    ),
    MetricSpec(
        section="ClusterQueue Status",
        name="kueue_evicted_workloads_total",
        metric_type=MetricType.COUNTER,  # Counter: total workload evictions per cluster_queue
        labels=("cluster_queue", "reason", "underlying_cause", "priority_class", "replica_role"),
        description="Total number of evicted workloads per cluster_queue",
    ),
    MetricSpec(
        section="ClusterQueue Status",
        name="kueue_execution_time_seconds",
        metric_type=MetricType.HISTOGRAM,  # Histogram: workload execution time from admission to completion
        labels=("cluster_queue", "priority_class", "replica_role"),
        description="Total execution time of a finished admitted workload per cluster_queue",
    ),
    MetricSpec(
        section="ClusterQueue Status",
        name="kueue_finished_workloads",
        metric_type=MetricType.GAUGE,  # Gauge: current number of finished workloads in cache
        labels=("cluster_queue", "replica_role"),
        description="Number of finished workloads currently present per cluster_queue",
    ),
    MetricSpec(
        section="ClusterQueue Status",
        name="kueue_finished_workloads_total",
        metric_type=MetricType.COUNTER,  # Counter: cumulative finished workloads
        labels=("cluster_queue", "priority_class", "replica_role"),
        description="Total number of finished workloads per cluster_queue",
    ),
    MetricSpec(
        section="ClusterQueue Status",
        name="kueue_pending_scheduling_hashes",
        metric_type=MetricType.GAUGE,  # Gauge: unique pending scheduling equivalence hashes
        labels=("cluster_queue", "status", "replica_role"),
        description="Number of unique pending scheduling equivalence hashes per cluster_queue",
    ),
    MetricSpec(
        section="ClusterQueue Status",
        name="kueue_pending_workloads",
        metric_type=MetricType.GAUGE,  # Gauge: pending workloads (status: active | inadmissible)
        labels=("cluster_queue", "status", "replica_role"),
        description="Number of pending workloads per cluster_queue and status",
    ),
    MetricSpec(
        section="ClusterQueue Status",
        name="kueue_pods_ready_to_evicted_time_seconds",
        metric_type=MetricType.HISTOGRAM,  # Histogram: seconds between pods ready and workload eviction
        labels=("cluster_queue", "reason", "underlying_cause", "replica_role"),
        description="Seconds between a workload's pods being ready and eviction per cluster_queue",
    ),
    MetricSpec(
        section="ClusterQueue Status",
        name="kueue_preempted_workloads_total",
        metric_type=MetricType.COUNTER,  # Counter: preempted workloads per preempting_cluster_queue
        labels=("preempting_cluster_queue", "reason", "replica_role"),
        description="Number of preempted workloads per preempting_cluster_queue",
    ),
    MetricSpec(
        section="ClusterQueue Status",
        name="kueue_preemption_target_recomputations_total",
        metric_type=MetricType.COUNTER,  # Counter: preemption target recomputations
        labels=("cluster_queue", "result", "replica_role"),
        description="Total preemption target recomputations on overlapping targets",
    ),
    MetricSpec(
        section="ClusterQueue Status",
        name="kueue_quota_reserved_wait_time_seconds",
        metric_type=MetricType.HISTOGRAM,  # Histogram: creation/requeue until quota reservation
        labels=("cluster_queue", "priority_class", "replica_role"),
        description="Time between workload creation/requeue and quota reservation per cluster_queue",
    ),
    MetricSpec(
        section="ClusterQueue Status",
        name="kueue_quota_reserved_workloads_total",
        metric_type=MetricType.COUNTER,  # Counter: cumulative quota-reserved workloads
        labels=("cluster_queue", "priority_class", "replica_role"),
        description="Total number of quota-reserved workloads per cluster_queue",
    ),
    MetricSpec(
        section="ClusterQueue Status",
        name="kueue_replaced_workload_slices_total",
        metric_type=MetricType.COUNTER,  # Counter: replaced workload slices
        labels=("cluster_queue", "replica_role"),
        description="Number of replaced workload slices per cluster_queue",
    ),
    MetricSpec(
        section="ClusterQueue Status",
        name="kueue_reserving_active_workloads",
        metric_type=MetricType.GAUGE,  # Gauge: workloads currently reserving quota
        labels=("cluster_queue", "replica_role"),
        description="Number of Workloads currently reserving quota per cluster_queue",
    ),
    MetricSpec(
        section="ClusterQueue Status",
        name="kueue_unadmitted_workloads",
        metric_type=MetricType.GAUGE,  # Gauge: unadmitted workloads by reason/underlying_cause
        labels=("cluster_queue", "reason", "underlying_cause", "replica_role"),
        description="Number of unadmitted workloads per cluster_queue, reason, and cause",
    ),
    MetricSpec(
        section="ClusterQueue Status",
        name="kueue_workload_eviction_latency_seconds",
        metric_type=MetricType.HISTOGRAM,  # Histogram: eviction condition True until quota released
        labels=("cluster_queue", "reason", "replica_role"),
        description="Time from workload eviction until workload returns to Pending",
    ),

    # --------------------------------------------------------------------------
    # Section 3: LocalQueue Status (alpha)
    # Docs: https://kueue.sigs.k8s.io/docs/reference/metrics/#localqueue-status-alpha
    # --------------------------------------------------------------------------
    MetricSpec(
        section="LocalQueue Status",
        name="kueue_local_queue_admission_checks_wait_time_seconds",
        metric_type=MetricType.HISTOGRAM,  # Histogram: quota reservation until admission per LocalQueue
        labels=("name", "namespace", "priority_class", "replica_role"),
        description="Time from quota reservation until admission per local_queue",
    ),
    MetricSpec(
        section="LocalQueue Status",
        name="kueue_local_queue_admission_fair_sharing_usage",
        metric_type=MetricType.GAUGE,  # Gauge: LocalQueue Admission Fair Sharing usage
        labels=("name", "namespace", "cluster_queue", "replica_role"),
        description="LocalQueue Admission Fair Sharing resource-weighted usage",
    ),
    MetricSpec(
        section="LocalQueue Status",
        name="kueue_local_queue_admission_wait_time_seconds",
        metric_type=MetricType.HISTOGRAM,  # Histogram: creation/requeue until admission per LocalQueue
        labels=("name", "namespace", "priority_class", "replica_role"),
        description="Time between workload creation/requeue and admission per local_queue",
    ),
    MetricSpec(
        section="LocalQueue Status",
        name="kueue_local_queue_admitted_active_workloads",
        metric_type=MetricType.GAUGE,  # Gauge: active admitted workloads per LocalQueue
        labels=("name", "namespace", "replica_role"),
        description="Number of admitted Workloads that are active per localQueue",
    ),
    MetricSpec(
        section="LocalQueue Status",
        name="kueue_local_queue_admitted_workloads_total",
        metric_type=MetricType.COUNTER,  # Counter: cumulative admitted workloads per LocalQueue
        labels=("name", "namespace", "priority_class", "replica_role"),
        description="Total number of admitted workloads per local_queue",
    ),
    MetricSpec(
        section="LocalQueue Status",
        name="kueue_local_queue_evicted_workloads_total",
        metric_type=MetricType.COUNTER,  # Counter: evicted workloads per LocalQueue
        labels=("name", "namespace", "reason", "underlying_cause", "priority_class", "replica_role"),
        description="Number of evicted workloads per local_queue",
    ),
    MetricSpec(
        section="LocalQueue Status",
        name="kueue_local_queue_execution_time_seconds",
        metric_type=MetricType.HISTOGRAM,  # Histogram: workload execution time per LocalQueue
        labels=("name", "namespace", "priority_class", "replica_role"),
        description="Total execution time of a finished admitted workload per local_queue",
    ),
    MetricSpec(
        section="LocalQueue Status",
        name="kueue_local_queue_finished_workloads",
        metric_type=MetricType.GAUGE,  # Gauge: current finished workloads per LocalQueue
        labels=("name", "namespace", "replica_role"),
        description="Number of finished workloads currently present per local_queue",
    ),
    MetricSpec(
        section="LocalQueue Status",
        name="kueue_local_queue_finished_workloads_total",
        metric_type=MetricType.COUNTER,  # Counter: cumulative finished workloads per LocalQueue
        labels=("name", "namespace", "priority_class", "replica_role"),
        description="Total number of finished workloads per local_queue",
    ),
    MetricSpec(
        section="LocalQueue Status",
        name="kueue_local_queue_pending_workloads",
        metric_type=MetricType.GAUGE,  # Gauge: pending workloads per LocalQueue and status
        labels=("name", "namespace", "status", "replica_role"),
        description="Number of pending workloads per local_queue and status",
    ),
    MetricSpec(
        section="LocalQueue Status",
        name="kueue_local_queue_quota_reserved_wait_time_seconds",
        metric_type=MetricType.HISTOGRAM,  # Histogram: creation/requeue until quota reservation per LocalQueue
        labels=("name", "namespace", "priority_class", "replica_role"),
        description="Time between workload creation/requeue and quota reservation per local_queue",
    ),
    MetricSpec(
        section="LocalQueue Status",
        name="kueue_local_queue_quota_reserved_workloads_total",
        metric_type=MetricType.COUNTER,  # Counter: cumulative quota-reserved workloads per LocalQueue
        labels=("name", "namespace", "priority_class", "replica_role"),
        description="Total number of quota-reserved workloads per local_queue",
    ),
    MetricSpec(
        section="LocalQueue Status",
        name="kueue_local_queue_reserving_active_workloads",
        metric_type=MetricType.GAUGE,  # Gauge: workloads reserving quota per LocalQueue
        labels=("name", "namespace", "replica_role"),
        description="Number of Workloads that are reserving quota per localQueue",
    ),
    MetricSpec(
        section="LocalQueue Status",
        name="kueue_local_queue_resource_reservation",
        metric_type=MetricType.GAUGE,  # Gauge: LocalQueue resource reservation per flavor/resource
        labels=("name", "namespace", "flavor", "resource", "replica_role"),
        description="LocalQueue total resource reservation across flavors",
    ),
    MetricSpec(
        section="LocalQueue Status",
        name="kueue_local_queue_resource_usage",
        metric_type=MetricType.GAUGE,  # Gauge: LocalQueue resource usage per flavor/resource
        labels=("name", "namespace", "flavor", "resource", "replica_role"),
        description="LocalQueue total resource usage across flavors",
    ),
    MetricSpec(
        section="LocalQueue Status",
        name="kueue_local_queue_status",
        metric_type=MetricType.GAUGE,  # Gauge: LocalQueue active status (True | False | Unknown)
        labels=("name", "namespace", "active", "replica_role"),
        description="LocalQueue active status indicator",
    ),
    MetricSpec(
        section="LocalQueue Status",
        name="kueue_local_queue_unadmitted_workloads",
        metric_type=MetricType.GAUGE,  # Gauge: unadmitted workloads per LocalQueue
        labels=("name", "namespace", "cluster_queue", "reason", "underlying_cause", "replica_role"),
        description="Number of unadmitted workloads per local_queue, reason, and cause",
    ),

    # --------------------------------------------------------------------------
    # Section 4: Cohort Status
    # Docs: https://kueue.sigs.k8s.io/docs/reference/metrics/#cohort-status
    # --------------------------------------------------------------------------
    MetricSpec(
        section="Cohort Status",
        name="kueue_cohort_info",
        metric_type=MetricType.GAUGE,  # Gauge: Cohort hierarchy info
        labels=("cohort", "parent_cohort", "root_cohort", "dynamic_quota_orchestrator", "replica_role"),
        description="Cohort hierarchy information (constant 1)",
    ),
    MetricSpec(
        section="Cohort Status",
        name="kueue_cohort_subtree_admitted_active_workloads",
        metric_type=MetricType.GAUGE,  # Gauge: active admitted workloads in cohort subtree
        labels=("cohort", "replica_role"),
        description="Number of admitted Workloads that are active per cohort subtree",
    ),
    MetricSpec(
        section="Cohort Status",
        name="kueue_cohort_subtree_admitted_workloads_total",
        metric_type=MetricType.COUNTER,  # Counter: cumulative admitted workloads in cohort subtree
        labels=("cohort", "priority_class", "replica_role"),
        description="Total number of admitted workloads per cohort subtree",
    ),
    MetricSpec(
        section="Cohort Status",
        name="kueue_cohort_subtree_quota",
        metric_type=MetricType.GAUGE,  # Gauge: aggregated nominal quota in cohort subtree
        labels=("cohort", "flavor", "resource", "replica_role"),
        description="Cohort nominal quota aggregated within the cohort subtree",
    ),
    MetricSpec(
        section="Cohort Status",
        name="kueue_cohort_subtree_resource_reservations",
        metric_type=MetricType.GAUGE,  # Gauge: aggregated resource reservations in cohort subtree
        labels=("cohort", "flavor", "resource", "replica_role"),
        description="Cohort resource reservations aggregated within the cohort subtree",
    ),
    MetricSpec(
        section="Cohort Status",
        name="kueue_cohort_weighted_share",
        metric_type=MetricType.GAUGE,  # Gauge: Cohort fair-sharing weighted share
        labels=("cohort", "replica_role"),
        description="Maximum ratio of usage above nominal quota to lendable resources in Cohort",
    ),

    # --------------------------------------------------------------------------
    # Section 5: Optional Metrics — ClusterQueue Resources
    # Enabled via `metrics.enableClusterQueueResources: true`
    # Docs: https://kueue.sigs.k8s.io/docs/reference/metrics/#optional-metrics
    # --------------------------------------------------------------------------
    MetricSpec(
        section="ClusterQueue Resources (Optional)",
        name="kueue_cluster_queue_borrowing_limit",
        metric_type=MetricType.GAUGE,  # Gauge: borrowing limit per flavor/resource
        labels=("cohort", "cluster_queue", "flavor", "resource", "replica_role"),
        description="ClusterQueue resource borrowing limit across flavors",
    ),
    MetricSpec(
        section="ClusterQueue Resources (Optional)",
        name="kueue_cluster_queue_lending_limit",
        metric_type=MetricType.GAUGE,  # Gauge: lending limit per flavor/resource
        labels=("cohort", "cluster_queue", "flavor", "resource", "replica_role"),
        description="ClusterQueue resource lending limit across flavors",
    ),
    MetricSpec(
        section="ClusterQueue Resources (Optional)",
        name="kueue_cluster_queue_nominal_quota",
        metric_type=MetricType.GAUGE,  # Gauge: nominal quota per flavor/resource
        labels=("cohort", "cluster_queue", "flavor", "resource", "replica_role"),
        description="ClusterQueue resource nominal quota across flavors",
    ),
    MetricSpec(
        section="ClusterQueue Resources (Optional)",
        name="kueue_cluster_queue_resource_reservation",
        metric_type=MetricType.GAUGE,  # Gauge: total resource reservation per flavor/resource
        labels=("cohort", "cluster_queue", "flavor", "resource", "replica_role"),
        description="ClusterQueue total resource reservation across flavors",
    ),
    MetricSpec(
        section="ClusterQueue Resources (Optional)",
        name="kueue_cluster_queue_resource_usage",
        metric_type=MetricType.GAUGE,  # Gauge: total resource usage per flavor/resource
        labels=("cohort", "cluster_queue", "flavor", "resource", "replica_role"),
        description="ClusterQueue total resource usage across flavors",
    ),
    MetricSpec(
        section="ClusterQueue Resources (Optional)",
        name="kueue_cluster_queue_weighted_share",
        metric_type=MetricType.GAUGE,  # Gauge: ClusterQueue fair-sharing weighted share
        labels=("cluster_queue", "cohort", "replica_role"),
        description="ClusterQueue maximum ratio of usage above nominal quota to lendable resources",
    ),

    # --------------------------------------------------------------------------
    # Section 6: Optional Metrics — WaitForPodsReady
    # Enabled via `waitForPodsReady` in manager configuration
    # Docs: https://kueue.sigs.k8s.io/docs/reference/metrics/#optional-metrics
    # --------------------------------------------------------------------------
    MetricSpec(
        section="WaitForPodsReady (Optional)",
        name="kueue_admitted_until_ready_wait_time_seconds",
        metric_type=MetricType.HISTOGRAM,  # Histogram: workload admitted until ready wait time
        labels=("cluster_queue", "priority_class", "replica_role"),
        description="Time between workload admission and ready condition per cluster_queue",
    ),
    MetricSpec(
        section="WaitForPodsReady (Optional)",
        name="kueue_local_queue_admitted_until_ready_wait_time_seconds",
        metric_type=MetricType.HISTOGRAM,  # Histogram: admitted until ready wait time per LocalQueue
        labels=("name", "namespace", "priority_class", "replica_role"),
        description="Time between workload admission and ready condition per local_queue",
    ),
    MetricSpec(
        section="WaitForPodsReady (Optional)",
        name="kueue_local_queue_ready_wait_time_seconds",
        metric_type=MetricType.HISTOGRAM,  # Histogram: creation/requeue until ready per LocalQueue
        labels=("name", "namespace", "priority_class", "replica_role"),
        description="Time between workload creation/requeue and ready condition per local_queue",
    ),
    MetricSpec(
        section="WaitForPodsReady (Optional)",
        name="kueue_local_queue_workload_recovery_wait_time_seconds",
        metric_type=MetricType.HISTOGRAM,  # Histogram: recovery until ready per LocalQueue
        labels=("name", "namespace", "priority_class", "replica_role"),
        description="Time between workload entering recovery and ready condition per local_queue",
    ),
    MetricSpec(
        section="WaitForPodsReady (Optional)",
        name="kueue_ready_wait_time_seconds",
        metric_type=MetricType.HISTOGRAM,  # Histogram: creation/requeue until ready per ClusterQueue
        labels=("cluster_queue", "priority_class", "replica_role"),
        description="Time between workload creation/requeue and ready condition per cluster_queue",
    ),
    MetricSpec(
        section="WaitForPodsReady (Optional)",
        name="kueue_workload_recovery_wait_time_seconds",
        metric_type=MetricType.HISTOGRAM,  # Histogram: recovery until ready per ClusterQueue
        labels=("cluster_queue", "priority_class", "replica_role"),
        description="Time between workload entering recovery and ready condition per cluster_queue",
    ),
)


IGNORED_LABELS = frozenset({"__name__", "instance", "job", "component", "replica_role"})


class PrometheusClient:
    """Queries Prometheus via local HTTP port-forward (auto-started if needed) or kubectl proxy."""

    def __init__(self, prometheus_url: str | None, namespace: str, service: str = "prometheus", port: int = 9090):
        self.prometheus_url = prometheus_url.rstrip("/") if prometheus_url else None
        self.namespace = namespace
        self.service = service
        self.port = port
        self._opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        self._pf_proc: subprocess.Popen | None = None
        self._ensure_connection()

    def _is_url_ready(self, base_url: str, timeout: float = 1.5) -> bool:
        try:
            with self._opener.open(f"{base_url}/-/ready", timeout=timeout) as resp:
                return resp.status == 200
        except Exception:
            return False

    def _ensure_connection(self) -> None:
        if self.prometheus_url and self._is_url_ready(self.prometheus_url):
            return

        # Check common local port-forward ports before spawning a new subprocess
        for default_port in (19090, 9090):
            candidate = f"http://127.0.0.1:{default_port}"
            if candidate != self.prometheus_url and self._is_url_ready(candidate, timeout=0.5):
                self.prometheus_url = candidate
                return

        # Automatically start a local port-forward to svc/prometheus so queries
        # execute directly over HTTP in milliseconds without spawning per-metric
        # kubectl subprocesses.
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            local_port = s.getsockname()[1]

        self._pf_proc = subprocess.Popen(
            [
                "kubectl",
                "-n",
                self.namespace,
                "port-forward",
                f"svc/{self.service}",
                f"{local_port}:{self.port}",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        atexit.register(self.close)

        candidate_url = f"http://127.0.0.1:{local_port}"
        deadline = time.time() + 10.0
        while time.time() < deadline:
            if self._pf_proc.poll() is not None:
                break
            if self._is_url_ready(candidate_url, timeout=1.0):
                self.prometheus_url = candidate_url
                return
            time.sleep(0.2)

    def close(self) -> None:
        if self._pf_proc and self._pf_proc.poll() is None:
            try:
                self._pf_proc.terminate()
                self._pf_proc.wait(timeout=2)
            except Exception:
                pass
            self._pf_proc = None

    def _get_json(self, path: str, params: dict[str, str]) -> dict:
        query_string = urllib.parse.urlencode(params)
        full_path = f"{path}?{query_string}"
        if self.prometheus_url:
            url = f"{self.prometheus_url}{full_path}"
            try:
                with self._opener.open(url, timeout=15) as resp:
                    return json.loads(resp.read().decode("utf-8"))
            except urllib.error.HTTPError as e:
                body = e.read().decode("utf-8", errors="replace").strip()
                raise RuntimeError(f"Prometheus HTTP {e.code} on {full_path}: {body}") from e
            except Exception:
                pass

        raw_path = f"/api/v1/namespaces/{self.namespace}/services/http:{self.service}:{self.port}/proxy{full_path}"
        cmd = ["kubectl", "get", "--raw", raw_path]
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=20)
        if proc.returncode != 0:
            raise RuntimeError(f"Failed to query Prometheus ({raw_path}): {proc.stderr.strip()}")
        return json.loads(proc.stdout)

    def query_instant(self, expr: str, eval_time: float) -> list[dict]:
        data = self._get_json("/api/v1/query", {"query": expr, "time": f"{eval_time:.3f}"})
        if data.get("status") != "success":
            return []
        return data.get("data", {}).get("result", [])

    def query_range(self, expr: str, start: float, end: float, step: float = 1.0) -> list[dict]:
        if end < start:
            end = start
        data = self._get_json(
            "/api/v1/query_range",
            {
                "query": expr,
                "start": f"{start:.3f}",
                "end": f"{end:.3f}",
                "step": f"{step:g}",
            },
        )
        if data.get("status") != "success":
            return []
        return data.get("data", {}).get("result", [])


def format_labels(metric_dict: dict[str, str], extra_exclude: set[str] | None = None) -> str:
    exclude = set(IGNORED_LABELS)
    if extra_exclude:
        exclude |= extra_exclude
    items = [f"{k}={v}" for k, v in sorted(metric_dict.items()) if k not in exclude and v != ""]
    return "\n".join(items) if items else "default"


def _raw_series_identity(metric_dict: dict[str, str], extra_exclude: set[str] | None = None) -> tuple[tuple[str, str], ...]:
    """Return canonical sorted tuple of ALL raw Prometheus labels (preserving pod/instance/replica_role)
    so per-replica counter/histogram deltas never collide or overwrite each other."""
    return tuple(
        sorted(
            (str(k), str(v))
            for k, v in (metric_dict or {}).items()
            if k != "__name__" and (not extra_exclude or k not in extra_exclude)
        )
    )


def _build_monotonic_counter_points(values: list) -> list[tuple[float, float]]:
    """Convert raw Prometheus (ts, val) counter samples into reset-adjusted cumulative (ts, cum_val) points.

    Whenever a counter resets due to a controller restart (`val_i < val_{i-1}`), the post-reset value
    `max(0.0, val_i)` is accumulated onto the running cumulative total (matching Prometheus `increase()`).
    """
    raw_pts: list[tuple[float, float]] = []
    for ts, v_str in values:
        try:
            val = float(v_str)
            if not math.isnan(val) and not math.isinf(val):
                raw_pts.append((float(ts), val))
        except (ValueError, TypeError):
            pass
    if not raw_pts:
        return []
    raw_pts.sort(key=lambda x: x[0])
    cum_pts: list[tuple[float, float]] = [(raw_pts[0][0], raw_pts[0][1])]
    prev_raw = raw_pts[0][1]
    cum_val = raw_pts[0][1]
    for ts, val in raw_pts[1:]:
        if val >= prev_raw:
            cum_val += val - prev_raw
        else:
            # Counter reset (e.g., pod restart): add all new increments since 0
            cum_val += max(0.0, val)
        prev_raw = val
        cum_pts.append((ts, cum_val))
    return cum_pts


def counter_series_total_increase(series: dict, start: float) -> float:
    """Computes the total monotonic increase of a counter series over [start, end], handling counter resets."""
    pts = _build_monotonic_counter_points(series.get("values", []))
    if not pts:
        return 0.0
    ts_list = [ts for ts, _ in pts]
    val_list = [val for _, val in pts]
    start_idx = bisect_right(ts_list, start - 1e-9) - 1
    baseline_at_start = val_list[start_idx] if start_idx >= 0 else 0.0
    return max(0.0, val_list[-1] - baseline_at_start)


def histogram_quantile_from_buckets( q: float, buckets: list[tuple[float, float]]) -> float:
    """Standard Prometheus histogram_quantile implementation over cumulative (le, count) pairs."""
    if not buckets:
        return math.nan
    sorted_b = sorted(buckets, key=lambda x: x[0])
    total = sorted_b[-1][1]
    if total <= 0:
        return math.nan
    rank = q * total
    for i, (upper, cum_count) in enumerate(sorted_b):
        if cum_count >= rank:
            if math.isinf(upper):
                return sorted_b[i - 1][0] if i > 0 else math.nan
            prev_upper = sorted_b[i - 1][0] if i > 0 else 0.0
            prev_count = sorted_b[i - 1][1] if i > 0 else 0.0
            bucket_count = cum_count - prev_count
            if bucket_count <= 0:
                return upper
            return prev_upper + ((rank - prev_count) / bucket_count) * (upper - prev_upper)
    return sorted_b[-1][0]


def report_gauge(
    series_list: list[dict],
    start: float,
) -> list[tuple[str, str, str, str, str]]:
    rows: list[tuple[str, str, str, str, str]] = []
    for series in series_list:
        m = series.get("metric", {})
        lbl_str = format_labels(m)
        res_label = str(m.get("resource") or "").strip()
        is_bytes = res_label in ("memory", "ephemeral-storage")
        raw_vals = []
        for ts, v_str in series.get("values", []):
            try:
                if float(ts) >= start:
                    raw_vals.append(float(v_str))
            except ValueError:
                pass
        if not raw_vals:
            continue
        if all(math.isinf(v) for v in raw_vals):
            rows.append((lbl_str, "+Inf", "+Inf", "+Inf", "+Inf"))
            continue
        stats = summarize_samples(raw_vals)
        if stats is None:
            continue
        p50, p90, p99, avg = stats
        rows.append((
            lbl_str,
            format_num(p50, is_bytes=is_bytes),
            format_num(p90, is_bytes=is_bytes),
            format_num(p99, is_bytes=is_bytes),
            format_num(avg, is_bytes=is_bytes),
        ))
    return sorted(rows, key=lambda r: r[0])


def report_counter(
    series_list: list[dict],
    start: float,
    end: float,
    step: float,
    window_sec: float = 30.0,
) -> list[tuple[str, str, str, str, str, str]]:
    """Computes P50, P90, P99, and AVG of the 30-second sliding-window counter increase."""
    rows: list[tuple[str, str, str, str, str, str]] = []

    num_steps = max(1, int(round((end - start) / step)) + 1)
    eval_times = [start + i * step for i in range(num_steps)]

    for series in series_list:
        lbl_str = format_labels(series.get("metric", {}))
        pts = _build_monotonic_counter_points(series.get("values", []))
        if not pts:
            continue

        # Determine baseline strictly before `start`: if the series was only created
        # at or after `start`, its pre-creation value is 0.0.
        ts_list = [ts for ts, _ in pts]
        val_list = [val for _, val in pts]
        start_idx = bisect_right(ts_list, start - 1e-9) - 1
        baseline_at_start = val_list[start_idx] if start_idx >= 0 else 0.0

        def value_at(t: float) -> float:
            idx = bisect_right(ts_list, t) - 1
            return val_list[idx] if idx >= 0 else baseline_at_start

        increases_30s: list[float] = []
        for t in eval_times:
            cur_val = value_at(t)
            prev_t = t - window_sec
            prev_val = baseline_at_start if prev_t < start else value_at(prev_t)
            inc = max(0.0, cur_val - prev_val)
            increases_30s.append(inc)

        total_inc = max(0.0, val_list[-1] - baseline_at_start)
        stats = summarize_samples(increases_30s)
        if stats is None:
            continue
        p50, p90, p99, avg = stats
        rows.append((
            lbl_str,
            format_num(p50),
            format_num(p90),
            format_num(p99),
            format_num(avg),
            format_num(total_inc),
        ))
    return sorted(rows, key=lambda r: r[0])


def report_histogram(
    end_by_name: dict[str, list[dict]],
    start_by_name: dict[str, list[dict]],
    spec: MetricSpec,
) -> list[tuple[str, str, str, str, str, str, str]]:
    """Computes P50, P90, MAX, AVG, COUNT, and bucket breakdown for a Histogram metric."""
    end_buckets = end_by_name.get(f"{spec.name}_bucket", [])
    if not end_buckets:
        return []
    start_buckets = start_by_name.get(f"{spec.name}_bucket", [])
    end_sums = end_by_name.get(f"{spec.name}_sum", [])
    start_sums = start_by_name.get(f"{spec.name}_sum", [])
    end_counts = end_by_name.get(f"{spec.name}_count", [])
    start_counts = start_by_name.get(f"{spec.name}_count", [])

    def group_key(metric_dict: dict[str, str]) -> str:
        return format_labels(metric_dict, extra_exclude={"le"})

    start_b_map: dict[tuple[tuple[tuple[str, str], ...], float], float] = {}
    for item in start_buckets:
        m = item.get("metric", {})
        le_str = m.get("le", "")
        try:
            le_val = math.inf if le_str == "+Inf" else float(le_str)
            cnt = float(item["value"][1])
            start_b_map[(_raw_series_identity(m, extra_exclude={"le"}), le_val)] = cnt
        except (ValueError, KeyError, IndexError):
            pass

    grouped_bucket_sums: dict[str, dict[float, float]] = {}
    for item in end_buckets:
        m = item.get("metric", {})
        le_str = m.get("le", "")
        try:
            le_val = math.inf if le_str == "+Inf" else float(le_str)
            cnt_end = float(item["value"][1])
            raw_id = _raw_series_identity(m, extra_exclude={"le"})
            gkey = group_key(m)
            cnt_start = start_b_map.get((raw_id, le_val), 0.0)
            delta = max(0.0, cnt_end - cnt_start)
            le_dict = grouped_bucket_sums.setdefault(gkey, {})
            le_dict[le_val] = le_dict.get(le_val, 0.0) + delta
        except (ValueError, KeyError, IndexError):
            pass

    def extract_raw_scalar_map(items: list[dict]) -> dict[tuple[tuple[str, str], ...], tuple[str, float]]:
        out: dict[tuple[tuple[str, str], ...], tuple[str, float]] = {}
        for item in items:
            m = item.get("metric", {})
            try:
                out[_raw_series_identity(m)] = (group_key(m), float(item["value"][1]))
            except (ValueError, KeyError, IndexError):
                pass
        return out

    end_sum_raw = extract_raw_scalar_map(end_sums)
    start_sum_raw = extract_raw_scalar_map(start_sums)
    end_cnt_raw = extract_raw_scalar_map(end_counts)
    start_cnt_raw = extract_raw_scalar_map(start_counts)

    agg_sum_by_gkey: dict[str, float] = {}
    for raw_id, (gkey, end_s) in end_sum_raw.items():
        start_s = start_sum_raw.get(raw_id, (gkey, 0.0))[1]
        agg_sum_by_gkey[gkey] = agg_sum_by_gkey.get(gkey, 0.0) + max(0.0, end_s - start_s)

    agg_cnt_by_gkey: dict[str, float] = {}
    for raw_id, (gkey, end_c) in end_cnt_raw.items():
        start_c = start_cnt_raw.get(raw_id, (gkey, 0.0))[1]
        agg_cnt_by_gkey[gkey] = agg_cnt_by_gkey.get(gkey, 0.0) + max(0.0, end_c - start_c)

    rows: list[tuple[str, str, str, str, str, str, str]] = []
    for gkey, le_dict in sorted(grouped_bucket_sums.items()):
        # Enforce cumulative bucket monotonicity so minor Prometheus float/scrape skew never produces negative diffs
        raw_sorted = sorted(le_dict.items(), key=lambda x: x[0])
        b_sorted: list[tuple[float, float]] = []
        running_max = 0.0
        for le_val, cum in raw_sorted:
            running_max = max(running_max, cum)
            b_sorted.append((le_val, running_max))
        if not b_sorted:
            continue

        total_cnt = agg_cnt_by_gkey.get(gkey, b_sorted[-1][1])
        total_sum = agg_sum_by_gkey.get(gkey, 0.0)
        if total_cnt <= 0:
            continue

        p50 = histogram_quantile_from_buckets(0.50, b_sorted)
        p90 = histogram_quantile_from_buckets(0.90, b_sorted)
        # MAX in a Prometheus histogram:
        # - If count == 1 and sum > 0, the exact single observation is total_sum.
        # - Otherwise, histogram_quantile(1.0) gives the upper bound `le` of the
        #   highest bucket that recorded observations (and is at least avg).
        hist_max = histogram_quantile_from_buckets(1.00, b_sorted)
        avg = total_sum / total_cnt if total_cnt > 0 else math.nan
        if total_cnt == 1 and total_sum > 0:
            hist_max = total_sum
        elif not math.isnan(avg) and hist_max < avg:
            hist_max = avg

        # Build compact non-empty differential bucket summary so the user can see
        # exactly which buckets hold the P50, P90, and MAX observations.
        non_empty_bins: list[str] = []
        prev_cum = 0.0
        for le_val, cum in b_sorted:
            diff = int(round(max(0.0, cum - prev_cum)))
            prev_cum = max(prev_cum, cum)
            if diff > 0:
                le_label = "+Inf" if math.isinf(le_val) else f"{le_val:g}s"
                non_empty_bins.append(f"<={le_label}:{diff}")
        if non_empty_bins:
            bucket_lines = [
                ", ".join(non_empty_bins[i : i + 2])
                for i in range(0, len(non_empty_bins), 2)
            ]
            bucket_summary = "\n".join(bucket_lines)
        else:
            bucket_summary = "none"

        rows.append((
            gkey,
            format_num(p50, "s"),
            format_num(p90, "s"),
            format_num(hist_max, "s"),
            format_num(avg, "s"),
            str(int(round(total_cnt))),
            bucket_summary,
        ))
    return rows


def report_aggregated_quota_utilization(
    range_by_name: dict[str, list[dict]],
    start: float,
) -> list[list[str]]:
    """Computes P50, P90, P99, and AVG utilization (%) of ClusterQueue and Cohort quotas per resource type."""
    rows: list[list[str]] = []

    def _put_max_sample(bucket: dict[tuple[str, str], float], key: tuple[str, str], val: float) -> None:
        if key not in bucket or val > bucket[key]:
            bucket[key] = val

    # 1. ClusterQueue Quotas per resource type
    cq_quota_series = range_by_name.get("kueue_cluster_queue_nominal_quota", [])
    cq_usage_series = range_by_name.get("kueue_cluster_queue_resource_usage", [])
    if not cq_usage_series:
        cq_usage_series = range_by_name.get("kueue_cluster_queue_resource_reservation", [])

    # Map: resource -> ts -> (cluster_queue, flavor) -> value
    cq_quota_map: dict[str, dict[float, dict[tuple[str, str], float]]] = {}
    cq_usage_map: dict[str, dict[float, dict[tuple[str, str], float]]] = {}
    all_cqs_by_res: dict[str, set[str]] = {}
    cq_cohort_by_name: dict[str, str] = {}

    for s in cq_quota_series:
        m = s.get("metric", {})
        res = m.get("resource", "").strip()
        cq = m.get("cluster_queue", "").strip()
        flv = m.get("flavor", "").strip()
        cohort_lbl = m.get("cohort", "").strip()
        if not res or not cq:
            continue
        if cohort_lbl:
            cq_cohort_by_name[cq] = cohort_lbl
        all_cqs_by_res.setdefault(res, set()).add(cq)
        res_ts_map = cq_quota_map.setdefault(res, {})
        for ts_raw, v_str in s.get("values", []):
            try:
                ts = float(ts_raw)
                val = float(v_str)
                if ts >= start and not math.isnan(val) and not math.isinf(val):
                    _put_max_sample(res_ts_map.setdefault(ts, {}), (cq, flv), val)
            except ValueError:
                pass

    for s in cq_usage_series:
        m = s.get("metric", {})
        res = m.get("resource", "").strip()
        cq = m.get("cluster_queue", "").strip()
        flv = m.get("flavor", "").strip()
        cohort_lbl = m.get("cohort", "").strip()
        if not res or not cq:
            continue
        if cohort_lbl and cq not in cq_cohort_by_name:
            cq_cohort_by_name[cq] = cohort_lbl
        all_cqs_by_res.setdefault(res, set()).add(cq)
        res_ts_map = cq_usage_map.setdefault(res, {})
        for ts_raw, v_str in s.get("values", []):
            try:
                ts = float(ts_raw)
                val = float(v_str)
                if ts >= start and not math.isnan(val) and not math.isinf(val):
                    _put_max_sample(res_ts_map.setdefault(ts, {}), (cq, flv), val)
            except ValueError:
                pass

    # 2. Cohort Quotas per resource type (built before CQ formatting so zero-nominal-quota
    #    pure-borrower ClusterQueues can report their share of parent Cohort quota)
    cohort_quota_series = range_by_name.get("kueue_cohort_subtree_quota", [])
    cohort_usage_series = range_by_name.get("kueue_cohort_subtree_resource_reservations", [])

    # Determine cohort hierarchy from kueue_cohort_info so hierarchical parent/child
    # cohorts are not double-counted when aggregating total cohort quota at timestamp t.
    cohort_parent_map: dict[str, str] = {}
    for s in range_by_name.get("kueue_cohort_info", []):
        m = s.get("metric", {})
        c_name = m.get("cohort", "").strip()
        p_name = m.get("parent_cohort", "").strip()
        if c_name:
            cohort_parent_map[c_name] = p_name

    # Map: resource -> ts -> (cohort, flavor) -> value
    cohort_quota_map: dict[str, dict[float, dict[tuple[str, str], float]]] = {}
    cohort_usage_map: dict[str, dict[float, dict[tuple[str, str], float]]] = {}
    all_cohorts_by_res: dict[str, set[str]] = {}

    if cohort_quota_series:
        for s in cohort_quota_series:
            m = s.get("metric", {})
            res = m.get("resource", "").strip()
            cohort = m.get("cohort", "").strip()
            flv = m.get("flavor", "").strip()
            if not res or not cohort:
                continue
            all_cohorts_by_res.setdefault(res, set()).add(cohort)
            res_ts_map = cohort_quota_map.setdefault(res, {})
            for ts_raw, v_str in s.get("values", []):
                try:
                    ts = float(ts_raw)
                    val = float(v_str)
                    if ts >= start and not math.isnan(val) and not math.isinf(val):
                        _put_max_sample(res_ts_map.setdefault(ts, {}), (cohort, flv), val)
                except ValueError:
                    pass

        for s in cohort_usage_series:
            m = s.get("metric", {})
            res = m.get("resource", "").strip()
            cohort = m.get("cohort", "").strip()
            flv = m.get("flavor", "").strip()
            if not res or not cohort:
                continue
            all_cohorts_by_res.setdefault(res, set()).add(cohort)
            res_ts_map = cohort_usage_map.setdefault(res, {})
            for ts_raw, v_str in s.get("values", []):
                try:
                    ts = float(ts_raw)
                    val = float(v_str)
                    if ts >= start and not math.isnan(val) and not math.isinf(val):
                        _put_max_sample(res_ts_map.setdefault(ts, {}), (cohort, flv), val)
                except ValueError:
                    pass
    else:
        # Fallback: aggregate ClusterQueue nominal quota & usage by `cohort` label if
        # `kueue_cohort_subtree_quota` was not emitted.
        for s in cq_quota_series:
            m = s.get("metric", {})
            res = m.get("resource", "").strip()
            cohort = m.get("cohort", "").strip()
            cq = m.get("cluster_queue", "").strip()
            flv = m.get("flavor", "").strip()
            if not res or not cohort:
                continue
            all_cohorts_by_res.setdefault(res, set()).add(cohort)
            res_ts_map = cohort_quota_map.setdefault(res, {})
            for ts_raw, v_str in s.get("values", []):
                try:
                    ts = float(ts_raw)
                    val = float(v_str)
                    if ts >= start and not math.isnan(val) and not math.isinf(val):
                        _put_max_sample(res_ts_map.setdefault(ts, {}), (cohort, f"{cq}:{flv}"), val)
                except ValueError:
                    pass

        for s in cq_usage_series:
            m = s.get("metric", {})
            res = m.get("resource", "").strip()
            cohort = m.get("cohort", "").strip()
            cq = m.get("cluster_queue", "").strip()
            flv = m.get("flavor", "").strip()
            if not res or not cohort:
                continue
            all_cohorts_by_res.setdefault(res, set()).add(cohort)
            res_ts_map = cohort_usage_map.setdefault(res, {})
            for ts_raw, v_str in s.get("values", []):
                try:
                    ts = float(ts_raw)
                    val = float(v_str)
                    if ts >= start and not math.isnan(val) and not math.isinf(val):
                        _put_max_sample(res_ts_map.setdefault(ts, {}), (cohort, f"{cq}:{flv}"), val)
                except ValueError:
                    pass

    for res in sorted(cq_quota_map.keys()):
        is_bytes = res in ("memory", "ephemeral-storage")
        ts_quota = cq_quota_map[res]
        ts_usage = cq_usage_map.get(res, {})
        ts_cohort_quota = cohort_quota_map.get(res, {})
        total_util_samples: list[float] = []
        per_cq_util_samples: list[float] = []
        per_cq_util_by_name: dict[str, list[float]] = {}
        per_cq_cohort_util_by_name: dict[str, list[float]] = {}
        per_cq_used_by_name: dict[str, list[float]] = {}
        per_cq_quota_by_name: dict[str, list[float]] = {}
        all_cqs_seen_for_res: set[str] = set()
        active_cqs_for_ratio: set[str] = set()
        total_used_samples: list[float] = []
        total_quota_samples: list[float] = []

        for ts in sorted(ts_quota.keys()):
            q_entries = ts_quota[ts]
            u_entries = ts_usage.get(ts, {})
            cq_entries = ts_cohort_quota.get(ts, {})
            tot_q = sum(q_entries.values())
            tot_u = sum(u_entries.values())
            if tot_q > 0:
                total_util_samples.append((tot_u / tot_q) * 100.0)
                total_used_samples.append(tot_u)
                total_quota_samples.append(tot_q)

            # Per-ClusterQueue utilization at timestamp ts
            q_by_cq: dict[str, float] = {}
            u_by_cq: dict[str, float] = {}
            q_by_cohort_ts: dict[str, float] = {}
            for (cq, _flv), q_val in q_entries.items():
                q_by_cq[cq] = q_by_cq.get(cq, 0.0) + q_val
            for (cq, _flv), u_val in u_entries.items():
                u_by_cq[cq] = u_by_cq.get(cq, 0.0) + u_val
            for (c_name, _flv), c_val in cq_entries.items():
                q_by_cohort_ts[c_name] = q_by_cohort_ts.get(c_name, 0.0) + c_val
            cqs_at_ts = sorted(set(q_by_cq.keys()) | set(u_by_cq.keys()))
            for cq in cqs_at_ts:
                all_cqs_seen_for_res.add(cq)
                cq_q = q_by_cq.get(cq, 0.0)
                cq_u = u_by_cq.get(cq, 0.0)
                per_cq_used_by_name.setdefault(cq, []).append(cq_u)
                per_cq_quota_by_name.setdefault(cq, []).append(cq_q)
                if cq_q > 0:
                    active_cqs_for_ratio.add(cq)
                    ratio = (cq_u / cq_q) * 100.0
                    per_cq_util_samples.append(ratio)
                    per_cq_util_by_name.setdefault(cq, []).append(ratio)
                else:
                    parent_cohort = cq_cohort_by_name.get(cq, "")
                    parent_q = q_by_cohort_ts.get(parent_cohort, 0.0) if parent_cohort else 0.0
                    if parent_q > 0:
                        per_cq_cohort_util_by_name.setdefault(cq, []).append((cq_u / parent_q) * 100.0)

        stats_total = summarize_samples(total_util_samples)
        if stats_total is not None:
            p50, p90, p99, avg = stats_total
            avg_u = sum(total_used_samples) / len(total_used_samples) if total_used_samples else 0.0
            avg_q = sum(total_quota_samples) / len(total_quota_samples) if total_quota_samples else 0.0
            rows.append([
                "ClusterQueue Quotas (All)",
                res,
                format_pct(p50),
                format_pct(p90),
                format_pct(p99),
                format_pct(avg),
                f"{format_num(avg_u, is_bytes=is_bytes)} / {format_num(avg_q, is_bytes=is_bytes)}",
            ])

        if len(active_cqs_for_ratio) > 1 and per_cq_util_samples:
            stats_per_cq = summarize_samples(per_cq_util_samples)
            if stats_per_cq is not None:
                p50, p90, p99, avg = stats_per_cq
                n_cqs = len(active_cqs_for_ratio)
                rows.append([
                    f"ClusterQueue Quotas (Per-CQ, n={n_cqs})",
                    res,
                    format_pct(p50),
                    format_pct(p90),
                    format_pct(p99),
                    format_pct(avg),
                    "per-queue ratio",
                ])

        if all_cqs_seen_for_res:
            for cq in sorted(all_cqs_seen_for_res):
                u_list = per_cq_used_by_name.get(cq, [])
                q_list = per_cq_quota_by_name.get(cq, [])
                cq_avg_u = sum(u_list) / len(u_list) if u_list else 0.0
                cq_avg_q = sum(q_list) / len(q_list) if q_list else 0.0
                cq_ratios = per_cq_util_by_name.get(cq, [])
                cq_stats = summarize_samples(cq_ratios) if cq_ratios else None
                if cq_stats is not None:
                    cq_p50, cq_p90, cq_p99, cq_avg = cq_stats
                    rows.append([
                        f"ClusterQueue: {cq}",
                        res,
                        format_pct(cq_p50),
                        format_pct(cq_p90),
                        format_pct(cq_p99),
                        format_pct(cq_avg),
                        f"{format_num(cq_avg_u, is_bytes=is_bytes)} / {format_num(cq_avg_q, is_bytes=is_bytes)}",
                    ])
                else:
                    used_quota_str = (
                        f"{format_num(cq_avg_u, is_bytes=is_bytes)} / {format_num(cq_avg_q, is_bytes=is_bytes)}"
                        + (" (borrowed)" if cq_avg_u > 0 else "")
                    )
                    cq_cohort_ratios = per_cq_cohort_util_by_name.get(cq, [])
                    cq_cohort_stats = summarize_samples(cq_cohort_ratios) if cq_cohort_ratios else None
                    if cq_cohort_stats is not None:
                        c_p50, c_p90, c_p99, c_avg = cq_cohort_stats
                        rows.append([
                            f"ClusterQueue: {cq}",
                            res,
                            f"{format_pct(c_p50)} (cohort)",
                            f"{format_pct(c_p90)} (cohort)",
                            f"{format_pct(c_p99)} (cohort)",
                            f"{format_pct(c_avg)} (cohort)",
                            used_quota_str,
                        ])
                    else:
                        rows.append([
                            f"ClusterQueue: {cq}",
                            res,
                            "N/A (0 quota)",
                            "N/A (0 quota)",
                            "N/A (0 quota)",
                            "N/A (0 quota)",
                            used_quota_str,
                        ])

    for res in sorted(cohort_quota_map.keys()):
        is_bytes = res in ("memory", "ephemeral-storage")
        cohorts_for_res = all_cohorts_by_res.get(res, set())
        root_cohorts = {
            c for c in cohorts_for_res
            if not cohort_parent_map.get(c) or cohort_parent_map.get(c) not in cohorts_for_res
        }
        if not root_cohorts:
            root_cohorts = set(cohorts_for_res)

        ts_quota = cohort_quota_map[res]
        ts_usage = cohort_usage_map.get(res, {})
        total_util_samples = []
        per_cohort_util_samples: list[float] = []
        per_cohort_util_by_name: dict[str, list[float]] = {}
        per_cohort_used_by_name: dict[str, list[float]] = {}
        per_cohort_quota_by_name: dict[str, list[float]] = {}
        all_cohorts_seen_for_res: set[str] = set()
        active_cohorts_for_ratio: set[str] = set()
        total_used_samples = []
        total_quota_samples = []

        for ts in sorted(ts_quota.keys()):
            q_entries = ts_quota[ts]
            u_entries = ts_usage.get(ts, {})

            # Sum across root cohorts to avoid double-counting child subtrees
            tot_q = sum(v for (c, _flv), v in q_entries.items() if c in root_cohorts)
            tot_u = sum(v for (c, _flv), v in u_entries.items() if c in root_cohorts)
            if tot_q > 0:
                total_util_samples.append((tot_u / tot_q) * 100.0)
                total_used_samples.append(tot_u)
                total_quota_samples.append(tot_q)

            # Per-Cohort subtree utilization at timestamp ts across all cohorts
            q_by_cohort: dict[str, float] = {}
            u_by_cohort: dict[str, float] = {}
            for (c, _flv), q_val in q_entries.items():
                q_by_cohort[c] = q_by_cohort.get(c, 0.0) + q_val
            for (c, _flv), u_val in u_entries.items():
                u_by_cohort[c] = u_by_cohort.get(c, 0.0) + u_val
            cohorts_at_ts = sorted(set(q_by_cohort.keys()) | set(u_by_cohort.keys()))
            for c in cohorts_at_ts:
                all_cohorts_seen_for_res.add(c)
                c_q = q_by_cohort.get(c, 0.0)
                c_u = u_by_cohort.get(c, 0.0)
                per_cohort_used_by_name.setdefault(c, []).append(c_u)
                per_cohort_quota_by_name.setdefault(c, []).append(c_q)
                if c_q > 0:
                    active_cohorts_for_ratio.add(c)
                    ratio = (c_u / c_q) * 100.0
                    per_cohort_util_samples.append(ratio)
                    per_cohort_util_by_name.setdefault(c, []).append(ratio)

        stats_total = summarize_samples(total_util_samples)
        if stats_total is not None:
            p50, p90, p99, avg = stats_total
            avg_u = sum(total_used_samples) / len(total_used_samples) if total_used_samples else 0.0
            avg_q = sum(total_quota_samples) / len(total_quota_samples) if total_quota_samples else 0.0
            rows.append([
                "Cohort Quotas (All)",
                res,
                format_pct(p50),
                format_pct(p90),
                format_pct(p99),
                format_pct(avg),
                f"{format_num(avg_u, is_bytes=is_bytes)} / {format_num(avg_q, is_bytes=is_bytes)}",
            ])

        if len(active_cohorts_for_ratio) > 1 and per_cohort_util_samples:
            stats_per_cohort = summarize_samples(per_cohort_util_samples)
            if stats_per_cohort is not None:
                p50, p90, p99, avg = stats_per_cohort
                n_cohorts = len(active_cohorts_for_ratio)
                rows.append([
                    f"Cohort Quotas (Per-Cohort, n={n_cohorts})",
                    res,
                    format_pct(p50),
                    format_pct(p90),
                    format_pct(p99),
                    format_pct(avg),
                    "per-cohort ratio",
                ])

        if all_cohorts_seen_for_res:
            for c in sorted(all_cohorts_seen_for_res):
                u_list = per_cohort_used_by_name.get(c, [])
                q_list = per_cohort_quota_by_name.get(c, [])
                c_avg_u = sum(u_list) / len(u_list) if u_list else 0.0
                c_avg_q = sum(q_list) / len(q_list) if q_list else 0.0
                c_ratios = per_cohort_util_by_name.get(c, [])
                c_stats = summarize_samples(c_ratios) if c_ratios else None
                if c_stats is not None:
                    c_p50, c_p90, c_p99, c_avg = c_stats
                    rows.append([
                        f"Cohort: {c}",
                        res,
                        format_pct(c_p50),
                        format_pct(c_p90),
                        format_pct(c_p99),
                        format_pct(c_avg),
                        f"{format_num(c_avg_u, is_bytes=is_bytes)} / {format_num(c_avg_q, is_bytes=is_bytes)}",
                    ])
                else:
                    used_quota_str = (
                        f"{format_num(c_avg_u, is_bytes=is_bytes)} / {format_num(c_avg_q, is_bytes=is_bytes)}"
                        + (" (borrowed)" if c_avg_u > 0 else "")
                    )
                    rows.append([
                        f"Cohort: {c}",
                        res,
                        "N/A (0 quota)",
                        "N/A (0 quota)",
                        "N/A (0 quota)",
                        "N/A (0 quota)",
                        used_quota_str,
                    ])

    return rows


def _priority_sort_key(priority_class: str) -> tuple[int, str]:
    """Orders workload priority classes from highest to lowest tier, then alphabetically."""
    lower = priority_class.lower()
    if "hero" in lower or "urgent" in lower or "critical" in lower:
        return (0, priority_class)
    if "high" in lower:
        return (1, priority_class)
    if "med" in lower or "mid" in lower or "normal" in lower:
        return (2, priority_class)
    if "low" in lower or "bg" in lower or "background" in lower:
        return (3, priority_class)
    return (4, priority_class)


def report_aggregated_wait_time_by_priority(
    end_by_name: dict[str, list[dict]],
    start_by_name: dict[str, list[dict]],
    metric_name: str = "kueue_admission_wait_time_seconds",
) -> list[list[str]]:
    """Computes P50, P90, P99, and AVG wait time aggregated across all ClusterQueues per workload priority."""
    end_buckets = end_by_name.get(f"{metric_name}_bucket", [])
    start_buckets = start_by_name.get(f"{metric_name}_bucket", [])
    end_sums = end_by_name.get(f"{metric_name}_sum", [])
    start_sums = start_by_name.get(f"{metric_name}_sum", [])
    end_counts = end_by_name.get(f"{metric_name}_count", [])
    start_counts = start_by_name.get(f"{metric_name}_count", [])

    if not end_buckets and metric_name == "kueue_admission_wait_time_seconds":
        # Fallback to quota_reserved_wait_time_seconds if admission_wait_time_seconds was not populated
        alt = "kueue_quota_reserved_wait_time_seconds"
        if end_by_name.get(f"{alt}_bucket"):
            return report_aggregated_wait_time_by_priority(end_by_name, start_by_name, metric_name=alt)
        return []

    start_b_map: dict[tuple[tuple[tuple[str, str], ...], float], float] = {}
    for item in start_buckets:
        m = item.get("metric", {})
        le_str = m.get("le", "")
        try:
            le_val = math.inf if le_str == "+Inf" else float(le_str)
            cnt = float(item["value"][1])
            start_b_map[(_raw_series_identity(m, extra_exclude={"le"}), le_val)] = cnt
        except (ValueError, KeyError, IndexError):
            pass

    prio_series_buckets: dict[str, dict[tuple[tuple[str, str], ...], dict[float, float]]] = {}
    all_le_set: set[float] = set()

    for item in end_buckets:
        m = item.get("metric", {})
        le_str = m.get("le", "")
        prio = m.get("priority_class", "").strip() or "<none>"
        try:
            le_val = math.inf if le_str == "+Inf" else float(le_str)
            cnt_end = float(item["value"][1])
            raw_id = _raw_series_identity(m, extra_exclude={"le"})
            cnt_start = start_b_map.get((raw_id, le_val), 0.0)
            delta = max(0.0, cnt_end - cnt_start)
            all_le_set.add(le_val)
            s_map = prio_series_buckets.setdefault(prio, {}).setdefault(raw_id, {})
            s_map[le_val] = s_map.get(le_val, 0.0) + delta
        except (ValueError, KeyError, IndexError):
            pass

    sorted_all_les = sorted(all_le_set)
    prio_buckets: dict[str, dict[float, float]] = {}
    all_buckets: dict[float, float] = {le: 0.0 for le in sorted_all_les}

    for prio, series_dict in prio_series_buckets.items():
        p_map: dict[float, float] = {le: 0.0 for le in sorted_all_les}
        for _raw_id, le_map in series_dict.items():
            prev_cum = 0.0
            for le in sorted_all_les:
                if le in le_map:
                    prev_cum = max(prev_cum, le_map[le])
                p_map[le] += prev_cum
                all_buckets[le] += prev_cum
        prio_buckets[prio] = p_map

    def extract_series_scalar(items: list[dict]) -> dict[tuple[tuple[str, str], ...], tuple[str, float]]:
        out: dict[tuple[tuple[str, str], ...], tuple[str, float]] = {}
        for item in items:
            m = item.get("metric", {})
            prio = m.get("priority_class", "").strip() or "<none>"
            try:
                out[_raw_series_identity(m)] = (prio, float(item["value"][1]))
            except (ValueError, KeyError, IndexError):
                pass
        return out

    end_sum_map = extract_series_scalar(end_sums)
    start_sum_map = extract_series_scalar(start_sums)
    end_cnt_map = extract_series_scalar(end_counts)
    start_cnt_map = extract_series_scalar(start_counts)

    prio_sums: dict[str, float] = {}
    prio_cnts: dict[str, float] = {}
    all_sum = 0.0
    all_cnt = 0.0

    for raw_id, (prio, end_s) in end_sum_map.items():
        start_s = start_sum_map.get(raw_id, (prio, 0.0))[1]
        delta_s = max(0.0, end_s - start_s)
        prio_sums[prio] = prio_sums.get(prio, 0.0) + delta_s
        all_sum += delta_s

    for raw_id, (prio, end_c) in end_cnt_map.items():
        start_c = start_cnt_map.get(raw_id, (prio, 0.0))[1]
        delta_c = max(0.0, end_c - start_c)
        prio_cnts[prio] = prio_cnts.get(prio, 0.0) + delta_c
        all_cnt += delta_c

    rows: list[list[str]] = []
    for prio in sorted(prio_buckets.keys(), key=_priority_sort_key):
        b_sorted = sorted(prio_buckets[prio].items(), key=lambda x: x[0])
        if not b_sorted:
            continue
        total_cnt = prio_cnts.get(prio, b_sorted[-1][1])
        total_sum = prio_sums.get(prio, 0.0)
        if total_cnt <= 0:
            continue
        p50 = histogram_quantile_from_buckets(0.50, b_sorted)
        p90 = histogram_quantile_from_buckets(0.90, b_sorted)
        p99 = histogram_quantile_from_buckets(0.99, b_sorted)
        avg = total_sum / total_cnt if total_cnt > 0 else math.nan
        rows.append([
            prio,
            format_num(p50, "s"),
            format_num(p90, "s"),
            format_num(p99, "s"),
            format_num(avg, "s"),
            str(int(round(total_cnt))),
        ])

    if len(rows) > 1 and all_buckets:
        b_sorted = sorted(all_buckets.items(), key=lambda x: x[0])
        total_cnt = all_cnt if all_cnt > 0 else b_sorted[-1][1]
        if total_cnt > 0:
            p50 = histogram_quantile_from_buckets(0.50, b_sorted)
            p90 = histogram_quantile_from_buckets(0.90, b_sorted)
            p99 = histogram_quantile_from_buckets(0.99, b_sorted)
            avg = all_sum / total_cnt if total_cnt > 0 else math.nan
            rows.append([
                "ALL (Across All Priorities)",
                format_num(p50, "s"),
                format_num(p90, "s"),
                format_num(p99, "s"),
                format_num(avg, "s"),
                str(int(round(total_cnt))),
            ])

    return rows


def report_aggregated_evictions_and_reclaims(
    range_by_name: dict[str, list[dict]],
    start: float,
) -> tuple[list[list[str]], list[list[str]]]:
    """Computes total number of evictions and reclaims (plus breakdown by reason/priority)."""
    evicted_series = range_by_name.get("kueue_evicted_workloads_total", [])
    evicted_once_series = range_by_name.get("kueue_evicted_workloads_once_total", [])
    preempted_series = range_by_name.get("kueue_preempted_workloads_total", [])

    total_evictions = 0.0
    evictions_by_reason_cause: dict[tuple[str, str, str], float] = {}
    reclaims_from_evicted = 0.0

    def _is_reclaim(label_val: str) -> bool:
        low = label_val.lower()
        return "reclaim" in low or "reclam" in low

    for s in evicted_series:
        inc = counter_series_total_increase(s, start)
        if inc <= 0:
            continue
        total_evictions += inc
        m = s.get("metric", {})
        reason = m.get("reason", "").strip() or "<none>"
        cause = m.get("underlying_cause", "").strip() or "<none>"
        prio = m.get("priority_class", "").strip() or "<none>"
        key = (reason, cause, prio)
        evictions_by_reason_cause[key] = evictions_by_reason_cause.get(key, 0.0) + inc
        if _is_reclaim(reason) or _is_reclaim(cause):
            reclaims_from_evicted += inc

    total_unique_evictions = 0.0
    for s in evicted_once_series:
        inc = counter_series_total_increase(s, start)
        if inc > 0:
            total_unique_evictions += inc

    total_preemptions = 0.0
    reclaims_from_preempted = 0.0
    in_cq_preemptions = 0.0
    fair_sharing_preemptions = 0.0
    preemptions_by_reason: dict[str, float] = {}

    for s in preempted_series:
        inc = counter_series_total_increase(s, start)
        if inc <= 0:
            continue
        total_preemptions += inc
        m = s.get("metric", {})
        reason = m.get("reason", "").strip() or "<none>"
        preemptions_by_reason[reason] = preemptions_by_reason.get(reason, 0.0) + inc
        if _is_reclaim(reason):
            reclaims_from_preempted += inc
        elif reason == "InClusterQueue":
            in_cq_preemptions += inc
        elif "fairsharing" in reason.lower():
            fair_sharing_preemptions += inc

    total_reclaims = max(reclaims_from_preempted, reclaims_from_evicted)
    evicted_in_cohort_reclam = sum(
        cnt
        for (r, c, _p), cnt in evictions_by_reason_cause.items()
        if r == "InCohortReclamation" or c == "InCohortReclamation"
    )
    evicted_reclaim_while_borrowing = sum(
        cnt
        for (r, c, _p), cnt in evictions_by_reason_cause.items()
        if r == "InCohortReclaimWhileBorrowing" or c == "InCohortReclaimWhileBorrowing"
    )
    detail_in_cohort_reclam = max(
        preemptions_by_reason.get("InCohortReclamation", 0.0),
        evicted_in_cohort_reclam,
    )
    detail_reclaim_while_borrowing = max(
        preemptions_by_reason.get("InCohortReclaimWhileBorrowing", 0.0),
        evicted_reclaim_while_borrowing,
    )

    summary_rows: list[list[str]] = [
        [
            "Total Evictions",
            "kueue_evicted_workloads_total",
            str(int(round(total_evictions))),
            f"Unique workloads evicted once: {int(round(total_unique_evictions))}",
        ],
        [
            "Total Reclaims",
            "kueue_preempted_workloads_total",
            str(int(round(total_reclaims))),
            (
                f"InCohortReclamation={int(round(detail_in_cohort_reclam))}, "
                f"InCohortReclaimWhileBorrowing={int(round(detail_reclaim_while_borrowing))}"
            ),
        ],
        [
            "Total Preemptions",
            "kueue_preempted_workloads_total",
            str(int(round(total_preemptions))),
            (
                f"InClusterQueue={int(round(in_cq_preemptions))}, "
                f"Reclaims={int(round(total_reclaims))}, "
                f"FairSharing={int(round(fair_sharing_preemptions))}"
            ),
        ],
    ]

    breakdown_rows: list[list[str]] = []
    for (reason, cause, prio), cnt in sorted(evictions_by_reason_cause.items()):
        breakdown_rows.append([
            "Eviction (by reason/priority)",
            f"reason={reason}, underlying_cause={cause}, priority_class={prio}",
            str(int(round(cnt))),
        ])
    for reason, cnt in sorted(preemptions_by_reason.items()):
        breakdown_rows.append([
            "Preemption / Reclaim (by reason)",
            f"reason={reason}",
            str(int(round(cnt))),
        ])

    return summary_rows, breakdown_rows


_SAT_SYSTEM_NAMESPACES: frozenset[str] = frozenset(
    {
        "kube-system",
        "kube-public",
        "kube-node-lease",
        "kueue-system",
        "jobset-system",
        "infra-pods",
    }
)

_parse_rfc3339_ts = parse_rfc3339_epoch
_parse_k8s_qty = parse_k8s_quantity
_fmt_cap_qty = format_resource_quantity
_fmt_compact_si = format_compact_si
_extract_pod_spec_resources = extract_pod_spec_resources


def _parse_pos_float_ann(anns: dict | None, key: str) -> float | None:
    return parse_positive_duration_annotation(anns, key)


def load_or_fetch_cluster_snapshot(
    snapshot_json_path: str,
    chamber_namespace: str,
    kubeconfig_path: str = "",
) -> dict | None:
    """Load pre-captured test-end snapshot JSON, or fall back to live chamber kubectl query if available."""
    if snapshot_json_path:
        try:
            with open(snapshot_json_path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            if isinstance(data, dict) and ("nodes" in data or "workloads" in data):
                return data
        except Exception:
            pass

    if kubeconfig_path:
        kc_path = kubeconfig_path if os.path.isfile(kubeconfig_path) else ""
    elif (
        chamber_namespace
        and "/" not in chamber_namespace
        and "\\" not in chamber_namespace
        and ".." not in chamber_namespace
    ):
        repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        default_kc = os.path.join(repo_root, f"kwok-{chamber_namespace}.kubeconfig")
        kc_path = default_kc if os.path.isfile(default_kc) else ""
    else:
        kc_path = ""

    if not kc_path:
        return None

    try:
        env = dict(os.environ, KUBECONFIG=kc_path)
        nodes_proc = subprocess.run(
            ["kubectl", "get", "nodes", "-o", "json"],
            env=env,
            capture_output=True,
            text=True,
            timeout=15.0,
            check=False,
        )
        wl_proc = subprocess.run(
            ["kubectl", "get", "workloads.kueue.x-k8s.io", "-A", "-o", "json"],
            env=env,
            capture_output=True,
            text=True,
            timeout=15.0,
            check=False,
        )
        pods_proc = subprocess.run(
            ["kubectl", "get", "pods", "-A", "-o", "json"],
            env=env,
            capture_output=True,
            text=True,
            timeout=15.0,
            check=False,
        )
        if nodes_proc.returncode != 0:
            return None
        return {
            "nodes": (json.loads(nodes_proc.stdout) or {}).get("items") or [],
            "workloads": (
                (json.loads(wl_proc.stdout) or {}).get("items") or []
                if wl_proc.returncode == 0 and wl_proc.stdout
                else []
            ),
            "pods": (
                (json.loads(pods_proc.stdout) or {}).get("items") or []
                if pods_proc.returncode == 0 and pods_proc.stdout
                else []
            ),
        }
    except Exception:
        return None


def _extract_workload_resource_size_and_cfg_duration(
    wl: dict,
    assoc_pods: list[dict] | None = None,
) -> tuple[dict[str, float], float | None]:
    """Extract total workload resource requests (summing per-pod resources * pod count across all podSets)
    and configured execution duration from a Kueue Workload (supplemented by status.admission & assoc_pods)."""
    meta = wl.get("metadata") or {}
    spec = wl.get("spec") or {}
    pod_sets = spec.get("podSets") or []
    totals: dict[str, float] = {}
    cfg_dur_candidates: list[float] = []

    wl_dur = _parse_pos_float_ann(meta.get("annotations"), "kueue-optimizer.gke.io/pod-execution-time-seconds")
    if wl_dur is not None:
        cfg_dur_candidates.append(wl_dur)

    for ps in pod_sets:
        if not isinstance(ps, dict):
            continue
        ps_count = max(1, int(ps.get("count") if ps.get("count") is not None else 1))
        tpl = ps.get("template") or {}
        tpl_meta = tpl.get("metadata") or {}
        ps_dur = _parse_pos_float_ann(tpl_meta.get("annotations"), "kueue-optimizer.gke.io/pod-execution-time-seconds")
        if ps_dur is not None:
            cfg_dur_candidates.append(ps_dur)

        per_pod = _extract_pod_spec_resources(tpl.get("spec"))
        for rk, rv in per_pod.items():
            totals[rk] = totals.get(rk, 0.0) + rv * ps_count
        totals["pods"] = totals.get("pods", 0.0) + float(ps_count)

    # Reconcile with Kueue's authoritative status.admission.podSetAssignments across ALL podSets
    psa_totals: dict[str, float] = {}
    psa_pod_count = 0.0
    admission = (wl.get("status") or {}).get("admission") or {}
    for psa in admission.get("podSetAssignments") or []:
        if not isinstance(psa, dict):
            continue
        if psa.get("count") is not None:
            try:
                psa_pod_count += max(1.0, float(psa["count"]))
            except (ValueError, TypeError):
                pass
        r_usage = psa.get("resourceUsage") or {}
        for rk, rv in r_usage.items():
            if rk == "ephemeral-storage":
                continue
            psa_totals[rk] = psa_totals.get(rk, 0.0) + _parse_k8s_qty(rv, rk)

    for rk, rv in psa_totals.items():
        if rv > totals.get(rk, 0.0):
            totals[rk] = rv
    if psa_pod_count > totals.get("pods", 0.0):
        totals["pods"] = psa_pod_count

    # Supplement from instantiated Kubernetes Pod objects if any resource key or duration was only on Pods
    wl_pod_count = max(1, int(round(totals.get("pods", float(len(assoc_pods or [])) or 1.0))))
    totals["pods"] = float(wl_pod_count)
    if assoc_pods:
        pod_res_list: list[dict[str, float]] = []
        for pod in assoc_pods:
            if not isinstance(pod, dict):
                continue
            p_meta = pod.get("metadata") or {}
            p_dur = _parse_pos_float_ann(p_meta.get("annotations"), "kueue-optimizer.gke.io/pod-execution-time-seconds")
            if p_dur is not None:
                cfg_dur_candidates.append(p_dur)
            pod_res_list.append(_extract_pod_spec_resources(pod.get("spec")))

        if pod_res_list:
            all_pod_keys = {rk for pr in pod_res_list for rk in pr}
            for rk in all_pod_keys:
                if totals.get(rk, 0.0) > 0.0:
                    continue
                if len(pod_res_list) == wl_pod_count:
                    totals[rk] = sum(pr.get(rk, 0.0) for pr in pod_res_list)
                else:
                    totals[rk] = max(pr.get(rk, 0.0) for pr in pod_res_list) * wl_pod_count

    cfg_dur = max(cfg_dur_candidates) if cfg_dur_candidates else None
    return totals, cfg_dur


def _completed_effective_duration(
    t_start: float,
    t_finish: float,
    start_epoch: float,
    end_epoch: float,
    cfg_dur: float | None,
) -> tuple[float, float]:
    """Return (d_eff, d_full) for a completed workload/pod within [start_epoch, end_epoch]."""
    start_f = float(start_epoch)
    end_f = float(end_epoch)
    eval_duration = max(0.0, end_f - start_f)
    has_cfg = cfg_dur is not None and cfg_dur > 0.0

    # No overlap with [start_epoch, end_epoch] -> zero effective credit.
    if t_start >= end_f or t_finish <= start_f:
        return 0.0, (float(cfg_dur) if has_cfg else 0.0)

    in_window_wall = max(0.0, min(t_finish, end_f) - max(t_start, start_f))
    if not has_cfg:
        return in_window_wall, in_window_wall

    d_full = float(cfg_dur)
    max_possible = min(d_full, eval_duration)
    if in_window_wall <= 0.0:
        # Same-second RFC3339 timestamps inside the window: actual runtime is < 1s.
        d_eff = min(max_possible, 1.0) if t_finish >= t_start else 0.0
    elif t_start >= start_f and t_finish <= end_f and abs(in_window_wall - max_possible) <= 1.5:
        # Fully inside the window: snap 1s RFC3339 rounding jitter to configured duration.
        d_eff = max_possible
    else:
        d_eff = min(max_possible, in_window_wall)
    return d_eff, d_full


def report_effective_uninterrupted_saturation(
    snapshot: dict | None,
    start_epoch: float,
    end_epoch: float,
) -> tuple[list[list[str]], str]:
    """Compute effective uninterrupted cluster saturation across all cluster resources.

    Qualifying workloads:
      1. Fully completed (succeeded) during the evaluation window [start_epoch, end_epoch], OR
      2. Actively running uninterrupted at the very end of the evaluation window (end_epoch).
    Excluded workloads (0 contribution):
      - Workloads that ran partially or stopped due to preemptions, cohort reclaims, evictions, or failures
        without completing and not actively running at end_epoch.
      - Unadmitted / pending workloads.
    """
    eval_duration = max(0.0, float(end_epoch) - float(start_epoch))
    if not snapshot or eval_duration <= 0.0:
        return [], ""

    nodes = snapshot.get("nodes") or []
    workloads = snapshot.get("workloads") or []
    pods = snapshot.get("pods") or []
    if not nodes:
        return [], ""

    # 1. Aggregate physical cluster capacity from all Nodes (.status.allocatable fallback .status.capacity)
    cluster_cap: dict[str, float] = {}
    for node in nodes:
        if not isinstance(node, dict):
            continue
        st = node.get("status") or {}
        alloc = st.get("allocatable") or st.get("capacity") or {}
        for rk, rv in alloc.items():
            if rk == "ephemeral-storage":
                continue
            cluster_cap[rk] = cluster_cap.get(rk, 0.0) + _parse_k8s_qty(rv, rk)

    if not cluster_cap:
        return [], ""

    # 2. Index Pods by (namespace, workload_or_owner_name) for exact pod phase / startTime cross-checking
    pods_by_wl_key: dict[tuple[str, str], list[dict]] = {}
    for pod in pods:
        if not isinstance(pod, dict):
            continue
        p_meta = pod.get("metadata") or {}
        p_ns = str(p_meta.get("namespace") or "default")
        if p_ns in _SAT_SYSTEM_NAMESPACES:
            continue
        p_labels = p_meta.get("labels") or {}
        p_anns = p_meta.get("annotations") or {}
        candidate_keys: set[str] = set()
        for k_name in (
            p_labels.get("kueue.x-k8s.io/prebuilt-workload-name"),
            p_anns.get("kueue.x-k8s.io/workload"),
            p_labels.get("jobset.sigs.k8s.io/jobset-name"),
            p_labels.get("batch.kubernetes.io/job-name"),
            p_labels.get("job-name"),
        ):
            if k_name:
                candidate_keys.add(str(k_name))
        for k_name in candidate_keys:
            pods_by_wl_key.setdefault((p_ns, k_name), []).append(pod)

    # 3. Classify every non-system Kueue Workload & accumulate uninterrupted resource-seconds
    eff_res_sec_completed: dict[str, float] = {}
    eff_res_sec_active: dict[str, float] = {}
    full_dur_res_sec_qual: dict[str, float] = {}

    cnt_completed = 0
    cnt_active_end = 0
    cnt_excluded_interrupted = 0
    cnt_pending = 0
    pods_completed = 0
    pods_active_end = 0
    pods_excluded_interrupted = 0
    pods_pending = 0
    cnt_infra_completed = 0
    cnt_infra_active = 0

    eval_end_with_tol = float(end_epoch) + 2.0

    # Identify top-level JobSet owners so any duplicate child Job Workloads for the same JobSet are skipped
    jobset_wl_owners: set[tuple[str, str]] = set()
    for wl in workloads:
        if not isinstance(wl, dict):
            continue
        meta = wl.get("metadata") or {}
        wl_ns = str(meta.get("namespace") or "default")
        for oref in meta.get("ownerReferences") or []:
            if isinstance(oref, dict) and str(oref.get("kind") or "") == "JobSet" and oref.get("name"):
                jobset_wl_owners.add((wl_ns, str(oref.get("name"))))

    for wl in workloads:
        if not isinstance(wl, dict):
            continue
        meta = wl.get("metadata") or {}
        wl_ns = str(meta.get("namespace") or "default")
        if wl_ns in _SAT_SYSTEM_NAMESPACES:
            continue
        wl_name = str(meta.get("name") or "")
        orefs = [oref for oref in (meta.get("ownerReferences") or []) if isinstance(oref, dict)]
        owner_names = [str(oref.get("name") or "") for oref in orefs if oref.get("name")]

        # If a parent JobSet Workload already exists in this namespace, skip duplicate child Job Workloads
        if jobset_wl_owners and not any(str(o.get("kind") or "") == "JobSet" for o in orefs):
            parent_js: str | None = (meta.get("labels") or {}).get("jobset.sigs.k8s.io/jobset-name")
            if not parent_js:
                for ps in ((wl.get("spec") or {}).get("podSets") or []):
                    if isinstance(ps, dict):
                        parent_js = (((ps.get("template") or {}).get("metadata") or {}).get("labels") or {}).get(
                            "jobset.sigs.k8s.io/jobset-name"
                        )
                        if parent_js:
                            break
            if parent_js and (wl_ns, str(parent_js)) in jobset_wl_owners:
                continue

        # Gather associated pods first so pod-level resource requests/limits & annotations can supplement Workload spec
        raw_assoc_pods: list[dict] = list(pods_by_wl_key.get((wl_ns, wl_name), []))
        if not raw_assoc_pods:
            for oname in owner_names:
                if (wl_ns, oname) in pods_by_wl_key:
                    raw_assoc_pods.extend(pods_by_wl_key[(wl_ns, oname)])
                    break
        seen_pod_ids: set[str] = set()
        assoc_pods: list[dict] = []
        for p in raw_assoc_pods:
            pm = p.get("metadata") or {}
            pid = str(pm.get("uid") or pm.get("name") or id(p))
            if pid not in seen_pod_ids:
                seen_pod_ids.add(pid)
                assoc_pods.append(p)

        wl_sizes, cfg_dur = _extract_workload_resource_size_and_cfg_duration(wl, assoc_pods=assoc_pods)
        wl_pod_count = int(round(wl_sizes.get("pods", 1.0)))

        pod_phases = [str((p.get("status") or {}).get("phase") or "") for p in assoc_pods]
        pod_start_epochs = [
            ts
            for p in assoc_pods
            for ts in (_parse_rfc3339_ts((p.get("status") or {}).get("startTime")),)
            if ts is not None
        ]

        # Inspect Kueue Workload status.conditions
        st = wl.get("status") or {}
        conds = {
            str(c.get("type")): c
            for c in (st.get("conditions") or [])
            if isinstance(c, dict) and c.get("type")
        }
        c_finished = conds.get("Finished")
        c_admitted = conds.get("Admitted")
        c_quota = conds.get("QuotaReserved")
        c_evicted = conds.get("Evicted")
        c_preempted = conds.get("Preempted")

        is_finished_true = bool(c_finished and str(c_finished.get("status")) == "True")
        fin_reason = str((c_finished or {}).get("reason") or "").lower()
        fin_msg = str((c_finished or {}).get("message") or "").lower()
        is_failed_finish = any(tok in fin_reason or tok in fin_msg for tok in ("fail", "error", "timeout"))
        has_failed_pods = any(ph == "Failed" for ph in pod_phases) and not all(
            ph == "Succeeded" for ph in pod_phases if ph
        )

        is_evicted_true = bool(c_evicted and str(c_evicted.get("status")) == "True")
        is_preempted_true = bool(c_preempted and str(c_preempted.get("status")) == "True")
        is_admitted_true = bool(
            (c_admitted and str(c_admitted.get("status")) == "True")
            or (c_quota and str(c_quota.get("status")) == "True")
        )
        requeue_cnt = int(((st.get("requeueState") or {}).get("count")) or 0)

        t_admit = (
            _parse_rfc3339_ts((c_admitted or {}).get("lastTransitionTime"))
            or _parse_rfc3339_ts((c_quota or {}).get("lastTransitionTime"))
            or (min(pod_start_epochs) if pod_start_epochs else None)
            or _parse_rfc3339_ts(meta.get("creationTimestamp"))
            or float(start_epoch)
        )
        # If the workload was previously evicted/preempted and requeued, its uninterrupted run
        # cannot begin earlier than its most recent eviction transition timestamp.
        t_evicted = _parse_rfc3339_ts((c_evicted or {}).get("lastTransitionTime"))
        if requeue_cnt > 0 and t_evicted is not None and t_evicted > t_admit:
            t_admit = t_evicted

        t_finish = _parse_rfc3339_ts((c_finished or {}).get("lastTransitionTime")) or float(end_epoch)

        # Ignore out-of-window completions (finished at/before start_epoch or admitted at/after end_epoch)
        if is_finished_true and (t_finish <= float(start_epoch) or t_admit >= float(end_epoch)):
            continue

        # Case 1: Fully completed (succeeded) during the evaluation window
        all_pods_succeeded = bool(pod_phases) and all(ph == "Succeeded" for ph in pod_phases)
        if (is_finished_true and not is_failed_finish and not has_failed_pods and t_finish <= eval_end_with_tol) or (
            all_pods_succeeded and not is_evicted_true and not is_failed_finish
        ):
            if t_finish <= float(start_epoch) or t_admit >= float(end_epoch):
                continue
            cnt_completed += 1
            pods_completed += wl_pod_count
            d_eff, d_full = _completed_effective_duration(t_admit, t_finish, start_epoch, end_epoch, cfg_dur)

            for rk, r_amt in wl_sizes.items():
                eff_res_sec_completed[rk] = eff_res_sec_completed.get(rk, 0.0) + r_amt * d_eff
                full_dur_res_sec_qual[rk] = full_dur_res_sec_qual.get(rk, 0.0) + r_amt * d_full
            continue

        # Case 2: Actively running uninterrupted at the very end of the test (t_eval = end_epoch)
        has_active_pods = (not pod_phases) or any(ph in ("Running", "Succeeded") for ph in pod_phases)
        if (
            is_admitted_true
            and not is_evicted_true
            and not is_preempted_true
            and not is_failed_finish
            and not has_failed_pods
            and has_active_pods
            and t_admit < float(end_epoch)
        ):
            cnt_active_end += 1
            pods_active_end += wl_pod_count
            effective_start = max(float(start_epoch), min(float(t_admit), float(end_epoch)))
            elapsed_uninterrupted = max(0.0, float(end_epoch) - effective_start)
            if cfg_dur is not None and cfg_dur > 0.0:
                d_eff = min(float(cfg_dur), elapsed_uninterrupted)
                d_full = float(cfg_dur)
            else:
                d_eff = elapsed_uninterrupted
                d_full = elapsed_uninterrupted

            for rk, r_amt in wl_sizes.items():
                eff_res_sec_active[rk] = eff_res_sec_active.get(rk, 0.0) + r_amt * d_eff
                full_dur_res_sec_qual[rk] = full_dur_res_sec_qual.get(rk, 0.0) + r_amt * d_full
            continue

        # Case 3: Interrupted by preemption, cohort reclaim, eviction, or failure (0 contribution)
        # vs never-admitted (Pending / Inadmissible) workloads.
        has_admission_record = bool((st.get("admission") or {}).get("clusterQueue"))
        has_started_pods = any(ph in ("Running", "Succeeded", "Failed") for ph in pod_phases)
        was_ever_admitted_or_evicted = (
            is_admitted_true
            or has_admission_record
            or has_started_pods
            or is_evicted_true
            or is_preempted_true
            or is_failed_finish
            or has_failed_pods
            or requeue_cnt > 0
        )
        if was_ever_admitted_or_evicted:
            cnt_excluded_interrupted += 1
            pods_excluded_interrupted += wl_pod_count
        else:
            cnt_pending += 1
            pods_pending += wl_pod_count

    # 3b. Account for pre-existing t0 running pods in namespace "infra-pods" (created outside
    #     Kueue with kueue.x-k8s.io/exclude=true to model initial steady-state cluster occupancy).
    for pod in pods:
        if not isinstance(pod, dict):
            continue
        p_meta = pod.get("metadata") or {}
        p_ns = str(p_meta.get("namespace") or "default")
        if p_ns != "infra-pods":
            continue
        p_spec = pod.get("spec") or {}
        p_status = pod.get("status") or {}
        per_pod_res = _extract_pod_spec_resources(p_spec)
        if not per_pod_res:
            continue
        per_pod_res["pods"] = 1.0
        cfg_dur = _parse_pos_float_ann(p_meta.get("annotations"), "kueue-optimizer.gke.io/pod-execution-time-seconds")
        phase = str(p_status.get("phase") or "")
        t_start = (
            _parse_rfc3339_ts(p_status.get("startTime"))
            or _parse_rfc3339_ts(p_meta.get("creationTimestamp"))
            or float(start_epoch)
        )
        term_fin_epochs: list[float] = []
        for cs in p_status.get("containerStatuses") or []:
            if isinstance(cs, dict):
                t_fin_cs = _parse_rfc3339_ts((((cs.get("state") or {}).get("terminated") or {}).get("finishedAt")))
                if t_fin_cs is not None:
                    term_fin_epochs.append(t_fin_cs)
        t_pod_fin = max(term_fin_epochs) if term_fin_epochs else _parse_rfc3339_ts(p_meta.get("deletionTimestamp"))

        if phase == "Succeeded":
            if (t_pod_fin is not None and t_pod_fin <= float(start_epoch)) or t_start >= float(end_epoch):
                continue
            cnt_infra_completed += 1
            t_pfin = t_pod_fin if t_pod_fin is not None else float(end_epoch)
            d_eff, d_full = _completed_effective_duration(t_start, t_pfin, start_epoch, end_epoch, cfg_dur)
            for rk, r_amt in per_pod_res.items():
                eff_res_sec_completed[rk] = eff_res_sec_completed.get(rk, 0.0) + r_amt * d_eff
                full_dur_res_sec_qual[rk] = full_dur_res_sec_qual.get(rk, 0.0) + r_amt * d_full
        elif phase == "Running" and t_start < float(end_epoch):
            cnt_infra_active += 1
            effective_start = max(float(start_epoch), min(float(t_start), float(end_epoch)))
            elapsed_uninterrupted = max(0.0, float(end_epoch) - effective_start)
            if cfg_dur is not None and cfg_dur > 0.0:
                d_eff = min(float(cfg_dur), elapsed_uninterrupted)
                d_full = float(cfg_dur)
            else:
                d_eff = elapsed_uninterrupted
                d_full = elapsed_uninterrupted
            for rk, r_amt in per_pod_res.items():
                eff_res_sec_active[rk] = eff_res_sec_active.get(rk, 0.0) + r_amt * d_eff
                full_dur_res_sec_qual[rk] = full_dur_res_sec_qual.get(rk, 0.0) + r_amt * d_full

    # 4. Order resource rows: accelerators first (e.g. google.com/tpu, nvidia.com/gpu), then cpu, memory, pods
    accel_keys = sorted(
        rk for rk in cluster_cap if rk not in ("cpu", "memory", "pods") and cluster_cap.get(rk, 0.0) > 0.0
    )
    ordered_res = accel_keys + [
        rk for rk in ("cpu", "memory", "pods") if cluster_cap.get(rk, 0.0) > 0.0
    ]

    rows: list[list[str]] = []
    for rk in ordered_res:
        c_val = cluster_cap.get(rk, 0.0)
        denom = c_val * eval_duration
        if denom <= 0.0:
            continue
        comp_s = eff_res_sec_completed.get(rk, 0.0)
        act_s = eff_res_sec_active.get(rk, 0.0)
        tot_eff_s = comp_s + act_s
        full_s = full_dur_res_sec_qual.get(rk, 0.0)

        eff_pct = (tot_eff_s / denom) * 100.0
        comp_pct = (comp_s / denom) * 100.0
        act_pct = (act_s / denom) * 100.0
        full_pct = (full_s / denom) * 100.0

        unit_lbl = "core-s" if rk == "cpu" else ("GiB-s" if rk == "memory" else f"{rk.split('/')[-1]}-s")
        rows.append(
            [
                rk,
                _fmt_cap_qty(c_val, rk),
                f"{eff_pct:.2f}%",
                f"{comp_pct:.2f}%",
                f"{act_pct:.2f}%",
                f"{full_pct:.2f}%",
                f"{_fmt_compact_si(tot_eff_s)} / {_fmt_compact_si(denom)} {unit_lbl}",
            ]
        )

    infra_part = (
        f" | Initial Infra Pods: {cnt_infra_completed + cnt_infra_active} "
        f"(completed={cnt_infra_completed}, active_at_end={cnt_infra_active})"
        if (cnt_infra_completed + cnt_infra_active) > 0
        else ""
    )
    unadmitted_str = (
        f"{cnt_pending} [{pods_pending} pods]" if cnt_pending > 0 and pods_pending > 0 else str(cnt_pending)
    )
    subtitle = (
        f"    Eval Window: {eval_duration:.1f}s | "
        f"Qualifying Workloads: {cnt_completed + cnt_active_end} "
        f"(completed={cnt_completed} [{pods_completed} pods], "
        f"active_at_end={cnt_active_end} [{pods_active_end} pods]){infra_part} | "
        f"Excluded (preempted/reclaimed/failed): {cnt_excluded_interrupted} [{pods_excluded_interrupted} pods] | "
        f"Unadmitted: {unadmitted_str}"
    )
    return rows, subtitle


MAX_TERMINAL_WIDTH = 120


def _wrap_single_line(line: str, width: int) -> list[str]:
    """Wraps a single line to at most `width` characters at natural boundaries."""
    if len(line) <= width or width <= 0:
        return [line]

    # 1. Prefer wrapping comma-separated lists (e.g. labels "{a=1, b=2}" or buckets "<=1s:5, <=10s:2")
    if ", " in line:
        raw_parts = line.split(", ")
        parts = [p + "," if i < len(raw_parts) - 1 else p for i, p in enumerate(raw_parts)]
        lines: list[str] = []
        cur = ""
        for part in parts:
            candidate = f"{cur} {part}" if cur else part
            if len(candidate) <= width:
                cur = candidate
            else:
                if cur:
                    lines.extend(_wrap_single_line(cur, width))
                if len(part) <= width:
                    cur = part
                else:
                    lines.extend(_wrap_single_line(part, width))
                    cur = ""
        if cur:
            lines.extend(_wrap_single_line(cur, width))
        return lines

    # 2. Next prefer wrapping on spaces (e.g. SECTION names like "ClusterQueue Resources (Optional)")
    if " " in line:
        words = line.split(" ")
        lines = []
        cur = ""
        for word in words:
            candidate = f"{cur} {word}" if cur else word
            if len(candidate) <= width:
                cur = candidate
            else:
                if cur:
                    lines.extend(_wrap_single_line(cur, width))
                if len(word) <= width:
                    cur = word
                else:
                    lines.extend(_wrap_single_line(word, width))
                    cur = ""
        if cur:
            lines.extend(_wrap_single_line(cur, width))
        return lines

    # 3. Next wrap on identifier/label delimiters (_, =, -, /, :) so metric names
    #    and key=value pairs break cleanly at token boundaries.
    tokens = re.findall(r"[^_=\-/:]+[_=\-/:]*", line)
    if len(tokens) > 1:
        lines = []
        cur = ""
        for tok in tokens:
            if len(cur) + len(tok) <= width:
                cur += tok
            else:
                if cur:
                    lines.append(cur)
                if len(tok) <= width:
                    cur = tok
                else:
                    for idx in range(0, len(tok), width):
                        chunk = tok[idx : idx + width]
                        if len(chunk) == width:
                            lines.append(chunk)
                        else:
                            cur = chunk
        if cur:
            lines.append(cur)
        return lines

    # 4. Fallback hard slice for single contiguous tokens exceeding `width`.
    return [line[idx : idx + width] for idx in range(0, len(line), width)]


def wrap_cell(cell: str, width: int) -> list[str]:
    """Wraps a potentially multi-line cell string so every line is <= width chars."""
    if not cell:
        return [""]
    out: list[str] = []
    for raw_line in cell.splitlines():
        out.extend(_wrap_single_line(raw_line, width))
    return out or [""]


def print_table(headers: list[str], rows: list[list[str]], max_width: int = MAX_TERMINAL_WIDTH) -> None:
    if not rows:
        return
    num_cols = len(headers)
    border_overhead = 2 + 3 * (num_cols - 1)
    available_width = max(num_cols * 4, max_width - border_overhead)

    def compute_actual_widths(limits: list[int]) -> list[int]:
        widths = [0] * num_cols
        for i in range(num_cols):
            widths[i] = max(len(line) for line in wrap_cell(headers[i], limits[i]))
        for row in rows:
            for i in range(num_cols):
                widths[i] = max(widths[i], max(len(line) for line in wrap_cell(row[i], limits[i])))
        return widths

    natural_widths = [max(len(line) for line in h.splitlines()) for h in headers]
    for row in rows:
        for idx, cell in enumerate(row):
            cell_max = max((len(line) for line in cell.splitlines()), default=0)
            natural_widths[idx] = max(natural_widths[idx], cell_max)

    col_widths = list(natural_widths)

    # First wrap SECTION (column 0) at word boundaries (<= 16 chars) if the table
    # exceeds available_width.
    if sum(col_widths) > available_width and col_widths[0] > 16:
        col_widths[0] = 16
        col_widths = compute_actual_widths(col_widths)

    # Protect compact columns (numeric stats, short headers <= 12 chars) from
    # wrapping, and iteratively reduce the widest column until actual rendered
    # width fits within `available_width`.
    min_widths = [
        col_widths[i]
        if natural_widths[i] <= 12
        else max(10, min(max(len(line) for line in headers[i].splitlines()), 14))
        for i in range(num_cols)
    ]
    while sum(col_widths) > available_width:
        reducible = [i for i in range(num_cols) if col_widths[i] > min_widths[i]]
        if not reducible:
            reducible = [i for i in range(num_cols) if col_widths[i] > 4]
            if not reducible:
                break
        widest_idx = max(reducible, key=lambda i: col_widths[i])
        col_widths[widest_idx] -= 1
        col_widths = compute_actual_widths(col_widths)

    wrapped_headers = [wrap_cell(headers[i], col_widths[i]) for i in range(num_cols)]
    wrapped_rows = [[wrap_cell(row[i], col_widths[i]) for i in range(num_cols)] for row in rows]
    actual_widths = compute_actual_widths(col_widths)

    fmt = "  " + " | ".join(f"{{:<{w}}}" for w in actual_widths)
    sep = "  " + "-+-".join("-" * w for w in actual_widths)

    header_height = max(len(cell_lines) for cell_lines in wrapped_headers)
    for line_idx in range(header_height):
        line_cells = [
            wrapped_headers[col_idx][line_idx] if line_idx < len(wrapped_headers[col_idx]) else ""
            for col_idx in range(num_cols)
        ]
        print(fmt.format(*line_cells).rstrip())
    print(sep)

    has_multiline_rows = any(len(cell_lines) > 1 for wrow in wrapped_rows for cell_lines in wrow)
    for row_idx, wrow in enumerate(wrapped_rows):
        row_height = max(len(cell_lines) for cell_lines in wrow)
        for line_idx in range(row_height):
            line_cells = [
                wrow[col_idx][line_idx] if line_idx < len(wrow[col_idx]) else ""
                for col_idx in range(num_cols)
            ]
            print(fmt.format(*line_cells).rstrip())
        if has_multiline_rows and row_idx < len(wrapped_rows) - 1:
            print(sep)


def main() -> int:
    parser = argparse.ArgumentParser(description="Print Demo Summary and Kueue Prometheus Metrics Report")
    parser.add_argument("--namespace", required=True, help="Host cluster namespace for the chamber")
    parser.add_argument("--workload-namespace", default="ai-lab", help="Workload namespace in KWOK cluster")
    parser.add_argument("--queueing-strategy", default="StrictFIFO", help="ClusterQueue queueingStrategy (StrictFIFO or BestEffortFIFO)")
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

    print(f"  [4/4] Computing P50 / P90 / P99 / AVG / MAX across {len(KUEUE_METRICS_CATALOG)} catalog metrics...", flush=True)

    # Group catalog entries by metric type while preserving section metadata
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
