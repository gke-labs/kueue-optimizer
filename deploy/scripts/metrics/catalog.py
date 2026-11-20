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

"""Kueue Prometheus metrics catalog, label formatting, and priority ordering helpers."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


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


def format_labels(metric_dict: dict[str, str], extra_exclude: set[str] | None = None) -> str:
    exclude = set(IGNORED_LABELS)
    if extra_exclude:
        exclude |= extra_exclude
    items = [f"{k}={v}" for k, v in sorted(metric_dict.items()) if k not in exclude and v != ""]
    return "\n".join(items) if items else "default"


def raw_series_identity(
    metric_dict: dict[str, str],
    extra_exclude: set[str] | None = None,
) -> tuple[tuple[str, str], ...]:
    """Return canonical sorted tuple of ALL raw Prometheus labels (preserving pod/instance/replica_role)
    so per-replica counter/histogram deltas never collide or overwrite each other."""
    return tuple(
        sorted(
            (str(k), str(v))
            for k, v in (metric_dict or {}).items()
            if k != "__name__" and (not extra_exclude or k not in extra_exclude)
        )
    )


def priority_sort_key(priority_class: str) -> tuple[int, str]:
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
