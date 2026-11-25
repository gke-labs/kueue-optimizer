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

"""Metric report aggregations for Sections 1–3 and 5.1–5.3 (gauges, counters, histograms, quotas, wait times, evictions)."""

from __future__ import annotations

from bisect import bisect_right
from collections.abc import Callable
import math

from chamber.parsers import format_num, format_pct, summarize_samples
from metrics.catalog import MetricSpec, format_labels, priority_sort_key, raw_series_identity
from metrics.prometheus import (
    build_monotonic_counter_points,
    counter_series_total_increase,
    histogram_quantile_from_buckets,
)

# Static configuration/status gauges whose absence before the first scrape does not mean 0.
_STATIC_GAUGE_METRICS: frozenset[str] = frozenset({
    "kueue_build_info",
    "kueue_cluster_queue_info",
    "kueue_cohort_info",
    "kueue_cluster_queue_status",
    "kueue_local_queue_status",
    "multikueue_cluster_status",
    "kueue_cluster_queue_nominal_quota",
    "kueue_cluster_queue_borrowing_limit",
    "kueue_cluster_queue_lending_limit",
    "kueue_cohort_subtree_quota",
})

# Exact canonical Kueue preemption/eviction reason labels for cohort reclaims and fair sharing.
_RECLAIM_REASONS: frozenset[str] = frozenset({
    "InCohortReclamation",
    "InCohortReclaimWhileBorrowing",
})
_FAIR_SHARING_REASONS: frozenset[str] = frozenset({
    "InCohortFairSharing",
    "FairSharing",
})

RawSeriesId = tuple[tuple[str, str], ...]


def report_gauge(
    series_list: list[dict],
    start: float,
    end: float | None = None,
    *,
    step: float | None = None,
) -> list[tuple[str, str, str, str, str]]:
    """Computes P50, P90, P99, and AVG over [start, end] for Gauge series.

    When both `end` and `step` (> 0) are provided, non-static gauges that Kueue deletes when
    they drop to 0 (e.g., `kueue_cohort_subtree_resource_reservations`) are zero-filled for
    missing query grid steps in `[start, end]` so `AVG` and percentiles reflect the true
    run-wide distribution instead of only active periods.
    """
    rows: list[tuple[str, str, str, str, str]] = []
    for series in series_list:
        m = series.get("metric", {})
        lbl_str = format_labels(m)
        metric_name = str(m.get("__name__") or "").strip()
        res_label = str(m.get("resource") or "").strip()
        is_bytes = res_label in ("memory", "ephemeral-storage")
        raw_vals: list[float] = []
        fallback_vals: list[float] = []
        for ts, v_str in series.get("values", []):
            try:
                ts_val = float(ts)
                if ts_val >= start:
                    v_val = float(v_str)
                    fallback_vals.append(v_val)
                    if end is None or ts_val <= end + 1e-9:
                        raw_vals.append(v_val)
            except ValueError:
                pass
        used_fallback = False
        if not raw_vals and fallback_vals:
            raw_vals = fallback_vals
            used_fallback = True
        if not raw_vals:
            continue
        if all(math.isinf(v) for v in raw_vals):
            rows.append((lbl_str, "+Inf", "+Inf", "+Inf", "+Inf"))
            continue
        if (
            not used_fallback
            and end is not None
            and end > start
            and step is not None
            and step > 0
            and metric_name not in _STATIC_GAUGE_METRICS
        ):
            expected_steps = max(len(raw_vals), int(round((end - start) / step)))
            missing = expected_steps - len(raw_vals)
            if missing > 0:
                raw_vals.extend([0.0] * missing)
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
        pts = build_monotonic_counter_points(series.get("values", []))
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


def _parse_histogram_bucket_map(items: list[dict]) -> dict[RawSeriesId, dict[float, float]]:
    out: dict[RawSeriesId, dict[float, float]] = {}
    for item in items:
        m = item.get("metric", {})
        le_str = m.get("le", "")
        try:
            le_val = math.inf if le_str == "+Inf" else float(le_str)
            cnt = float(item["value"][1])
            if math.isnan(cnt):
                continue
            raw_id = raw_series_identity(m, extra_exclude={"le"})
            out.setdefault(raw_id, {})[le_val] = cnt
        except (ValueError, KeyError, IndexError, TypeError):
            pass
    return out


def _parse_histogram_scalar_map(items: list[dict]) -> dict[RawSeriesId, tuple[dict[str, str], float]]:
    out: dict[RawSeriesId, tuple[dict[str, str], float]] = {}
    for item in items:
        m = item.get("metric", {})
        try:
            val = float(item["value"][1])
            if math.isnan(val):
                continue
            out[raw_series_identity(m, extra_exclude={"le"})] = (m, val)
        except (ValueError, KeyError, IndexError, TypeError):
            pass
    return out


def compute_histogram_deltas_by_group(
    end_by_name: dict[str, list[dict]],
    start_by_name: dict[str, list[dict]],
    metric_name: str,
    group_key_fn: Callable[[dict[str, str]], str],
) -> tuple[
    dict[str, list[tuple[float, float]]],
    dict[str, float],
    dict[str, float],
    list[tuple[float, float]],
    float,
    float,
]:
    """Compute reset-aware per-group and combined histogram deltas from end/start snapshots.

    Detects counter resets per raw Prometheus series (`raw_series_identity`, preserving replica
    and instance labels): if `_count`, `_sum`, or any bucket decreased (`end < start - 1e-9`)
    for a series (e.g., due to a Kueue controller restart on a reused chamber), that series is
    treated as having reset to 0 so its post-restart observations (`end`) are preserved intact
    instead of clamping buckets to 0 or mixing pre-restart and post-restart sums.
    """
    end_buckets = end_by_name.get(f"{metric_name}_bucket", [])
    if not end_buckets:
        return {}, {}, {}, [], 0.0, 0.0

    start_buckets = start_by_name.get(f"{metric_name}_bucket", [])
    end_sums = end_by_name.get(f"{metric_name}_sum", [])
    start_sums = start_by_name.get(f"{metric_name}_sum", [])
    end_counts = end_by_name.get(f"{metric_name}_count", [])
    start_counts = start_by_name.get(f"{metric_name}_count", [])

    start_b_by_raw = _parse_histogram_bucket_map(start_buckets)
    end_b_by_raw: dict[RawSeriesId, dict[float, float]] = {}
    raw_group: dict[RawSeriesId, str] = {}
    for item in end_buckets:
        m = item.get("metric", {})
        le_str = m.get("le", "")
        try:
            le_val = math.inf if le_str == "+Inf" else float(le_str)
            cnt = float(item["value"][1])
            if math.isnan(cnt):
                continue
            raw_id = raw_series_identity(m, extra_exclude={"le"})
            raw_group.setdefault(raw_id, group_key_fn(m))
            end_b_by_raw.setdefault(raw_id, {})[le_val] = cnt
        except (ValueError, KeyError, IndexError, TypeError):
            pass

    end_sum_by_raw = _parse_histogram_scalar_map(end_sums)
    start_sum_by_raw = _parse_histogram_scalar_map(start_sums)
    end_cnt_by_raw = _parse_histogram_scalar_map(end_counts)
    start_cnt_by_raw = _parse_histogram_scalar_map(start_counts)

    for raw_id, (m, _val) in end_sum_by_raw.items():
        raw_group.setdefault(raw_id, group_key_fn(m))
    for raw_id, (m, _val) in end_cnt_by_raw.items():
        raw_group.setdefault(raw_id, group_key_fn(m))

    group_series_buckets: dict[str, dict[RawSeriesId, dict[float, float]]] = {}
    group_sums: dict[str, float] = {}
    group_cnts: dict[str, float] = {}
    all_sum = 0.0
    all_cnt = 0.0

    all_raw_ids = set(end_b_by_raw.keys()) | set(end_sum_by_raw.keys()) | set(end_cnt_by_raw.keys())
    for raw_id in all_raw_ids:
        gkey = raw_group[raw_id]
        end_b_map = end_b_by_raw.get(raw_id)
        start_b_map = start_b_by_raw.get(raw_id, {})

        # A decrease in any bucket, _count, or _sum for this raw series proves a counter reset.
        is_reset = False
        if raw_id in end_cnt_by_raw and raw_id in start_cnt_by_raw:
            if end_cnt_by_raw[raw_id][1] < start_cnt_by_raw[raw_id][1] - 1e-9:
                is_reset = True
        if not is_reset and raw_id in end_sum_by_raw and raw_id in start_sum_by_raw:
            if end_sum_by_raw[raw_id][1] < start_sum_by_raw[raw_id][1] - 1e-9:
                is_reset = True
        if not is_reset and end_b_map and start_b_map:
            for le_val, end_c in end_b_map.items():
                if le_val in start_b_map and end_c < start_b_map[le_val] - 1e-9:
                    is_reset = True
                    break

        if end_b_map:
            base_b_map = {} if is_reset else start_b_map
            delta_le_map = {
                le_val: max(0.0, end_c - base_b_map.get(le_val, 0.0))
                for le_val, end_c in end_b_map.items()
            }
            group_series_buckets.setdefault(gkey, {})[raw_id] = delta_le_map

        if raw_id in end_sum_by_raw:
            end_s = end_sum_by_raw[raw_id][1]
            start_s = 0.0 if is_reset else start_sum_by_raw.get(raw_id, ({}, 0.0))[1]
            delta_s = max(0.0, end_s - start_s)
            group_sums[gkey] = group_sums.get(gkey, 0.0) + delta_s
            all_sum += delta_s

        if raw_id in end_cnt_by_raw:
            end_c = end_cnt_by_raw[raw_id][1]
            start_c = 0.0 if is_reset else start_cnt_by_raw.get(raw_id, ({}, 0.0))[1]
            delta_c = max(0.0, end_c - start_c)
            group_cnts[gkey] = group_cnts.get(gkey, 0.0) + delta_c
            all_cnt += delta_c

    def _sum_monotonic_series(
        series_maps: list[dict[float, float]],
        sorted_les: list[float],
    ) -> list[tuple[float, float]]:
        totals = {le_val: 0.0 for le_val in sorted_les}
        for le_map in series_maps:
            running_max = 0.0
            for le_val in sorted_les:
                if le_val in le_map:
                    running_max = max(running_max, le_map[le_val])
                totals[le_val] += running_max
        return [(le_val, totals[le_val]) for le_val in sorted_les]

    group_buckets: dict[str, list[tuple[float, float]]] = {}
    all_series_maps: list[dict[float, float]] = []
    all_le_set: set[float] = set()
    for gkey, series_dict in group_series_buckets.items():
        g_maps = list(series_dict.values())
        g_les = sorted({le_val for le_map in g_maps for le_val in le_map})
        if g_les:
            group_buckets[gkey] = _sum_monotonic_series(g_maps, g_les)
            all_series_maps.extend(g_maps)
            all_le_set.update(g_les)

    all_buckets = _sum_monotonic_series(all_series_maps, sorted(all_le_set)) if all_le_set else []
    return group_buckets, group_sums, group_cnts, all_buckets, all_sum, all_cnt


def report_histogram(
    end_by_name: dict[str, list[dict]],
    start_by_name: dict[str, list[dict]],
    spec: MetricSpec,
) -> list[tuple[str, str, str, str, str, str, str]]:
    """Computes P50, P90, MAX, AVG, COUNT, and bucket breakdown for a Histogram metric."""
    group_buckets, agg_sum_by_gkey, agg_cnt_by_gkey, _all_b, _all_s, _all_c = compute_histogram_deltas_by_group(
        end_by_name,
        start_by_name,
        spec.name,
        lambda m: format_labels(m, extra_exclude={"le"}),
    )
    if not group_buckets:
        return []

    rows: list[tuple[str, str, str, str, str, str, str]] = []
    for gkey, b_sorted in sorted(group_buckets.items()):
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


def _put_max_sample(bucket: dict[tuple[str, str], float], key: tuple[str, str], val: float) -> None:
    if key not in bucket or val > bucket[key]:
        bucket[key] = val


def _index_quota_series_by_resource(
    series_list: list[dict],
    start: float,
    primary_label: str,
    *,
    end: float | None = None,
    secondary_from_cq_flavor: bool = False,
    record_cq_cohort: dict[str, str] | None = None,
    overwrite_cq_cohort: bool = True,
) -> tuple[dict[str, dict[float, dict[tuple[str, str], float]]], dict[str, set[str]]]:
    """Index Prometheus range series into resource -> ts -> (entity, flavor_key) -> max_val."""
    res_map: dict[str, dict[float, dict[tuple[str, str], float]]] = {}
    entities_by_res: dict[str, set[str]] = {}

    for s in series_list:
        m = s.get("metric", {})
        res = m.get("resource", "").strip()
        entity = m.get(primary_label, "").strip()
        flv = m.get("flavor", "").strip()
        if not res or not entity:
            continue
        if record_cq_cohort is not None:
            cohort_lbl = m.get("cohort", "").strip()
            if cohort_lbl and (overwrite_cq_cohort or entity not in record_cq_cohort):
                record_cq_cohort[entity] = cohort_lbl
        entities_by_res.setdefault(res, set()).add(entity)
        res_ts_map = res_map.setdefault(res, {})
        sub_key = f"{m.get('cluster_queue', '').strip()}:{flv}" if secondary_from_cq_flavor else flv
        for ts_raw, v_str in s.get("values", []):
            try:
                ts = float(ts_raw)
                val = float(v_str)
                if (
                    ts >= start
                    and (end is None or ts <= end + 1e-9)
                    and not math.isnan(val)
                    and not math.isinf(val)
                ):
                    _put_max_sample(res_ts_map.setdefault(ts, {}), (entity, sub_key), val)
            except ValueError:
                pass

    return res_map, entities_by_res


def _resolve_root_cohort(
    cohort_name: str,
    cohort_parent_map: dict[str, str],
    known_cohorts: set[str],
) -> str:
    """Resolve the highest ancestor cohort present in `known_cohorts` (cycle-safe)."""
    cur = cohort_name
    best = cur if cur in known_cohorts else ""
    visited: set[str] = set()
    while cur and cur not in visited:
        visited.add(cur)
        if cur in known_cohorts:
            best = cur
        cur = cohort_parent_map.get(cur, "")
    return best


def _build_quota_scope_rows(
    quota_map: dict[str, dict[float, dict[tuple[str, str], float]]],
    usage_map: dict[str, dict[float, dict[tuple[str, str], float]]],
    *,
    scope_plural: str,
    per_entity_tag: str,
    per_entity_note: str,
    entity_prefix: str,
    aggregate_totals_fn: Callable[[str, float, dict[str, float], dict[str, float]], tuple[float, float]],
    zero_quota_fallback_fn: Callable[[str, float, str], float] | None = None,
) -> list[list[str]]:
    """Build `(All)`, `(Per-<Entity>, n=K)`, and per-entity utilization rows for CQs or Cohorts."""
    rows: list[list[str]] = []
    for res in sorted(quota_map.keys()):
        is_bytes = res in ("memory", "ephemeral-storage")
        ts_quota = quota_map[res]
        ts_usage = usage_map.get(res, {})
        total_util_samples: list[float] = []
        per_entity_util_samples: list[float] = []
        per_entity_util_by_name: dict[str, list[float]] = {}
        per_entity_fallback_util_by_name: dict[str, list[float]] = {}
        per_entity_used_by_name: dict[str, list[float]] = {}
        per_entity_quota_by_name: dict[str, list[float]] = {}
        all_entities_seen: set[str] = set()
        active_entities_for_ratio: set[str] = set()
        total_used_samples: list[float] = []
        total_quota_samples: list[float] = []

        for ts in sorted(ts_quota.keys()):
            q_entries = ts_quota[ts]
            u_entries = ts_usage.get(ts, {})

            q_by_entity: dict[str, float] = {}
            u_by_entity: dict[str, float] = {}
            for (entity, _flv), q_val in q_entries.items():
                q_by_entity[entity] = q_by_entity.get(entity, 0.0) + q_val
            for (entity, _flv), u_val in u_entries.items():
                u_by_entity[entity] = u_by_entity.get(entity, 0.0) + u_val

            tot_u, tot_q = aggregate_totals_fn(res, ts, q_by_entity, u_by_entity)
            if tot_q > 0:
                total_util_samples.append((tot_u / tot_q) * 100.0)
                total_used_samples.append(tot_u)
                total_quota_samples.append(tot_q)

            for entity in sorted(set(q_by_entity.keys()) | set(u_by_entity.keys())):
                all_entities_seen.add(entity)
                e_q = q_by_entity.get(entity, 0.0)
                e_u = u_by_entity.get(entity, 0.0)
                per_entity_used_by_name.setdefault(entity, []).append(e_u)
                per_entity_quota_by_name.setdefault(entity, []).append(e_q)
                if e_q > 0:
                    active_entities_for_ratio.add(entity)
                    ratio = (e_u / e_q) * 100.0
                    per_entity_util_samples.append(ratio)
                    per_entity_util_by_name.setdefault(entity, []).append(ratio)
                elif zero_quota_fallback_fn is not None:
                    fallback_q = zero_quota_fallback_fn(res, ts, entity)
                    if fallback_q > 0:
                        per_entity_fallback_util_by_name.setdefault(entity, []).append((e_u / fallback_q) * 100.0)

        stats_total = summarize_samples(total_util_samples)
        if stats_total is not None:
            p50, p90, p99, avg = stats_total
            avg_u = sum(total_used_samples) / len(total_used_samples) if total_used_samples else 0.0
            avg_q = sum(total_quota_samples) / len(total_quota_samples) if total_quota_samples else 0.0
            rows.append([
                f"{scope_plural} (All)",
                res,
                format_pct(p50),
                format_pct(p90),
                format_pct(p99),
                format_pct(avg),
                f"{format_num(avg_u, is_bytes=is_bytes)} / {format_num(avg_q, is_bytes=is_bytes)}",
            ])

        if len(active_entities_for_ratio) > 1 and per_entity_util_samples:
            stats_per_entity = summarize_samples(per_entity_util_samples)
            if stats_per_entity is not None:
                p50, p90, p99, avg = stats_per_entity
                n_entities = len(active_entities_for_ratio)
                rows.append([
                    f"{scope_plural} ({per_entity_tag}, n={n_entities})",
                    res,
                    format_pct(p50),
                    format_pct(p90),
                    format_pct(p99),
                    format_pct(avg),
                    per_entity_note,
                ])

        for entity in sorted(all_entities_seen):
            u_list = per_entity_used_by_name.get(entity, [])
            q_list = per_entity_quota_by_name.get(entity, [])
            e_avg_u = sum(u_list) / len(u_list) if u_list else 0.0
            e_avg_q = sum(q_list) / len(q_list) if q_list else 0.0
            e_ratios = per_entity_util_by_name.get(entity, [])
            e_stats = summarize_samples(e_ratios) if e_ratios else None
            if e_stats is not None:
                e_p50, e_p90, e_p99, e_avg = e_stats
                rows.append([
                    f"{entity_prefix}: {entity}",
                    res,
                    format_pct(e_p50),
                    format_pct(e_p90),
                    format_pct(e_p99),
                    format_pct(e_avg),
                    f"{format_num(e_avg_u, is_bytes=is_bytes)} / {format_num(e_avg_q, is_bytes=is_bytes)}",
                ])
            else:
                used_quota_str = (
                    f"{format_num(e_avg_u, is_bytes=is_bytes)} / {format_num(e_avg_q, is_bytes=is_bytes)}"
                    + (" (borrowed)" if e_avg_u > 0 else "")
                )
                fb_ratios = per_entity_fallback_util_by_name.get(entity, [])
                fb_stats = summarize_samples(fb_ratios) if fb_ratios else None
                if fb_stats is not None:
                    fb_p50, fb_p90, fb_p99, fb_avg = fb_stats
                    rows.append([
                        f"{entity_prefix}: {entity}",
                        res,
                        f"{format_pct(fb_p50)} (cohort)",
                        f"{format_pct(fb_p90)} (cohort)",
                        f"{format_pct(fb_p99)} (cohort)",
                        f"{format_pct(fb_avg)} (cohort)",
                        used_quota_str,
                    ])
                else:
                    rows.append([
                        f"{entity_prefix}: {entity}",
                        res,
                        "N/A (0 quota)",
                        "N/A (0 quota)",
                        "N/A (0 quota)",
                        "N/A (0 quota)",
                        used_quota_str,
                    ])
    return rows


def _build_cq_quota_rows(
    cq_quota_map: dict[str, dict[float, dict[tuple[str, str], float]]],
    cq_usage_map: dict[str, dict[float, dict[tuple[str, str], float]]],
    cohort_quota_map: dict[str, dict[float, dict[tuple[str, str], float]]],
    cq_cohort_by_name: dict[str, str],
    all_cohorts_by_res: dict[str, set[str]],
    cohort_parent_map: dict[str, str],
) -> list[list[str]]:
    cohort_q_by_res_ts: dict[str, dict[float, dict[str, float]]] = {}
    for res, ts_map in cohort_quota_map.items():
        res_ts_totals: dict[float, dict[str, float]] = {}
        for ts, entries in ts_map.items():
            totals: dict[str, float] = {}
            for (c_name, _flv), c_val in entries.items():
                totals[c_name] = totals.get(c_name, 0.0) + c_val
            res_ts_totals[ts] = totals
        cohort_q_by_res_ts[res] = res_ts_totals

    def _cq_totals(
        res: str,
        ts: float,
        q_by_cq: dict[str, float],
        u_by_cq: dict[str, float],
    ) -> tuple[float, float]:
        tot_u = sum(u_by_cq.values())
        known_cohorts = all_cohorts_by_res.get(res, set())
        q_by_cohort_ts = cohort_q_by_res_ts.get(res, {}).get(ts, {})
        standalone_q = 0.0
        cq_q_by_root: dict[str, float] = {}
        covered_roots: set[str] = set()
        for cq in set(q_by_cq.keys()) | set(u_by_cq.keys()):
            c_name = cq_cohort_by_name.get(cq, "")
            root_c = _resolve_root_cohort(c_name, cohort_parent_map, known_cohorts) if c_name else ""
            if root_c:
                covered_roots.add(root_c)
                cq_q_by_root[root_c] = cq_q_by_root.get(root_c, 0.0) + q_by_cq.get(cq, 0.0)
            else:
                standalone_q += q_by_cq.get(cq, 0.0)
        tot_q = standalone_q + sum(
            max(q_by_cohort_ts.get(r, 0.0), cq_q_by_root.get(r, 0.0))
            for r in covered_roots
        )
        return tot_u, tot_q

    def _cq_zero_quota_fallback(res: str, ts: float, cq: str) -> float:
        parent_cohort = cq_cohort_by_name.get(cq, "")
        if not parent_cohort:
            return 0.0
        q_by_cohort_ts = cohort_q_by_res_ts.get(res, {}).get(ts, {})
        direct_q = q_by_cohort_ts.get(parent_cohort, 0.0)
        if direct_q > 0:
            return direct_q
        root_c = _resolve_root_cohort(parent_cohort, cohort_parent_map, all_cohorts_by_res.get(res, set()))
        return q_by_cohort_ts.get(root_c, 0.0) if root_c else 0.0

    return _build_quota_scope_rows(
        cq_quota_map,
        cq_usage_map,
        scope_plural="ClusterQueue Quotas",
        per_entity_tag="Per-CQ",
        per_entity_note="per-queue ratio",
        entity_prefix="ClusterQueue",
        aggregate_totals_fn=_cq_totals,
        zero_quota_fallback_fn=_cq_zero_quota_fallback,
    )


def _build_cohort_quota_rows(
    cohort_quota_map: dict[str, dict[float, dict[tuple[str, str], float]]],
    cohort_usage_map: dict[str, dict[float, dict[tuple[str, str], float]]],
    all_cohorts_by_res: dict[str, set[str]],
    cohort_parent_map: dict[str, str],
) -> list[list[str]]:
    root_cohorts_by_res: dict[str, set[str]] = {}
    for res in cohort_quota_map:
        cohorts_for_res = all_cohorts_by_res.get(res, set())
        roots = {
            c for c in cohorts_for_res
            if not cohort_parent_map.get(c) or cohort_parent_map.get(c) not in cohorts_for_res
        }
        root_cohorts_by_res[res] = roots if roots else set(cohorts_for_res)

    def _cohort_totals(
        res: str,
        _ts: float,
        q_by_cohort: dict[str, float],
        u_by_cohort: dict[str, float],
    ) -> tuple[float, float]:
        # Sum across root cohorts to avoid double-counting child subtrees.
        roots = root_cohorts_by_res.get(res, set())
        tot_q = sum(v for c, v in q_by_cohort.items() if c in roots)
        tot_u = sum(v for c, v in u_by_cohort.items() if c in roots)
        return tot_u, tot_q

    return _build_quota_scope_rows(
        cohort_quota_map,
        cohort_usage_map,
        scope_plural="Cohort Quotas",
        per_entity_tag="Per-Cohort",
        per_entity_note="per-cohort ratio",
        entity_prefix="Cohort",
        aggregate_totals_fn=_cohort_totals,
    )


def report_aggregated_quota_utilization(
    range_by_name: dict[str, list[dict]],
    start: float,
    end: float | None = None,
) -> list[list[str]]:
    """Computes P50, P90, P99, and AVG utilization (%) of ClusterQueue and Cohort quotas per resource type."""
    cq_quota_series = range_by_name.get("kueue_cluster_queue_nominal_quota", [])
    cq_usage_series = range_by_name.get("kueue_cluster_queue_resource_usage", [])
    if not cq_usage_series:
        cq_usage_series = range_by_name.get("kueue_cluster_queue_resource_reservation", [])

    cohort_quota_series = range_by_name.get("kueue_cohort_subtree_quota", [])
    cohort_usage_series = range_by_name.get("kueue_cohort_subtree_resource_reservations", [])

    def _index_all(upper_end: float | None):
        cohort_by_name: dict[str, str] = {}
        cqq, _ = _index_quota_series_by_resource(
            cq_quota_series,
            start,
            "cluster_queue",
            end=upper_end,
            record_cq_cohort=cohort_by_name,
            overwrite_cq_cohort=True,
        )
        cqu, _ = _index_quota_series_by_resource(
            cq_usage_series,
            start,
            "cluster_queue",
            end=upper_end,
            record_cq_cohort=cohort_by_name,
            overwrite_cq_cohort=False,
        )
        if cohort_quota_series:
            cohq, cohs_q = _index_quota_series_by_resource(cohort_quota_series, start, "cohort", end=upper_end)
            cohu, cohs_u = _index_quota_series_by_resource(cohort_usage_series, start, "cohort", end=upper_end)
        else:
            cohq, cohs_q = _index_quota_series_by_resource(
                cq_quota_series, start, "cohort", end=upper_end, secondary_from_cq_flavor=True
            )
            cohu, cohs_u = _index_quota_series_by_resource(
                cq_usage_series, start, "cohort", end=upper_end, secondary_from_cq_flavor=True
            )
        return cohort_by_name, cqq, cqu, cohq, cohu, cohs_q, cohs_u

    cq_cohort_by_name, cq_quota_map, cq_usage_map, cohort_quota_map, cohort_usage_map, all_cohorts_q, all_cohorts_u = _index_all(end)
    if end is not None and not any(cq_quota_map.values()) and not any(cohort_quota_map.values()):
        cq_cohort_by_name, cq_quota_map, cq_usage_map, cohort_quota_map, cohort_usage_map, all_cohorts_q, all_cohorts_u = _index_all(None)

    # Determine cohort hierarchy from kueue_cohort_info so hierarchical parent/child
    # cohorts are not double-counted when aggregating total cohort quota at timestamp t.
    cohort_parent_map: dict[str, str] = {}
    for s in range_by_name.get("kueue_cohort_info", []):
        m = s.get("metric", {})
        c_name = m.get("cohort", "").strip()
        p_name = m.get("parent_cohort", "").strip()
        if c_name:
            cohort_parent_map[c_name] = p_name

    all_cohorts_by_res: dict[str, set[str]] = {}
    for res, names in all_cohorts_q.items():
        all_cohorts_by_res.setdefault(res, set()).update(names)
    for res, names in all_cohorts_u.items():
        all_cohorts_by_res.setdefault(res, set()).update(names)

    rows = _build_cq_quota_rows(
        cq_quota_map,
        cq_usage_map,
        cohort_quota_map,
        cq_cohort_by_name,
        all_cohorts_by_res,
        cohort_parent_map,
    )
    rows.extend(_build_cohort_quota_rows(cohort_quota_map, cohort_usage_map, all_cohorts_by_res, cohort_parent_map))
    return rows


def report_aggregated_wait_time_by_priority(
    end_by_name: dict[str, list[dict]],
    start_by_name: dict[str, list[dict]],
    metric_name: str = "kueue_admission_wait_time_seconds",
) -> list[list[str]]:
    """Computes P50, P90, P99, and AVG wait time aggregated across all ClusterQueues per workload priority."""
    if not end_by_name.get(f"{metric_name}_bucket") and metric_name == "kueue_admission_wait_time_seconds":
        # Fallback to quota_reserved_wait_time_seconds if admission_wait_time_seconds was not populated
        alt = "kueue_quota_reserved_wait_time_seconds"
        if end_by_name.get(f"{alt}_bucket"):
            return report_aggregated_wait_time_by_priority(end_by_name, start_by_name, metric_name=alt)
        return []

    prio_buckets, prio_sums, prio_cnts, all_buckets, all_sum, all_cnt = compute_histogram_deltas_by_group(
        end_by_name,
        start_by_name,
        metric_name,
        lambda m: m.get("priority_class", "").strip() or "<none>",
    )
    if not prio_buckets:
        return []

    rows: list[list[str]] = []
    for prio in sorted(prio_buckets.keys(), key=priority_sort_key):
        b_sorted = prio_buckets[prio]
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
        total_cnt = all_cnt if all_cnt > 0 else all_buckets[-1][1]
        if total_cnt > 0:
            p50 = histogram_quantile_from_buckets(0.50, all_buckets)
            p90 = histogram_quantile_from_buckets(0.90, all_buckets)
            p99 = histogram_quantile_from_buckets(0.99, all_buckets)
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
        if reason in _RECLAIM_REASONS or cause in _RECLAIM_REASONS:
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
    configurable_preemptions = 0.0
    other_preemptions: dict[str, float] = {}
    preemptions_by_reason: dict[str, float] = {}

    for s in preempted_series:
        inc = counter_series_total_increase(s, start)
        if inc <= 0:
            continue
        total_preemptions += inc
        m = s.get("metric", {})
        reason = m.get("reason", "").strip() or "<none>"
        preemptions_by_reason[reason] = preemptions_by_reason.get(reason, 0.0) + inc
        if reason in _RECLAIM_REASONS:
            reclaims_from_preempted += inc
        elif reason == "InClusterQueue":
            in_cq_preemptions += inc
        elif reason in _FAIR_SHARING_REASONS:
            fair_sharing_preemptions += inc
        elif reason == "ConfigurablePreemption":
            configurable_preemptions += inc
        else:
            other_preemptions[reason] = other_preemptions.get(reason, 0.0) + inc

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

    preemption_detail_parts = [
        f"InClusterQueue={int(round(in_cq_preemptions))}",
        f"Reclaims={int(round(reclaims_from_preempted))}",
        f"FairSharing={int(round(fair_sharing_preemptions))}",
    ]
    if configurable_preemptions > 0:
        preemption_detail_parts.append(f"ConfigurablePreemption={int(round(configurable_preemptions))}")
    for reason, cnt in sorted(other_preemptions.items()):
        preemption_detail_parts.append(f"{reason}={int(round(cnt))}")

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
            ", ".join(preemption_detail_parts),
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
