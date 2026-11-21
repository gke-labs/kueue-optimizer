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
import math

from chamber.parsers import format_num, format_pct, summarize_samples
from metrics.catalog import MetricSpec, format_labels, priority_sort_key, raw_series_identity
from metrics.prometheus import (
    build_monotonic_counter_points,
    counter_series_total_increase,
    histogram_quantile_from_buckets,
)


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
            start_b_map[(raw_series_identity(m, extra_exclude={"le"}), le_val)] = cnt
        except (ValueError, KeyError, IndexError):
            pass

    grouped_bucket_sums: dict[str, dict[float, float]] = {}
    for item in end_buckets:
        m = item.get("metric", {})
        le_str = m.get("le", "")
        try:
            le_val = math.inf if le_str == "+Inf" else float(le_str)
            cnt_end = float(item["value"][1])
            raw_id = raw_series_identity(m, extra_exclude={"le"})
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
                out[raw_series_identity(m)] = (group_key(m), float(item["value"][1]))
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


def _put_max_sample(bucket: dict[tuple[str, str], float], key: tuple[str, str], val: float) -> None:
    if key not in bucket or val > bucket[key]:
        bucket[key] = val


def _index_quota_series_by_resource(
    series_list: list[dict],
    start: float,
    primary_label: str,
    *,
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
                if ts >= start and not math.isnan(val) and not math.isinf(val):
                    _put_max_sample(res_ts_map.setdefault(ts, {}), (entity, sub_key), val)
            except ValueError:
                pass

    return res_map, entities_by_res


def _build_cq_quota_rows(
    cq_quota_map: dict[str, dict[float, dict[tuple[str, str], float]]],
    cq_usage_map: dict[str, dict[float, dict[tuple[str, str], float]]],
    cohort_quota_map: dict[str, dict[float, dict[tuple[str, str], float]]],
    cq_cohort_by_name: dict[str, str],
) -> list[list[str]]:
    rows: list[list[str]] = []
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
    return rows


def _build_cohort_quota_rows(
    cohort_quota_map: dict[str, dict[float, dict[tuple[str, str], float]]],
    cohort_usage_map: dict[str, dict[float, dict[tuple[str, str], float]]],
    all_cohorts_by_res: dict[str, set[str]],
    cohort_parent_map: dict[str, str],
) -> list[list[str]]:
    rows: list[list[str]] = []
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
        total_util_samples: list[float] = []
        per_cohort_util_samples: list[float] = []
        per_cohort_util_by_name: dict[str, list[float]] = {}
        per_cohort_used_by_name: dict[str, list[float]] = {}
        per_cohort_quota_by_name: dict[str, list[float]] = {}
        all_cohorts_seen_for_res: set[str] = set()
        active_cohorts_for_ratio: set[str] = set()
        total_used_samples: list[float] = []
        total_quota_samples: list[float] = []

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


def report_aggregated_quota_utilization(
    range_by_name: dict[str, list[dict]],
    start: float,
) -> list[list[str]]:
    """Computes P50, P90, P99, and AVG utilization (%) of ClusterQueue and Cohort quotas per resource type."""
    cq_quota_series = range_by_name.get("kueue_cluster_queue_nominal_quota", [])
    cq_usage_series = range_by_name.get("kueue_cluster_queue_resource_usage", [])
    if not cq_usage_series:
        cq_usage_series = range_by_name.get("kueue_cluster_queue_resource_reservation", [])

    cq_cohort_by_name: dict[str, str] = {}
    cq_quota_map, _ = _index_quota_series_by_resource(
        cq_quota_series, start, "cluster_queue", record_cq_cohort=cq_cohort_by_name, overwrite_cq_cohort=True
    )
    cq_usage_map, _ = _index_quota_series_by_resource(
        cq_usage_series, start, "cluster_queue", record_cq_cohort=cq_cohort_by_name, overwrite_cq_cohort=False
    )

    # Determine cohort hierarchy from kueue_cohort_info so hierarchical parent/child
    # cohorts are not double-counted when aggregating total cohort quota at timestamp t.
    cohort_parent_map: dict[str, str] = {}
    for s in range_by_name.get("kueue_cohort_info", []):
        m = s.get("metric", {})
        c_name = m.get("cohort", "").strip()
        p_name = m.get("parent_cohort", "").strip()
        if c_name:
            cohort_parent_map[c_name] = p_name

    cohort_quota_series = range_by_name.get("kueue_cohort_subtree_quota", [])
    cohort_usage_series = range_by_name.get("kueue_cohort_subtree_resource_reservations", [])
    if cohort_quota_series:
        cohort_quota_map, all_cohorts_q = _index_quota_series_by_resource(cohort_quota_series, start, "cohort")
        cohort_usage_map, all_cohorts_u = _index_quota_series_by_resource(cohort_usage_series, start, "cohort")
    else:
        cohort_quota_map, all_cohorts_q = _index_quota_series_by_resource(
            cq_quota_series, start, "cohort", secondary_from_cq_flavor=True
        )
        cohort_usage_map, all_cohorts_u = _index_quota_series_by_resource(
            cq_usage_series, start, "cohort", secondary_from_cq_flavor=True
        )

    all_cohorts_by_res: dict[str, set[str]] = {}
    for res, names in all_cohorts_q.items():
        all_cohorts_by_res.setdefault(res, set()).update(names)
    for res, names in all_cohorts_u.items():
        all_cohorts_by_res.setdefault(res, set()).update(names)

    rows = _build_cq_quota_rows(cq_quota_map, cq_usage_map, cohort_quota_map, cq_cohort_by_name)
    rows.extend(_build_cohort_quota_rows(cohort_quota_map, cohort_usage_map, all_cohorts_by_res, cohort_parent_map))
    return rows


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
            start_b_map[(raw_series_identity(m, extra_exclude={"le"}), le_val)] = cnt
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
            raw_id = raw_series_identity(m, extra_exclude={"le"})
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
                out[raw_series_identity(m)] = (prio, float(item["value"][1]))
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
    for prio in sorted(prio_buckets.keys(), key=priority_sort_key):
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
