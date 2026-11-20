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

"""ASCII histogram renderers for scenario workload durations and arrival timelines."""

from __future__ import annotations

import math

from chamber.parsers import calc_summary_stats, format_duration_human


DURATION_BINS: list[tuple[str, float, float]] = [
    ("< 1m   (0-60s)", 0.0, 60.0),
    ("1-5m   (60-300s)", 60.0, 300.0),
    ("5-10m  (300-600s)", 300.0, 600.0),
    ("10-15m (600-900s)", 600.0, 900.0),
    ("15-20m (900-1200s)", 900.0, 1200.0),
    ("20-30m (1200-1800s)", 1200.0, 1800.0),
    ("30-45m (1800-2700s)", 1800.0, 2700.0),
    ("45-60m (2700-3600s)", 2700.0, 3600.0),
    ("1-2h   (3600-7200s)", 3600.0, 7200.0),
    (">= 2h  (7200s+)", 7200.0, math.inf),
]


def render_duration_histogram(
    title: str,
    durations: list[float],
    bins: list[tuple[str, float, float]] = DURATION_BINS,
    max_bar_width: int = 32,
) -> str:
    """Render a self-contained ASCII bar histogram of execution durations with summary quantiles."""
    if not durations:
        return f"=== {title} (count=0) ===\n  (no workloads)"

    st = calc_summary_stats(durations)
    n = int(st["count"])
    bin_counts = [0] * len(bins)
    for d in durations:
        placed = False
        for i, (_, lo, hi) in enumerate(bins):
            if lo <= d < hi or (i == len(bins) - 1 and d >= lo):
                bin_counts[i] += 1
                placed = True
                break
        if not placed:
            bin_counts[0] += 1

    peak = max(bin_counts) if bin_counts else 1
    lines: list[str] = [
        (
            f"=== {title} | count={n:,} | min={st['min']:.0f}s, "
            f"p50={st['p50']:.0f}s ({st['p50']/60.0:.1f}m), "
            f"avg={st['avg']:.1f}s ({st['avg']/60.0:.1f}m), "
            f"p90={st['p90']:.0f}s ({st['p90']/60.0:.1f}m), "
            f"p99={st['p99']:.0f}s ({st['p99']/60.0:.1f}m), "
            f"max={st['max']:.0f}s ==="
        )
    ]

    # Trim trailing empty bins after the highest populated bin (keep at least first 6 bins)
    last_non_zero = max((i for i, c in enumerate(bin_counts) if c > 0), default=0)
    visible_end = max(min(len(bins), 6), last_non_zero + 1)

    for i in range(visible_end):
        label, _, _ = bins[i]
        cnt = bin_counts[i]
        pct = (100.0 * cnt / n) if n > 0 else 0.0
        bar_len = int(round((cnt / peak) * max_bar_width)) if (peak > 0 and cnt > 0) else 0
        if cnt > 0 and bar_len == 0:
            bar_len = 1
        bar = "█" * bar_len
        lines.append(f"  {label:<20} | {cnt:>5,} ({pct:>5.1f}%) | {bar}")

    return "\n".join(lines)


def render_timeline_arrival_histogram(
    arrivals_offset_sec: list[float],
    test_duration_sec: float,
    num_buckets: int = 12,
    max_bar_width: int = 32,
) -> str:
    """Render an ASCII arrival histogram showing how unadmitted workload arrivals distribute over time."""
    if not arrivals_offset_sec:
        return "  (no unadmitted workload arrivals)"
    if test_duration_sec <= 0:
        return f"  All {len(arrivals_offset_sec):,} unadmitted workloads arrive simultaneously at t = 0.0s."

    bucket_width = test_duration_sec / num_buckets
    counts = [0] * num_buckets
    for off in arrivals_offset_sec:
        b = int(off / bucket_width) if bucket_width > 0 else 0
        b = max(0, min(num_buckets - 1, b))
        counts[b] += 1

    n = len(arrivals_offset_sec)
    peak = max(counts) if counts else 1
    lines = [
        f"=== Unadmitted Workload Arrival Distribution Across {num_buckets} Equal Time Windows "
        f"(total={n:,}, span={format_duration_human(test_duration_sec)}) ==="
    ]
    for b in range(num_buckets):
        lo = b * bucket_width
        hi = (b + 1) * bucket_width
        cnt = counts[b]
        pct = 100.0 * cnt / n
        bar_len = int(round((cnt / peak) * max_bar_width)) if (peak > 0 and cnt > 0) else 0
        if cnt > 0 and bar_len == 0:
            bar_len = 1
        bar = "█" * bar_len
        if test_duration_sec >= 3600:
            lbl = f"{lo/3600.0:>5.2f}h - {hi/3600.0:>5.2f}h"
        else:
            lbl = f"{lo:>6.1f}s - {hi:>6.1f}s"
        lines.append(f"  [{lbl}] | {cnt:>5,} ({pct:>5.1f}%) | {bar}")
    return "\n".join(lines)
