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

"""Prometheus HTTP/port-forward client and monotonic counter / histogram quantile primitives."""

from __future__ import annotations

import atexit
from bisect import bisect_right
import json
import math
import socket
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request


DEFAULT_MAX_QUERY_RANGE_POINTS = 10_000


def compute_effective_step(
    range_start: float,
    query_end: float,
    requested_step: float = 1.0,
    *,
    max_points: int = DEFAULT_MAX_QUERY_RANGE_POINTS,
) -> float:
    """Return a query_range step >= requested_step keeping point count <= max_points.

    Prometheus returns ``floor((end - start) / step) + 1`` points (inclusive endpoints) and
    rejects ``/api/v1/query_range`` requests exceeding 11,000 points per series with HTTP 400.
    Keeping the effective step within 10,000 points leaves headroom while keeping ``step=1.0s``
    unchanged for runs up to ~2.7 hours.
    """
    base_step = requested_step if requested_step > 0 else 1.0
    span = max(0.0, query_end - range_start)
    if max_points <= 0 or span <= 0.0:
        return base_step
    max_intervals = max(1, max_points - 1)
    if int(math.floor(span / base_step + 1e-9)) + 1 <= max_points:
        return base_step
    return max(base_step, float(math.ceil(span / float(max_intervals))))


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

        # Automatically start a local port-forward to svc/prometheus in self.namespace so queries
        # execute directly over HTTP in milliseconds without spawning per-metric kubectl
        # subprocesses (never probing shared ports like 19090/9090 that may belong to another chamber).
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
                if self._pf_proc.poll() is None:
                    self.prometheus_url = candidate_url
                    return
                break
            time.sleep(0.2)

        # Port-forward did not become ready; terminate and clear prometheus_url so _get_json
        # falls back to the strictly namespaced kubectl proxy for self.namespace.
        self.close()
        self.prometheus_url = None

    def close(self) -> None:
        if self._pf_proc is not None:
            if self._pf_proc.poll() is None:
                try:
                    self._pf_proc.terminate()
                    self._pf_proc.wait(timeout=2)
                except Exception:
                    try:
                        self._pf_proc.kill()
                        self._pf_proc.wait(timeout=2)
                    except Exception:
                        pass
            else:
                try:
                    self._pf_proc.wait(timeout=1)
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


def build_monotonic_counter_points(values: list) -> list[tuple[float, float]]:
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
    pts = build_monotonic_counter_points(series.get("values", []))
    if not pts:
        return 0.0
    ts_list = [ts for ts, _ in pts]
    val_list = [val for _, val in pts]
    start_idx = bisect_right(ts_list, start - 1e-9) - 1
    baseline_at_start = val_list[start_idx] if start_idx >= 0 else 0.0
    return max(0.0, val_list[-1] - baseline_at_start)


def histogram_quantile_from_buckets(q: float, buckets: list[tuple[float, float]]) -> float:
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
