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

"""Environment, duration, RFC3339, Kubernetes quantity, image ref, port, and stats helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING
import math
import os
from pathlib import Path
import re
import subprocess
from typing import Any

from chamber.constants import (
    BINARY_SI_FACTORS,
    DECIMAL_SI_FACTORS,
    DEFAULT_APISERVER_PORT,
    DEFAULT_PROMETHEUS_BASE_PORT,
    DEFAULT_REGISTRY_BASE_PORT,
    NODEPORT_MAX,
    NODEPORT_MIN,
    ROOT_DIR,
)


def env_str(name: str, default: str) -> str:
    val = os.environ.get(name)
    return val if val is not None and val != "" else default


def env_int(name: str, default: int) -> int:
    val = os.environ.get(name)
    if val is None or val.strip() == "":
        return default
    return int(val.strip())


def env_bool(name: str, default: bool = False) -> bool:
    val = os.environ.get(name)
    if val is None or val.strip() == "":
        return default
    return val.strip().lower() in ("1", "true", "yes", "y", "on")


_DURATION_UNIT_SECONDS: dict[str, float] = {
    "ns": 1e-9,
    "us": 1e-6,
    "µs": 1e-6,
    "μs": 1e-6,
    "ms": 1e-3,
    "s": 1.0,
    "sec": 1.0,
    "secs": 1.0,
    "m": 60.0,
    "min": 60.0,
    "mins": 60.0,
    "h": 3600.0,
    "hr": 3600.0,
    "hrs": 3600.0,
    "d": 86400.0,
    "day": 86400.0,
    "days": 86400.0,
}

_DURATION_BARE_RE = re.compile(r"^\+?(?:\d+(?:\.\d*)?|\.\d+)$")
_DURATION_TOKEN_RE = re.compile(
    r"(\d+(?:\.\d*)?|\.\d+)\s*(secs|sec|mins|min|hrs|hr|days|day|ms|ns|us|µs|μs|s|m|h|d)"
)
_DURATION_COMPOUND_RE = re.compile(
    r"^\+?(?:(?:\d+(?:\.\d*)?|\.\d+)\s*(?:secs|sec|mins|min|hrs|hr|days|day|ms|ns|us|µs|μs|s|m|h|d)\s*)+$"
)


def parse_duration_seconds(raw: str | float | int | None) -> float | None:
    """Parse a duration string (e.g. '300', '45s', '1m30s', '1h2m3.5s', '500ms', '2d12h') into seconds."""
    if raw is None:
        return None
    if isinstance(raw, bool):
        raise ValueError(
            f"Invalid duration '{raw}'. Expected non-negative seconds or duration string (e.g. 30s, 1m30s, 1h)."
        )
    if isinstance(raw, (int, float)):
        val = float(raw)
        if math.isnan(val) or math.isinf(val) or val < 0.0:
            raise ValueError(
                f"Invalid duration '{raw}'. Expected finite non-negative seconds (e.g. 120) or duration string."
            )
        return val
    s = str(raw).strip().lower()
    if not s:
        return None
    if _DURATION_BARE_RE.fullmatch(s):
        val = float(s)
        if not math.isfinite(val):
            raise ValueError(
                f"Invalid duration '{raw}'. Expected finite non-negative seconds (e.g. 120) or duration string."
            )
        return val
    if not _DURATION_COMPOUND_RE.fullmatch(s):
        raise ValueError(
            f"Invalid duration '{raw}'. Expected seconds (e.g. 120) or unit-suffixed duration (e.g. 30s, 1m30s, 1h)."
        )
    total = 0.0
    for num_str, unit in _DURATION_TOKEN_RE.findall(s):
        total += float(num_str) * _DURATION_UNIT_SECONDS[unit]
    if not math.isfinite(total):
        raise ValueError(
            f"Invalid duration '{raw}'. Expected finite non-negative seconds (e.g. 120) or duration string."
        )
    return total


def normalize_scrape_interval(raw: str) -> str:
    s = str(raw).strip()
    if re.fullmatch(r"\d+", s):
        return f"{s}s"
    return s


def normalize_queueing_strategy(raw: str) -> str:
    lower = raw.strip().lower()
    if lower in ("strictfifo", "strict-fifo", "strict_fifo", "strict", "fifo"):
        return "StrictFIFO"
    if lower in (
        "besteffortfifo",
        "best-effort-fifo",
        "best_effort_fifo",
        "best-effort",
        "best_effort",
        "besteffort",
    ):
        return "BestEffortFIFO"
    raise ValueError(
        f"Invalid queueing mode '{raw}'. Supported values: StrictFIFO (strict-fifo) or BestEffortFIFO (best-effort)."
    )


def format_duration_hms(seconds: float) -> str:
    if seconds < 0:
        seconds = 0.0
    total_sec = int(round(seconds))
    hrs, rem = divmod(total_sec, 3600)
    mins, secs = divmod(rem, 60)
    if hrs > 0:
        return f"{hrs:02d}:{mins:02d}:{secs:02d}"
    return f"{mins:02d}:{secs:02d}"


def format_duration_human(sec: float) -> str:
    if sec < 0:
        sec = 0.0
    if sec == 0:
        return "0s"
    if sec < 60:
        return f"{sec:.1f}s"
    if sec < 3600:
        mins = sec / 60.0
        return f"{sec:.1f}s ({mins:.1f}m)"
    hrs = sec / 3600.0
    return f"{sec:,.1f}s ({hrs:.2f}h / {format_duration_hms(sec)})"


def format_duration_short(sec: float) -> str:
    if sec < 0:
        sec = 0.0
    if sec < 60:
        return f"{sec:.1f}s" if sec < 10 else f"{sec:.0f}s"
    if sec < 3600:
        return f"{sec:.0f}s ({sec / 60.0:.1f}m)"
    return f"{sec:.0f}s ({sec / 3600.0:.2f}h)"


def parse_rfc3339_epoch(ts_str: str | None, *, strict: bool = False) -> float | None:
    """Parse an RFC3339 / RFC3339Nano UTC timestamp string into Unix epoch seconds."""
    if not ts_str:
        return None
    s = str(ts_str).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    m = re.match(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\.(\d+)([+-]\d{2}:\d{2})$", s)
    if m:
        frac = (m.group(2) + "000000")[:6]
        s = f"{m.group(1)}.{frac}{m.group(3)}"
    try:
        return datetime.fromisoformat(s).timestamp()
    except ValueError:
        if strict:
            raise
        return None


def format_epoch_iso(epoch: float | None, *, microsecond_precision: bool = False) -> str:
    if epoch is None:
        return "n/a"
    dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
    if microsecond_precision:
        return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    if dt.microsecond:
        return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_positive_duration_annotation(anns: dict[str, Any] | None, *keys: str) -> float | None:
    """Parse the first present positive duration annotation (numeric seconds or unit-suffixed string)."""
    if not isinstance(anns, dict):
        return None
    for key in keys:
        raw = anns.get(key)
        if raw is None:
            continue
        try:
            val = parse_duration_seconds(raw)
        except (ValueError, TypeError):
            continue
        if val is not None and not math.isnan(val) and not math.isinf(val) and val > 0.0:
            return val
    return None


def format_pod_execution_seconds(seconds: float, min_seconds: float = 0.1) -> str:
    """Format pod execution time in plain numeric seconds for kueue-optimizer.gke.io/pod-execution-time-seconds."""
    clamped = max(min_seconds, float(seconds))
    if float(clamped).is_integer():
        return str(int(clamped))
    formatted = f"{clamped:.3f}".rstrip("0").rstrip(".")
    return formatted or "0.1"


# Backward-compatible alias used across runner scripts
format_kwok_pod_duration = format_pod_execution_seconds


def parse_k8s_quantity(raw: Any, resource_name: str = "") -> float:
    """Parse a Kubernetes resource.Quantity into standard base units:
    - memory / ephemeral-storage -> GiB (1024^3 bytes)
    - cpu -> cores (1.0 = 1 vCPU)
    - all other resources (pods, GPUs, TPUs, hugepages) -> scalar base units
    """
    if raw is None:
        return 0.0
    if isinstance(raw, (int, float)):
        val = float(raw)
        if resource_name in ("memory", "ephemeral-storage"):
            return val / (1024.0**3)
        return val
    s = str(raw).strip().strip("\"'").strip()
    if not s:
        return 0.0

    factor = 1.0
    num_str = s
    for suf, mult in BINARY_SI_FACTORS.items():
        if s.endswith(suf):
            num_str = s[: -len(suf)]
            factor = mult
            break
    else:
        for suf, mult in DECIMAL_SI_FACTORS.items():
            if s.endswith(suf):
                num_str = s[: -len(suf)]
                factor = mult
                break

    try:
        base_val = float(num_str) * factor
    except ValueError:
        return 0.0

    if resource_name in ("memory", "ephemeral-storage"):
        return base_val / (1024.0**3)
    return base_val


def multiply_k8s_quantity_exact(raw: Any, count: int, resource_name: str = "") -> str:
    """Multiply a per-pod Kubernetes resource.Quantity by integer `count` with exact integer divisibility.

    Guarantees Kueue's `WorkloadWebhook` invariant (`AmountFromQuantity(k, v).RemInt64(count) == 0`),
    where `cpu` is accounted in integer milliCPU and all other resources in integer whole units/bytes.
    """
    if count <= 0:
        return "0"
    s = str(raw if raw is not None else "0").strip().strip("\"'").strip()
    if not s:
        return "0"

    is_binary = False
    mult = Decimal(1)
    num_str = s
    for suf, factor in BINARY_SI_FACTORS.items():
        if s.endswith(suf):
            num_str = s[: -len(suf)]
            mult = Decimal(int(factor))
            is_binary = True
            break
    else:
        dec_exponents = {
            "n": Decimal("1e-9"),
            "u": Decimal("1e-6"),
            "m": Decimal("1e-3"),
            "k": Decimal("1e3"),
            "M": Decimal("1e6"),
            "G": Decimal("1e9"),
            "T": Decimal("1e12"),
            "P": Decimal("1e15"),
            "E": Decimal("1e18"),
        }
        for suf, dec_mult in dec_exponents.items():
            if s.endswith(suf):
                num_str = s[: -len(suf)]
                mult = dec_mult
                break

    try:
        dec_val = Decimal(num_str) * mult
    except InvalidOperation:
        dec_val = Decimal(str(parse_k8s_quantity(raw, resource_name)))
        if resource_name in ("memory", "ephemeral-storage"):
            dec_val *= Decimal(1024**3)
            is_binary = True

    if resource_name == "cpu":
        per_pod_mcpu = int((dec_val * Decimal(1000)).to_integral_value(rounding=ROUND_CEILING))
        tot_mcpu = per_pod_mcpu * int(count)
        if tot_mcpu % 1000 == 0:
            return str(tot_mcpu // 1000)
        return f"{tot_mcpu}m"

    per_pod_units = int(dec_val.to_integral_value(rounding=ROUND_CEILING))
    tot_units = per_pod_units * int(count)
    if is_binary or resource_name in ("memory", "ephemeral-storage") or resource_name.startswith("hugepages-"):
        for suf, unit_bytes in (("Ti", 1024**4), ("Gi", 1024**3), ("Mi", 1024**2), ("Ki", 1024)):
            if tot_units > 0 and tot_units % unit_bytes == 0:
                return f"{tot_units // unit_bytes}{suf}"
    return str(tot_units)


def format_resource_quantity(val: float, resource_name: str = "", *, show_tib_paren: bool = False) -> str:
    """Format a parsed resource value (cores, GiB, or count) into a readable string."""
    if resource_name in ("memory", "ephemeral-storage"):
        if abs(val) >= 1024.0 * 10.0:
            if show_tib_paren and abs(val / 1024.0 - round(val / 1024.0, 1)) < 0.05:
                return f"{val:,.0f} GiB ({val / 1024.0:,.1f} TiB)"
            if not show_tib_paren:
                return f"{val / 1024.0:,.1f} TiB"
        if abs(val - round(val)) < 1e-2:
            return f"{int(round(val)):,} GiB"
        return f"{val:,.2f} GiB"
    if resource_name == "cpu":
        if abs(val - round(val)) < 1e-2:
            return f"{int(round(val)):,} cores"
        return f"{val:,.2f} cores"
    if abs(val - round(val)) < 1e-2:
        return f"{int(round(val)):,}"
    return f"{val:,.2f}"


def format_compact_si(val: float) -> str:
    abs_v = abs(val)
    if abs_v >= 1e9:
        return f"{val / 1e9:.2f}B"
    if abs_v >= 1e6:
        return f"{val / 1e6:.2f}M"
    if abs_v >= 1e4:
        return f"{val / 1e3:.2f}k"
    return f"{val:.1f}"


def format_num(val: float, unit: str = "", *, is_bytes: bool = False) -> str:
    if math.isnan(val):
        return "n/a"
    if math.isinf(val):
        return "+Inf" if val > 0 else "-Inf"
    abs_v = abs(val)
    if is_bytes and unit == "":
        if abs_v == 0:
            return "0.00B"
        if abs_v >= 1024.0**4:
            return f"{val / (1024.0**4):.2f}Ti"
        if abs_v >= 1024.0**3:
            return f"{val / (1024.0**3):.2f}Gi"
        if abs_v >= 1024.0**2:
            return f"{val / (1024.0**2):.2f}Mi"
        if abs_v >= 1024.0:
            return f"{val / 1024.0:.2f}Ki"
        return f"{val:.0f}B"
    if abs_v == 0:
        return f"0.00{unit}"
    if abs_v >= 1e9 and unit == "":
        return f"{val / 1e9:.2f}B"
    if abs_v >= 1e6 and unit == "":
        return f"{val / 1e6:.2f}M"
    if abs_v < 0.01:
        return f"{val:.4f}{unit}"
    if abs_v >= 1000 and val == int(val):
        return f"{int(val)}{unit}"
    return f"{val:.2f}{unit}"


def format_pct(val: float) -> str:
    """Format a percentage value (0..100+) with two decimal places."""
    if math.isnan(val):
        return "n/a"
    if math.isinf(val):
        return "+Inf" if val > 0 else "-Inf"
    return f"{val:.2f}%"


def compute_percentile(sorted_vals: list[float], q: float, *, empty_default: float = math.nan) -> float:
    """Compute the q-th percentile (0.0..1.0) on a sorted list via linear interpolation."""
    n = len(sorted_vals)
    if n == 0:
        return empty_default
    if n == 1:
        return float(sorted_vals[0])
    pos = q * (n - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return float(sorted_vals[lo])
    weight = pos - lo
    return float(sorted_vals[lo]) * (1.0 - weight) + float(sorted_vals[hi]) * weight


def summarize_samples(values: list[float]) -> tuple[float, float, float, float] | None:
    """Return (p50, p90, p99, avg) for a list of numeric samples."""
    finite = [float(v) for v in values if not math.isnan(v) and not math.isinf(v)]
    if not finite:
        return None
    s = sorted(finite)
    return (
        compute_percentile(s, 0.50),
        compute_percentile(s, 0.90),
        compute_percentile(s, 0.99),
        sum(s) / len(s),
    )


def calc_summary_stats(vals: list[float]) -> dict[str, float]:
    """Return count, min, p50, p90, p99, avg, max, and sum for a list of values."""
    finite = [float(v) for v in vals if not math.isnan(v) and not math.isinf(v)]
    if not finite:
        return {
            "count": 0.0,
            "min": 0.0,
            "p50": 0.0,
            "p90": 0.0,
            "p99": 0.0,
            "avg": 0.0,
            "max": 0.0,
            "sum": 0.0,
        }
    s = sorted(finite)
    total = sum(s)
    return {
        "count": float(len(s)),
        "min": s[0],
        "p50": compute_percentile(s, 0.50, empty_default=0.0),
        "p90": compute_percentile(s, 0.90, empty_default=0.0),
        "p99": compute_percentile(s, 0.99, empty_default=0.0),
        "avg": total / len(s),
        "max": s[-1],
        "sum": total,
    }


def sanitize_k8s_name(name: str, max_len: int = 52) -> str:
    s = re.sub(r"[^a-z0-9-]+", "-", name.lower()).strip("-")
    if not s:
        s = "job"
    if len(s) > max_len:
        s = s[:max_len].rstrip("-")
    return s


def compute_prometheus_port(apiserver_port: int, explicit_prom_port: int | None = None) -> int:
    if explicit_prom_port is not None:
        return explicit_prom_port
    env_port = os.environ.get("LOCAL_PROMETHEUS_PORT", "").strip()
    if env_port:
        return int(env_port)
    return DEFAULT_PROMETHEUS_BASE_PORT + apiserver_port - DEFAULT_APISERVER_PORT


def compute_registry_port(apiserver_port: int, explicit_registry_port: int | None = None) -> int:
    """Compute a deterministic Kubernetes NodePort (30000..32767) for the per-chamber registry."""
    if explicit_registry_port is not None:
        port = int(explicit_registry_port)
        if not (NODEPORT_MIN <= port <= NODEPORT_MAX):
            raise ValueError(
                f"Registry NodePort {port} out of valid Kubernetes NodePort range ({NODEPORT_MIN}..{NODEPORT_MAX})."
            )
        return port
    env_port = os.environ.get("LOCAL_REGISTRY_PORT", "").strip()
    if env_port:
        port = int(env_port)
        if not (NODEPORT_MIN <= port <= NODEPORT_MAX):
            raise ValueError(
                f"LOCAL_REGISTRY_PORT={port} out of valid Kubernetes NodePort range ({NODEPORT_MIN}..{NODEPORT_MAX})."
            )
        return port
    candidate = DEFAULT_REGISTRY_BASE_PORT + int(apiserver_port) - DEFAULT_APISERVER_PORT
    if NODEPORT_MIN <= candidate <= NODEPORT_MAX:
        return candidate
    span = NODEPORT_MAX - NODEPORT_MIN + 1
    return NODEPORT_MIN + ((candidate - NODEPORT_MIN) % span)


def parse_image_ref(image_ref: str) -> tuple[str, str, str]:
    """Parse an OCI/Docker image reference into (registry_host, repository, tag_or_digest).

    Examples:
      'localhost:30500/kueue:dev' -> ('localhost:30500', 'kueue', 'dev')
      'my-kueue:v1'               -> ('', 'my-kueue', 'v1')
      'kueue-dev'                 -> ('', 'kueue-dev', 'latest')
      'us-central1-docker.pkg.dev/proj/repo/kueue:main'
                                  -> ('us-central1-docker.pkg.dev', 'proj/repo/kueue', 'main')
    """
    ref = image_ref.strip()
    if not ref:
        raise ValueError("Image reference cannot be empty.")

    digest_part = ""
    if "@" in ref:
        ref, digest_part = ref.split("@", 1)

    # Separate optional tag after the last '/' (so registry host:port colons are preserved)
    last_slash = ref.rfind("/")
    after_slash = ref[last_slash + 1 :]
    if ":" in after_slash:
        colon_idx = last_slash + 1 + after_slash.rfind(":")
        name_part = ref[:colon_idx]
        tag_part = ref[colon_idx + 1 :]
    else:
        name_part = ref
        tag_part = ""

    if "/" in name_part:
        first_seg, rest = name_part.split("/", 1)
        if "." in first_seg or ":" in first_seg or first_seg == "localhost":
            registry_host = first_seg
            repo_path = rest
        else:
            registry_host = ""
            repo_path = name_part
    else:
        registry_host = ""
        repo_path = name_part

    qualifier = digest_part if digest_part else (tag_part or "latest")
    return registry_host, repo_path, qualifier


def docker_image_exists_locally(image_ref: str) -> bool:
    """Return True if image_ref exists in the local workstation Docker daemon."""
    try:
        proc = subprocess.run(
            ["docker", "image", "inspect", image_ref],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=15.0,
        )
        return proc.returncode == 0
    except Exception:
        return False


def resolve_kubeconfig_path(chamber_namespace: str, explicit_path: str | Path | None = None) -> Path:
    if explicit_path:
        return Path(explicit_path)
    env_kc = os.environ.get("KUBECONFIG_OUT", "").strip()
    if env_kc:
        return Path(env_kc)
    return ROOT_DIR / f"kwok-{chamber_namespace}.kubeconfig"
