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

"""Kubernetes/Kueue manifest builders and deployment manifest patch helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import re
from typing import Any

from chamber.constants import (
    DEFAULT_FAIR_SHARING_PREEMPTION_STRATEGIES,
    DEFAULT_FLAVOR_NAME,
)
from chamber.parsers import parse_duration_seconds, parse_k8s_quantity
from chamber.yaml_utils import to_clean_yaml


DEFAULT_WAIT_FOR_PODS_READY_TIMEOUT_SEC = 300.0
DEFAULT_WAIT_FOR_PODS_READY_MIN_WALL_SEC = 2.0
DEFAULT_WAIT_FOR_PODS_READY_MAX_DURATION_FRACTION = 0.5
DEFAULT_AFS_SAMPLING_INTERVAL_SEC = 300.0
DEFAULT_AFS_MIN_SAMPLING_WALL_SEC = 0.5

VALID_FAIR_SHARING_STRATEGY_SETS: tuple[tuple[str, ...], ...] = (
    ("LessThanOrEqualToFinalShare",),
    ("LessThanInitialShare",),
    ("LessThanOrEqualToFinalShare", "LessThanInitialShare"),
)


def format_go_duration_seconds(seconds: float) -> str:
    """Format a non-negative duration in seconds as a Go time.ParseDuration string ('0s', '10s', '2500ms')."""
    if seconds <= 0:
        return "0s"
    if abs(seconds - round(seconds)) < 1e-3:
        return f"{int(round(seconds))}s"
    return f"{max(100, int(round(seconds * 1000)))}ms"


@dataclass
class WaitForPodsReadyConfig:
    """Resolved Kueue waitForPodsReady configuration adjusted for --speedup and --duration."""

    enabled: bool = False
    raw_timeout_sec: float = DEFAULT_WAIT_FOR_PODS_READY_TIMEOUT_SEC
    effective_timeout_sec: float = DEFAULT_WAIT_FOR_PODS_READY_TIMEOUT_SEC
    clamped_timeout_to_duration: bool = False
    floored_timeout_to_min: bool = False
    raw_recovery_timeout_sec: float | None = None
    effective_recovery_timeout_sec: float | None = None
    clamped_recovery_to_duration: bool = False
    block_admission: bool | None = None
    requeuing_timestamp: str = "Eviction"
    backoff_limit_count: int | None = None
    raw_backoff_base_sec: float | None = None
    effective_backoff_base_sec: int | None = None
    raw_backoff_max_sec: float | None = None
    effective_backoff_max_sec: int | None = None

    def summary_str(self) -> str:
        if not self.enabled:
            return "disabled"
        notes: list[str] = [f"timeout={format_go_duration_seconds(self.effective_timeout_sec)} (raw {format_go_duration_seconds(self.raw_timeout_sec)})"]
        if self.clamped_timeout_to_duration:
            notes.append("clamped-to-duration")
        elif self.floored_timeout_to_min:
            notes.append("floored-to-min")
        if self.effective_recovery_timeout_sec is not None:
            notes.append(
                f"recoveryTimeout={format_go_duration_seconds(self.effective_recovery_timeout_sec)}"
                + (
                    f" (raw {format_go_duration_seconds(self.raw_recovery_timeout_sec)})"
                    if self.raw_recovery_timeout_sec is not None
                    else ""
                )
            )
        if self.block_admission is not None:
            notes.append(f"blockAdmission={str(self.block_admission).lower()}")
        if self.effective_backoff_base_sec is not None:
            notes.append(f"backoffBase={self.effective_backoff_base_sec}s")
        if self.effective_backoff_max_sec is not None:
            notes.append(f"backoffMax={self.effective_backoff_max_sec}s")
        if self.backoff_limit_count is not None:
            notes.append(f"backoffLimit={self.backoff_limit_count}")
        return ", ".join(notes)


def resolve_wait_for_pods_ready_config(
    *,
    enabled: bool | None = None,
    raw_timeout: str | float | None = None,
    raw_recovery_timeout: str | float | None = None,
    block_admission: bool | None = None,
    requeuing_timestamp: str | None = None,
    backoff_limit_count: int | None = None,
    raw_backoff_base: str | float | None = None,
    raw_backoff_max: str | float | None = None,
    speedup: float = 1.0,
    max_run_seconds: float | None = None,
    min_wall_timeout: str | float | None = DEFAULT_WAIT_FOR_PODS_READY_MIN_WALL_SEC,
    cap_to_duration: bool = True,
    max_duration_fraction: float = DEFAULT_WAIT_FOR_PODS_READY_MAX_DURATION_FRACTION,
) -> WaitForPodsReadyConfig:
    """Adjust Kueue waitForPodsReady timeouts and backoffs to --speedup and --duration."""
    has_explicit_subflag = any(
        x is not None
        for x in (
            raw_timeout,
            raw_recovery_timeout,
            block_admission,
            backoff_limit_count,
            raw_backoff_base,
            raw_backoff_max,
        )
    )
    eff_enabled = bool(enabled) if enabled is not None else has_explicit_subflag
    if not eff_enabled:
        return WaitForPodsReadyConfig(enabled=False)

    eff_speedup = max(1e-6, float(speedup))
    min_floor = (
        float(min_wall_timeout)
        if isinstance(min_wall_timeout, (int, float))
        else (parse_duration_seconds(str(min_wall_timeout)) or DEFAULT_WAIT_FOR_PODS_READY_MIN_WALL_SEC)
    )
    min_floor = max(0.5, min_floor)

    raw_t = (
        float(raw_timeout)
        if isinstance(raw_timeout, (int, float))
        else (
            parse_duration_seconds(str(raw_timeout))
            if raw_timeout is not None and str(raw_timeout).strip()
            else DEFAULT_WAIT_FOR_PODS_READY_TIMEOUT_SEC
        )
    )
    if raw_t is None or raw_t <= 0:
        raise ValueError(f"--wait-for-pods-ready-timeout must be > 0 (got {raw_timeout!r})")

    scaled_t = raw_t / eff_speedup
    floored_t = False
    if scaled_t < min_floor:
        scaled_t = min_floor
        floored_t = True

    clamped_t = False
    duration_cap: float | None = None
    if cap_to_duration and max_run_seconds is not None and max_run_seconds > 0:
        frac = max(0.1, min(0.9, float(max_duration_fraction)))
        duration_cap = max(min(min_floor, max_run_seconds * 0.8), max_run_seconds * frac)
        if scaled_t > duration_cap:
            scaled_t = duration_cap
            clamped_t = True
            floored_t = False

    raw_rec_sec: float | None = None
    eff_rec_sec: float | None = None
    clamped_rec = False
    if raw_recovery_timeout is not None and str(raw_recovery_timeout).strip() != "":
        raw_rec_sec = (
            float(raw_recovery_timeout)
            if isinstance(raw_recovery_timeout, (int, float))
            else parse_duration_seconds(str(raw_recovery_timeout))
        )
        if raw_rec_sec is None or raw_rec_sec < 0:
            raise ValueError(
                f"--wait-for-pods-ready-recovery-timeout must be >= 0 (got {raw_recovery_timeout!r})"
            )
        if raw_rec_sec == 0:
            eff_rec_sec = 0.0
        else:
            eff_rec_sec = max(min_floor, raw_rec_sec / eff_speedup)
            if duration_cap is not None and eff_rec_sec > duration_cap:
                eff_rec_sec = duration_cap
                clamped_rec = True

    raw_bb_sec: float | None = None
    eff_bb_sec: int | None = None
    if raw_backoff_base is not None and str(raw_backoff_base).strip() != "":
        raw_bb_sec = (
            float(raw_backoff_base)
            if isinstance(raw_backoff_base, (int, float))
            else parse_duration_seconds(str(raw_backoff_base))
        )
        if raw_bb_sec is not None and raw_bb_sec > 0:
            scaled_bb = max(1, int(round(raw_bb_sec / eff_speedup)))
            if cap_to_duration and max_run_seconds is not None and max_run_seconds > 0:
                scaled_bb = min(scaled_bb, max(1, int(max_run_seconds * 0.25)))
            eff_bb_sec = scaled_bb

    raw_bm_sec: float | None = None
    eff_bm_sec: int | None = None
    if raw_backoff_max is not None and str(raw_backoff_max).strip() != "":
        raw_bm_sec = (
            float(raw_backoff_max)
            if isinstance(raw_backoff_max, (int, float))
            else parse_duration_seconds(str(raw_backoff_max))
        )
        if raw_bm_sec is not None and raw_bm_sec > 0:
            scaled_bm = max(1, int(round(raw_bm_sec / eff_speedup)))
            if cap_to_duration and max_run_seconds is not None and max_run_seconds > 0:
                scaled_bm = min(scaled_bm, max(1, int(max_run_seconds * 0.5)))
            if eff_bb_sec is not None and scaled_bm < eff_bb_sec:
                scaled_bm = eff_bb_sec
            eff_bm_sec = scaled_bm
    elif eff_bb_sec is not None:
        default_bm = max(eff_bb_sec, int(round(3600.0 / eff_speedup)))
        if cap_to_duration and max_run_seconds is not None and max_run_seconds > 0:
            default_bm = max(eff_bb_sec, min(default_bm, max(1, int(max_run_seconds * 0.5))))
        eff_bm_sec = default_bm

    req_ts = (requeuing_timestamp or "Eviction").strip()
    if req_ts not in ("Eviction", "Creation"):
        raise ValueError(
            f"--wait-for-pods-ready-requeue-timestamp must be 'Eviction' or 'Creation' (got {requeuing_timestamp!r})"
        )

    return WaitForPodsReadyConfig(
        enabled=True,
        raw_timeout_sec=raw_t,
        effective_timeout_sec=scaled_t,
        clamped_timeout_to_duration=clamped_t,
        floored_timeout_to_min=floored_t,
        raw_recovery_timeout_sec=raw_rec_sec,
        effective_recovery_timeout_sec=eff_rec_sec,
        clamped_recovery_to_duration=clamped_rec,
        block_admission=block_admission,
        requeuing_timestamp=req_ts,
        backoff_limit_count=backoff_limit_count,
        raw_backoff_base_sec=raw_bb_sec,
        effective_backoff_base_sec=eff_bb_sec,
        raw_backoff_max_sec=raw_bm_sec,
        effective_backoff_max_sec=eff_bm_sec,
    )


def parse_feature_gates_arg(raw: str | dict[str, bool] | None) -> dict[str, bool]:
    """Parse comma-separated 'GateA=true,GateB=false' overrides into a dict[str, bool]."""
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return {str(k): bool(v) for k, v in raw.items() if str(k).strip()}
    out: dict[str, bool] = {}
    for item in str(raw).split(","):
        tok = item.strip()
        if not tok:
            continue
        if "=" not in tok:
            raise ValueError(f"Invalid --kueue-feature-gates entry {tok!r}: expected FeatureName=true|false")
        k, v = tok.split("=", 1)
        k_clean = k.strip()
        v_clean = v.strip().lower()
        if v_clean in ("true", "1", "yes"):
            out[k_clean] = True
        elif v_clean in ("false", "0", "no"):
            out[k_clean] = False
        else:
            raise ValueError(f"Invalid boolean in --kueue-feature-gates entry {tok!r}: expected true|false")
    return out


def parse_fair_sharing_strategies(
    raw: str | list[str] | tuple[str, ...] | None,
    *,
    field_desc: str = "fairSharing.preemptionStrategies",
) -> tuple[str, ...]:
    """Validate and normalize Kueue FairSharing preemptionStrategies against Kueue's legal sets."""
    if raw is None:
        return DEFAULT_FAIR_SHARING_PREEMPTION_STRATEGIES
    if isinstance(raw, str):
        items = tuple(tok.strip() for tok in raw.split(",") if tok.strip())
    elif isinstance(raw, (list, tuple)):
        items = tuple(str(tok).strip() for tok in raw if str(tok).strip())
    else:
        raise ValueError(f"Invalid {field_desc} {raw!r}: expected list of strategy names")
    if not items:
        raise ValueError(f"{field_desc}: must specify at least one PreemptionStrategy when fairSharing is enabled")
    if items not in VALID_FAIR_SHARING_STRATEGY_SETS:
        allowed_str = " | ".join("[" + ", ".join(s) + "]" for s in VALID_FAIR_SHARING_STRATEGY_SETS)
        raise ValueError(f"Unsupported {field_desc} {list(items)!r}; Kueue only supports: {allowed_str}")
    return items


def parse_afs_resource_weights(
    raw: str | dict[str, Any] | None,
    *,
    field_desc: str = "admissionFairSharing.resourceWeights",
) -> dict[str, float]:
    """Parse 'resA=1,resB=0' or dict[str, float] into validated non-negative resource weights."""
    if raw is None:
        return {}
    out: dict[str, float] = {}
    if isinstance(raw, dict):
        for k, v in raw.items():
            rk = str(k).strip()
            if not rk:
                continue
            try:
                val = float(v)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"Invalid numeric weight in {field_desc}[{rk!r}]={v!r}") from exc
            if val < 0:
                raise ValueError(f"Negative weight in {field_desc}[{rk!r}]={val}: must be >= 0")
            out[rk] = val
        return out
    for tok in str(raw).split(","):
        item = tok.strip()
        if not item:
            continue
        if "=" not in item:
            raise ValueError(f"Invalid {field_desc} entry {item!r}: expected <resource>=<non-negative-float>")
        rk, rv = item.split("=", 1)
        rk_clean = rk.strip()
        try:
            val = float(rv.strip())
        except ValueError as exc:
            raise ValueError(f"Invalid numeric weight in {field_desc} entry {item!r}") from exc
        if val < 0:
            raise ValueError(f"Negative weight in {field_desc} entry {item!r}: must be >= 0")
        out[rk_clean] = val
    return out


@dataclass
class AdmissionFairSharingConfig:
    """Resolved Kueue admissionFairSharing configuration adjusted for --speedup and --duration."""

    enabled: bool = False
    raw_usage_half_life_sec: float = 0.0
    effective_usage_half_life_sec: float = 0.0
    raw_usage_sampling_interval_sec: float = DEFAULT_AFS_SAMPLING_INTERVAL_SEC
    effective_usage_sampling_interval_sec: float = DEFAULT_AFS_SAMPLING_INTERVAL_SEC
    resource_weights: dict[str, float] = field(default_factory=dict)

    def summary_str(self) -> str:
        if not self.enabled:
            return "disabled"
        parts = [
            f"usageHalfLifeTime={format_go_duration_seconds(self.effective_usage_half_life_sec)} "
            f"(raw {format_go_duration_seconds(self.raw_usage_half_life_sec)})",
            f"usageSamplingInterval={format_go_duration_seconds(self.effective_usage_sampling_interval_sec)} "
            f"(raw {format_go_duration_seconds(self.raw_usage_sampling_interval_sec)})",
        ]
        if self.resource_weights:
            rw_str = ", ".join(f"{k}={v:g}" for k, v in sorted(self.resource_weights.items()))
            parts.append(f"resourceWeights={{{rw_str}}}")
        return ", ".join(parts)


def resolve_admission_fair_sharing_config(
    *,
    enabled: bool | None = None,
    raw_usage_half_life: str | float | None = None,
    raw_usage_sampling_interval: str | float | None = None,
    resource_weights: str | dict[str, Any] | None = None,
    scenario_afs: dict[str, Any] | None = None,
    speedup: float = 1.0,
    max_run_seconds: float | None = None,
) -> AdmissionFairSharingConfig:
    """Resolve Kueue admissionFairSharing configuration (CLI overrides > scenario Configuration > disabled)."""
    has_cli_subflag = any(
        x is not None for x in (raw_usage_half_life, raw_usage_sampling_interval, resource_weights)
    )
    if enabled is not None:
        eff_enabled = bool(enabled)
    elif has_cli_subflag:
        eff_enabled = True
    elif scenario_afs is not None:
        eff_enabled = True
    else:
        return AdmissionFairSharingConfig(enabled=False)

    if not eff_enabled:
        return AdmissionFairSharingConfig(enabled=False)

    base_afs = scenario_afs or {}
    hl_src = raw_usage_half_life if raw_usage_half_life is not None else base_afs.get("usageHalfLifeTime", "0s")
    si_src = (
        raw_usage_sampling_interval
        if raw_usage_sampling_interval is not None
        else base_afs.get("usageSamplingInterval", "5m")
    )
    rw_src = resource_weights if resource_weights is not None else base_afs.get("resourceWeights")

    if isinstance(hl_src, (int, float)):
        raw_hl = float(hl_src)
    elif str(hl_src).strip() in ("0", "0s", "0ms", ""):
        raw_hl = 0.0
    else:
        parsed_hl = parse_duration_seconds(str(hl_src))
        if parsed_hl is None:
            raise ValueError(f"Invalid admissionFairSharing.usageHalfLifeTime {hl_src!r}")
        raw_hl = parsed_hl
    if raw_hl < 0:
        raise ValueError(f"admissionFairSharing.usageHalfLifeTime must be >= 0 (got {hl_src!r})")

    if isinstance(si_src, (int, float)):
        raw_si = float(si_src)
    else:
        parsed_si = parse_duration_seconds(str(si_src))
        if parsed_si is None:
            raise ValueError(f"Invalid admissionFairSharing.usageSamplingInterval {si_src!r}")
        raw_si = parsed_si
    if raw_si <= 0:
        raise ValueError(f"admissionFairSharing.usageSamplingInterval must be > 0 (got {si_src!r})")

    parsed_weights = parse_afs_resource_weights(rw_src)
    eff_speedup = max(1e-6, float(speedup))
    eff_hl = 0.0 if raw_hl == 0.0 else max(DEFAULT_AFS_MIN_SAMPLING_WALL_SEC, raw_hl / eff_speedup)
    eff_si = max(DEFAULT_AFS_MIN_SAMPLING_WALL_SEC, raw_si / eff_speedup)
    if max_run_seconds is not None and max_run_seconds > 0:
        eff_si = min(eff_si, max(DEFAULT_AFS_MIN_SAMPLING_WALL_SEC, max_run_seconds * 0.25))

    return AdmissionFairSharingConfig(
        enabled=True,
        raw_usage_half_life_sec=raw_hl,
        effective_usage_half_life_sec=eff_hl,
        raw_usage_sampling_interval_sec=raw_si,
        effective_usage_sampling_interval_sec=eff_si,
        resource_weights=parsed_weights,
    )


@dataclass
class KueueScenarioConfiguration:
    """Validated Kueue `Configuration` statement extracted from a test scenario (`infra.json` or `kueue-config.json`)."""

    fair_sharing_enabled: bool = False
    fair_sharing_strategies: tuple[str, ...] = DEFAULT_FAIR_SHARING_PREEMPTION_STRATEGIES
    afs_raw: dict[str, Any] | None = None
    wfpr_raw: dict[str, Any] | None = None
    feature_gates: dict[str, bool] = field(default_factory=dict)
    resources_block: dict[str, Any] | None = None
    manage_jobs_without_queue_name: bool | None = None
    raw_doc: dict[str, Any] = field(default_factory=dict)


def parse_kueue_configuration_payload(
    doc: dict[str, Any],
    *,
    source_desc: str = "Configuration",
) -> KueueScenarioConfiguration:
    """Validate and parse a Kueue `config.kueue.x-k8s.io/v1beta2` (or `v1beta1`) `Configuration` payload."""
    if not isinstance(doc, dict):
        raise ValueError(f"{source_desc}: expected object dict for Kueue Configuration")
    api_ver = str(doc.get("apiVersion") or "")
    kind = str(doc.get("kind") or "")
    if kind != "Configuration" or api_ver not in (
        "config.kueue.x-k8s.io/v1beta2",
        "config.kueue.x-k8s.io/v1beta1",
    ):
        raise ValueError(
            f"{source_desc}: expected apiVersion 'config.kueue.x-k8s.io/v1beta2' (or v1beta1) and kind 'Configuration', "
            f"got ({api_ver!r}, {kind!r})"
        )

    fs_raw = doc.get("fairSharing")
    if fs_raw is None:
        fs_enabled = False
        fs_strategies = DEFAULT_FAIR_SHARING_PREEMPTION_STRATEGIES
    elif isinstance(fs_raw, dict):
        if fs_raw.get("enable") is False:
            fs_enabled = False
            fs_strategies = DEFAULT_FAIR_SHARING_PREEMPTION_STRATEGIES
        else:
            fs_enabled = True
            if "preemptionStrategies" in fs_raw:
                fs_strategies = parse_fair_sharing_strategies(
                    fs_raw["preemptionStrategies"],
                    field_desc=f"{source_desc} fairSharing.preemptionStrategies",
                )
            else:
                fs_strategies = DEFAULT_FAIR_SHARING_PREEMPTION_STRATEGIES
    else:
        raise ValueError(f"{source_desc}: fairSharing must be a mapping or null, got {type(fs_raw).__name__}")

    afs_raw = doc.get("admissionFairSharing")
    if afs_raw is not None:
        if not isinstance(afs_raw, dict):
            raise ValueError(
                f"{source_desc}: admissionFairSharing must be a mapping or null, got {type(afs_raw).__name__}"
            )
        # Validate values immediately via resolve_admission_fair_sharing_config.
        resolve_admission_fair_sharing_config(scenario_afs=afs_raw, speedup=1.0)

    wfpr_raw = doc.get("waitForPodsReady")
    if wfpr_raw is not None:
        if not isinstance(wfpr_raw, dict):
            raise ValueError(
                f"{source_desc}: waitForPodsReady must be a mapping or null, got {type(wfpr_raw).__name__}"
            )
        req_strat = wfpr_raw.get("requeuingStrategy") or {}
        if not isinstance(req_strat, dict):
            raise ValueError(f"{source_desc}: waitForPodsReady.requeuingStrategy must be a mapping")
        resolve_wait_for_pods_ready_config(
            enabled=True,
            raw_timeout=wfpr_raw.get("timeout"),
            raw_recovery_timeout=wfpr_raw.get("recoveryTimeout"),
            block_admission=wfpr_raw.get("blockAdmission"),
            requeuing_timestamp=req_strat.get("timestamp"),
            backoff_limit_count=req_strat.get("backoffLimitCount"),
            raw_backoff_base=req_strat.get("backoffBaseSeconds"),
            raw_backoff_max=req_strat.get("backoffMaxSeconds"),
            speedup=1.0,
        )

    fg_raw = doc.get("featureGates")
    fg_parsed: dict[str, bool] = {}
    if fg_raw is not None:
        if not isinstance(fg_raw, dict):
            raise ValueError(f"{source_desc}: featureGates must be a mapping of bools")
        for k, v in fg_raw.items():
            if not isinstance(v, bool):
                raise ValueError(f"{source_desc}: featureGates[{k!r}] must be a boolean, got {v!r}")
            fg_parsed[str(k)] = v

    res_raw = doc.get("resources")
    if res_raw is not None and not isinstance(res_raw, dict):
        raise ValueError(f"{source_desc}: resources must be a mapping or null")

    mjwqn_raw = doc.get("manageJobsWithoutQueueName")
    if mjwqn_raw is not None and not isinstance(mjwqn_raw, bool):
        raise ValueError(f"{source_desc}: manageJobsWithoutQueueName must be a boolean")

    return KueueScenarioConfiguration(
        fair_sharing_enabled=fs_enabled,
        fair_sharing_strategies=fs_strategies,
        afs_raw=dict(afs_raw) if isinstance(afs_raw, dict) else None,
        wfpr_raw=dict(wfpr_raw) if isinstance(wfpr_raw, dict) else None,
        feature_gates=fg_parsed,
        resources_block=dict(res_raw) if isinstance(res_raw, dict) and res_raw else None,
        manage_jobs_without_queue_name=mjwqn_raw,
        raw_doc=dict(doc),
    )


def build_kwok_node_manifest(
    name: str,
    node_idx: int,
    cpu: int,
    mem_gi: int,
    pods: int = 110,
    role: str = "agent",
    extra_labels: dict[str, str] | None = None,
    extra_annotations: dict[str, str] | None = None,
    accel_resource: str | None = None,
    accel_count: int | None = None,
    use_legacy_cidr: bool = False,
) -> dict[str, Any]:
    """Build a simulated KWOK Node manifest with annotations for Stage/node-initialize."""
    if use_legacy_cidr and node_idx < 256:
        cidr = f"10.244.{node_idx}.0/24"
    else:
        octet2 = 100 + (node_idx // 256)
        octet3 = node_idx % 256
        cidr = f"10.{octet2}.{octet3}.0/24"

    annotations: dict[str, str] = {
        "kwok.x-k8s.io/node": "fake",
        "node.alpha.kubernetes.io/ttl": "0",
        "kueue-optimizer.gke.io/node-cpu": str(cpu),
        "kueue-optimizer.gke.io/node-memory": f"{mem_gi}Gi",
        "kueue-optimizer.gke.io/node-pods": str(pods),
    }
    if accel_resource and accel_count:
        annotations["kueue-optimizer.gke.io/node-accel-resource"] = accel_resource
        annotations["kueue-optimizer.gke.io/node-accel-count"] = str(accel_count)
    if extra_annotations:
        annotations.update(extra_annotations)

    labels: dict[str, str] = {
        "beta.kubernetes.io/arch": "amd64",
        "beta.kubernetes.io/os": "linux",
        "kubernetes.io/arch": "amd64",
        "kubernetes.io/hostname": name,
        "kubernetes.io/os": "linux",
        "kubernetes.io/role": "agent",
        "node-role.kubernetes.io/agent": "",
        "type": "kwok",
        "kueue-optimizer.gke.io/node-role": role,
    }
    if extra_labels:
        labels.update(extra_labels)

    resources: dict[str, str] = {
        "cpu": str(cpu),
        "memory": f"{mem_gi}Gi",
        "pods": str(pods),
    }
    if accel_resource and accel_count:
        resources[accel_resource] = str(accel_count)

    return {
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {
            "name": name,
            "annotations": annotations,
            "labels": labels,
        },
        "spec": {
            "podCIDR": cidr,
            "podCIDRs": [cidr],
        },
        "status": {
            "allocatable": dict(resources),
            "capacity": dict(resources),
            "nodeInfo": {
                "architecture": "amd64",
                "kubeProxyVersion": "fake",
                "kubeletVersion": "fake",
                "operatingSystem": "linux",
            },
        },
    }


def build_namespace_manifest(
    name: str,
    part_of: str = "kueue-optimizer",
    exclude_from_kueue: bool = False,
    extra_labels: dict[str, str] | None = None,
) -> dict[str, Any]:
    labels: dict[str, str] = {
        "app.kubernetes.io/part-of": part_of,
    }
    if exclude_from_kueue:
        labels["kueue.x-k8s.io/exclude"] = "true"
        labels["kueue-optimizer.gke.io/kueue-exclude"] = "true"
    if extra_labels:
        labels.update(extra_labels)
    return {
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": name,
            "labels": labels,
        },
    }


def build_resource_flavor_manifest(
    name: str = DEFAULT_FLAVOR_NAME,
    node_labels: dict[str, str] | None = None,
) -> dict[str, Any]:
    return {
        "apiVersion": "kueue.x-k8s.io/v1beta2",
        "kind": "ResourceFlavor",
        "metadata": {"name": name},
        "spec": {
            "nodeLabels": node_labels if node_labels is not None else {"type": "kwok"},
        },
    }


def build_priority_class_manifest(name: str, value: int) -> dict[str, Any]:
    return {
        "apiVersion": "kueue.x-k8s.io/v1beta2",
        "kind": "WorkloadPriorityClass",
        "metadata": {"name": name},
        "value": int(value),
    }


def build_cohort_manifest(name: str) -> dict[str, Any]:
    return {
        "apiVersion": "kueue.x-k8s.io/v1beta2",
        "kind": "Cohort",
        "metadata": {"name": name},
    }


def build_cluster_queue_manifest(
    name: str,
    flavor_name: str,
    resources: list[dict[str, str]],
    queueing_strategy: str = "StrictFIFO",
    cohort_name: str | None = None,
    enable_preemption: bool = True,
    enable_fair_sharing: bool = True,
    fair_sharing_weight: int | str | None = None,
) -> dict[str, Any]:
    covered = [r["name"] for r in resources]
    spec: dict[str, Any] = {
        "namespaceSelector": {},
        "queueingStrategy": queueing_strategy,
        "resourceGroups": [
            {
                "coveredResources": covered,
                "flavors": [
                    {
                        "name": flavor_name,
                        "resources": resources,
                    }
                ],
            }
        ],
    }
    if cohort_name:
        spec["cohortName"] = cohort_name
    if enable_fair_sharing:
        spec["fairSharing"] = {"weight": 1 if fair_sharing_weight is None else fair_sharing_weight}
    elif fair_sharing_weight is not None:
        spec["fairSharing"] = {"weight": fair_sharing_weight}
    if enable_preemption:
        preemption_spec: dict[str, Any] = {
            "withinClusterQueue": "LowerPriority",
            "reclaimWithinCohort": "Any",
        }
        if not enable_fair_sharing:
            preemption_spec["borrowWithinCohort"] = {
                "policy": "LowerPriority",
            }
        spec["preemption"] = preemption_spec
    return {
        "apiVersion": "kueue.x-k8s.io/v1beta2",
        "kind": "ClusterQueue",
        "metadata": {"name": name},
        "spec": spec,
    }


def build_local_queue_manifest(name: str, namespace: str, cluster_queue: str) -> dict[str, Any]:
    return {
        "apiVersion": "kueue.x-k8s.io/v1beta2",
        "kind": "LocalQueue",
        "metadata": {
            "name": name,
            "namespace": namespace,
        },
        "spec": {
            "clusterQueue": cluster_queue,
        },
    }


def match_yaml_section_field(yaml_text: str, section_key: str, field_key: str) -> str | None:
    """Return the scalar value of `field_key` inside an indented YAML mapping `section_key:` (or None)."""
    pattern = (
        rf"^(?P<indent>[ \t]*)(?:-[ \t]+)?{re.escape(section_key)}:[ \t]*(?:#[^\n]*)?\n"
        rf"(?:(?P=indent)[ \t]+(?![- \t]*{re.escape(field_key)}:[ \t]*)[A-Za-z0-9_./-]+:[ \t]*[^\n]*\n)*"
        rf"(?P=indent)[ \t]+{re.escape(field_key)}:[ \t]*(?P<val>['\"]?[0-9A-Za-z._+-]+['\"]?)"
    )
    m = re.search(pattern, yaml_text, flags=re.MULTILINE)
    return m.group("val") if m else None


def replace_yaml_section_field(yaml_text: str, section_key: str, field_key: str, new_value: str) -> str:
    """Replace the scalar value of `field_key` inside any `section_key:` YAML mapping without depending on its prior value."""
    pattern = (
        rf"(^(?P<indent>[ \t]*)(?:-[ \t]+)?{re.escape(section_key)}:[ \t]*(?:#[^\n]*)?\n"
        rf"(?:(?P=indent)[ \t]+(?![- \t]*{re.escape(field_key)}:[ \t]*)[A-Za-z0-9_./-]+:[ \t]*[^\n]*\n)*"
        rf"(?P=indent)[ \t]+{re.escape(field_key)}:[ \t]*)['\"]?[0-9A-Za-z._+-]+['\"]?"
    )
    return re.sub(pattern, rf"\g<1>{new_value}", yaml_text, flags=re.MULTILINE)


def patch_registry_manifest(base_dst: Path, node_port: int, memory_limit: str) -> None:
    reg_yaml = base_dst / "registry.yaml"
    if not reg_yaml.exists():
        return
    reg_text = reg_yaml.read_text(encoding="utf-8")
    reg_text = re.sub(
        r"^([ \t]*nodePort:[ \t]*)\d+",
        rf"\g<1>{node_port}",
        reg_text,
        flags=re.MULTILINE,
    )
    mem_lim = (memory_limit or "").strip()
    if mem_lim:
        reg_text = replace_yaml_section_field(reg_text, "emptyDir", "sizeLimit", mem_lim)
        reg_text = replace_yaml_section_field(reg_text, "limits", "memory", mem_lim)
        # Ensure requests.memory never exceeds limits.memory when a smaller memory limit is requested
        cur_req = match_yaml_section_field(reg_text, "requests", "memory")
        if cur_req is not None:
            req_gib = parse_k8s_quantity(cur_req, "memory")
            lim_gib = parse_k8s_quantity(mem_lim, "memory")
            if lim_gib > 0 and req_gib > lim_gib:
                reg_text = replace_yaml_section_field(reg_text, "requests", "memory", mem_lim)
    reg_yaml.write_text(reg_text, encoding="utf-8")


def _insert_top_level_kueue_config_block(cfg_text: str, block_lines: list[str]) -> str:
    block = "\n".join(block_lines) + "\n"
    if re.search(r"^internalCertManagement:", cfg_text, flags=re.MULTILINE):
        return re.sub(
            r"^internalCertManagement:",
            f"{block}internalCertManagement:",
            cfg_text,
            count=1,
            flags=re.MULTILINE,
        )
    return cfg_text.rstrip("\n") + "\n" + block


def patch_kueue_fair_sharing_config(
    base_dst: Path,
    enable_fair_sharing: bool,
    strategies: tuple[str, ...] | list[str] | None = None,
) -> None:
    """Enable (with configurable preemptionStrategies) or disable global FairSharing in controller_manager_config.yaml."""
    cfg_yaml = base_dst / "config" / "controller_manager_config.yaml"
    if not cfg_yaml.exists():
        return
    cfg_text = cfg_yaml.read_text(encoding="utf-8")
    cfg_text = re.sub(
        r"^fairSharing:[ \t]*(?:#[^\n]*)?\n(?:[ \t]+[^\n]*\n|-+[ \t]*[^\n]*\n)*",
        "",
        cfg_text,
        flags=re.MULTILINE,
    )
    if enable_fair_sharing:
        resolved_strats = parse_fair_sharing_strategies(strategies)
        fs_lines = [
            "fairSharing:",
            "  preemptionStrategies:",
            *[f"  - {s}" for s in resolved_strats],
        ]
        cfg_text = _insert_top_level_kueue_config_block(cfg_text, fs_lines)
    cfg_yaml.write_text(cfg_text, encoding="utf-8")


def patch_kueue_admission_fair_sharing_config(
    base_dst: Path,
    afs_cfg: AdmissionFairSharingConfig | None,
) -> None:
    """Configure or remove admissionFairSharing in controller_manager_config.yaml."""
    cfg_yaml = base_dst / "config" / "controller_manager_config.yaml"
    if not cfg_yaml.exists():
        return
    cfg_text = cfg_yaml.read_text(encoding="utf-8")
    cfg_text = re.sub(
        r"^admissionFairSharing:[ \t]*(?:#[^\n]*)?\n(?:[ \t]+[^\n]*\n|-+[ \t]*[^\n]*\n)*",
        "",
        cfg_text,
        flags=re.MULTILINE,
    )
    if afs_cfg is not None and afs_cfg.enabled:
        afs_lines = [
            "admissionFairSharing:",
            f"  usageHalfLifeTime: {format_go_duration_seconds(afs_cfg.effective_usage_half_life_sec)}",
            f"  usageSamplingInterval: {format_go_duration_seconds(afs_cfg.effective_usage_sampling_interval_sec)}",
        ]
        if afs_cfg.resource_weights:
            afs_lines.append("  resourceWeights:")
            for rk, rv in sorted(afs_cfg.resource_weights.items()):
                val_str = str(int(rv)) if abs(rv - round(rv)) < 1e-9 else f"{rv:g}"
                afs_lines.append(f"    {rk}: {val_str}")
        cfg_text = _insert_top_level_kueue_config_block(cfg_text, afs_lines)
    cfg_yaml.write_text(cfg_text, encoding="utf-8")


def patch_kueue_extra_configuration_fields(
    base_dst: Path,
    *,
    resources_block: dict[str, Any] | None = None,
    manage_jobs_without_queue_name: bool | None = None,
) -> None:
    """Apply optional `resources` and `manageJobsWithoutQueueName` fields into controller_manager_config.yaml."""
    cfg_yaml = base_dst / "config" / "controller_manager_config.yaml"
    if not cfg_yaml.exists():
        return
    cfg_text = cfg_yaml.read_text(encoding="utf-8")
    cfg_text = re.sub(
        r"^resources:[ \t]*(?:#[^\n]*)?\n(?:[ \t]+[^\n]*\n|-+[ \t]*[^\n]*\n)*",
        "",
        cfg_text,
        flags=re.MULTILINE,
    )
    cfg_text = re.sub(
        r"^manageJobsWithoutQueueName:[ \t]*[^\n]*\n",
        "",
        cfg_text,
        flags=re.MULTILINE,
    )
    if resources_block:
        res_yaml = to_clean_yaml({"resources": resources_block})
        cfg_text = _insert_top_level_kueue_config_block(cfg_text, res_yaml.splitlines())
    if manage_jobs_without_queue_name is not None:
        cfg_text = _insert_top_level_kueue_config_block(
            cfg_text,
            [f"manageJobsWithoutQueueName: {str(bool(manage_jobs_without_queue_name)).lower()}"],
        )
    cfg_yaml.write_text(cfg_text, encoding="utf-8")


def patch_kueue_wait_for_pods_ready_config(
    base_dst: Path,
    wfpr_cfg: WaitForPodsReadyConfig | None,
) -> None:
    """Configure or remove waitForPodsReady in controller_manager_config.yaml."""
    cfg_yaml = base_dst / "config" / "controller_manager_config.yaml"
    if not cfg_yaml.exists():
        return
    cfg_text = cfg_yaml.read_text(encoding="utf-8")
    cfg_text = re.sub(
        r"^waitForPodsReady:[ \t]*(?:#[^\n]*)?\n(?:[ \t]+[^\n]*\n|-+[ \t]*[^\n]*\n)*",
        "",
        cfg_text,
        flags=re.MULTILINE,
    )
    if wfpr_cfg is not None and wfpr_cfg.enabled:
        lines = [
            "waitForPodsReady:",
            f"  timeout: {format_go_duration_seconds(wfpr_cfg.effective_timeout_sec)}",
        ]
        if wfpr_cfg.effective_recovery_timeout_sec is not None:
            lines.append(
                f"  recoveryTimeout: {format_go_duration_seconds(wfpr_cfg.effective_recovery_timeout_sec)}"
            )
        if wfpr_cfg.block_admission is not None:
            lines.append(f"  blockAdmission: {str(bool(wfpr_cfg.block_admission)).lower()}")
        has_req = (
            wfpr_cfg.requeuing_timestamp != "Eviction"
            or wfpr_cfg.backoff_limit_count is not None
            or wfpr_cfg.effective_backoff_base_sec is not None
            or wfpr_cfg.effective_backoff_max_sec is not None
        )
        if has_req:
            lines.append("  requeuingStrategy:")
            lines.append(f"    timestamp: {wfpr_cfg.requeuing_timestamp}")
            if wfpr_cfg.backoff_limit_count is not None:
                lines.append(f"    backoffLimitCount: {int(wfpr_cfg.backoff_limit_count)}")
            if wfpr_cfg.effective_backoff_base_sec is not None:
                lines.append(f"    backoffBaseSeconds: {int(wfpr_cfg.effective_backoff_base_sec)}")
            if wfpr_cfg.effective_backoff_max_sec is not None:
                lines.append(f"    backoffMaxSeconds: {int(wfpr_cfg.effective_backoff_max_sec)}")
        cfg_text = _insert_top_level_kueue_config_block(cfg_text, lines)
    cfg_yaml.write_text(cfg_text, encoding="utf-8")


def patch_kueue_feature_gates_config(
    base_dst: Path,
    feature_gates: dict[str, bool] | None,
) -> None:
    """Configure or remove featureGates in controller_manager_config.yaml."""
    cfg_yaml = base_dst / "config" / "controller_manager_config.yaml"
    if not cfg_yaml.exists():
        return
    cfg_text = cfg_yaml.read_text(encoding="utf-8")
    cfg_text = re.sub(
        r"^featureGates:[ \t]*(?:#[^\n]*)?\n(?:[ \t]+[^\n]*\n|-+[ \t]*[^\n]*\n)*",
        "",
        cfg_text,
        flags=re.MULTILINE,
    )
    if feature_gates:
        lines = ["featureGates:"]
        for k in sorted(feature_gates):
            lines.append(f"  {k}: {str(bool(feature_gates[k])).lower()}")
        cfg_text = _insert_top_level_kueue_config_block(cfg_text, lines)
    cfg_yaml.write_text(cfg_text, encoding="utf-8")


def patch_kustomize_kueue_image(base_dst: Path, image_ref: str) -> None:
    ref = image_ref.strip()
    if not ref:
        return
    kueue_yaml = base_dst / "kueue.yaml"
    if kueue_yaml.exists():
        k_text = kueue_yaml.read_text(encoding="utf-8")
        k_text = re.sub(
            r"(-\s+name:\s+manager\s*\n\s+image:\s*)\S+(\s*\n\s+imagePullPolicy:\s*)\S+",
            rf"\g<1>{ref}\g<2>Always",
            k_text,
            count=1,
        )
        kueue_yaml.write_text(k_text, encoding="utf-8")

    kust_yaml = base_dst / "kustomization.yaml"
    if kust_yaml.exists():
        kust_text = kust_yaml.read_text(encoding="utf-8")
        # Remove the default kueue images override block so the explicit image in kueue.yaml wins verbatim
        kust_text = re.sub(
            r"-\s+name:\s+us-central1-docker\.pkg\.dev/k8s-staging-images/kueue/kueue\s*\n"
            r"\s+newName:\s+\S+\s*\n"
            r"\s+newTag:\s+\S+\s*\n?",
            "",
            kust_text,
        )
        kust_yaml.write_text(kust_text, encoding="utf-8")
