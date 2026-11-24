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

"""Schema validation, GVK allowlists, and manifest sanitization for scraped scenarios."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

from chamber.constants import DEFAULT_PAUSE_IMAGE, RESERVED_POD_LABELS, validate_dns_label
from chamber.parsers import (
    format_pod_execution_seconds,
    parse_k8s_quantity,
    parse_positive_duration_annotation,
    parse_rfc3339_epoch,
    sanitize_k8s_name,
)


ALLOWED_WORKFLOW_GVKS: frozenset[tuple[str, str]] = frozenset(
    {
        ("batch/v1", "Job"),
        ("jobset.x-k8s.io/v1alpha2", "JobSet"),
        ("leaderworkerset.x-k8s.io/v1", "LeaderWorkerSet"),
        ("apps/v1", "Deployment"),
        ("ray.io/v1", "RayJob"),
        ("ray.io/v1", "RayCluster"),
        ("v1", "Pod"),
    }
)
ALLOWED_WORKFLOW_KINDS: frozenset[str] = frozenset(k for _, k in ALLOWED_WORKFLOW_GVKS)

ALLOWED_INFRA_GVKS: frozenset[tuple[str, str]] = frozenset(
    {
        ("v1", "Node"),
        ("v1", "Namespace"),
        ("v1", "Pod"),
        ("node.k8s.io/v1", "RuntimeClass"),
        ("scheduling.k8s.io/v1", "PriorityClass"),
        ("config.kueue.x-k8s.io/v1beta2", "Configuration"),
        ("config.kueue.x-k8s.io/v1beta1", "Configuration"),
        ("kueue.x-k8s.io/v1beta2", "ClusterQueue"),
        ("kueue.x-k8s.io/v1beta2", "LocalQueue"),
        ("kueue.x-k8s.io/v1beta2", "ResourceFlavor"),
        ("kueue.x-k8s.io/v1beta2", "Topology"),
        ("kueue.x-k8s.io/v1beta2", "Cohort"),
        ("kueue.x-k8s.io/v1beta2", "WorkloadPriorityClass"),
    }
)

SYSTEM_NAMESPACES: frozenset[str] = frozenset(
    {
        "default",
        "kube-system",
        "kube-public",
        "kube-node-lease",
        "kueue-system",
        "jobset-system",
    }
)

SERVER_METADATA_FIELDS: tuple[str, ...] = (
    "uid",
    "resourceVersion",
    "creationTimestamp",
    "generation",
    "managedFields",
    "selfLink",
    "deletionTimestamp",
    "deletionGracePeriodSeconds",
    "finalizers",
    "ownerReferences",
)

MAX_K8S_NAME_LEN = 62  # Strictly shorter than 63 characters (< 63)


def _require_event_epoch(ev: dict[str, Any], source_file: Path, event_index: int) -> float:
    """Parse a required RFC3339 event timestamp strictly, raising ValueError with file & event index on failure."""
    raw_ts = ev.get("timestamp")
    if raw_ts is None or not str(raw_ts).strip():
        raise ValueError(f"{source_file} event #{event_index}: missing required 'timestamp' field.")
    try:
        ep = parse_rfc3339_epoch(str(raw_ts), strict=True)
    except ValueError as exc:
        raise ValueError(
            f"{source_file} event #{event_index}: invalid RFC3339 timestamp {raw_ts!r}: {exc}"
        ) from exc
    if ep is None:
        raise ValueError(f"{source_file} event #{event_index}: invalid RFC3339 timestamp {raw_ts!r}.")
    return ep


def _parse_optional_event_epoch(
    raw_ts: Any,
    source_file: Path,
    event_index: int,
    field_name: str,
) -> float | None:
    """Parse an optional RFC3339 timestamp strictly, raising ValueError when present and malformed."""
    if raw_ts is None or not str(raw_ts).strip():
        return None
    try:
        return parse_rfc3339_epoch(str(raw_ts), strict=True)
    except ValueError as exc:
        raise ValueError(
            f"{source_file} event #{event_index}: invalid RFC3339 {field_name} {raw_ts!r}: {exc}"
        ) from exc


def validate_workflow_events(workflow_events: list[dict[str, Any]], wf_file: Path) -> None:
    """Validate workflow event payloads, GVK allowlists, DNS labels, and metadata consistency."""
    for idx_wf, ev in enumerate(workflow_events):
        ev_type = str(ev.get("eventType") or "")
        pl = ev.get("payload")
        if not isinstance(pl, dict):
            raise ValueError(f"{wf_file} event #{idx_wf}: missing or non-object 'payload'.")
        kind = str(pl.get("objectKind") or "")
        if kind not in ALLOWED_WORKFLOW_KINDS:
            raise ValueError(
                f"{wf_file} event #{idx_wf}: disallowed workflow objectKind {kind!r}; "
                f"allowed kinds: {sorted(ALLOWED_WORKFLOW_KINDS)}."
            )
        wf_name = validate_dns_label(pl.get("name"), f"{wf_file} event #{idx_wf} payload.name")
        wf_orig_ns = validate_dns_label(
            pl.get("namespace") or "default", f"{wf_file} event #{idx_wf} payload.namespace"
        )
        wf_lq = str(pl.get("localQueueName") or "")
        if wf_lq:
            validate_dns_label(wf_lq, f"{wf_file} event #{idx_wf} payload.localQueueName")
        _parse_optional_event_epoch(pl.get("creationTime"), wf_file, idx_wf, "payload.creationTime")

        raw_obj = pl.get("object")
        if ev_type != "Deleted" or raw_obj is not None:
            if not isinstance(raw_obj, dict):
                raise ValueError(f"{wf_file} event #{idx_wf}: missing or non-object 'payload.object'.")
            obj_api_ver = str(raw_obj.get("apiVersion") or "")
            obj_kind = str(raw_obj.get("kind") or "")
            if obj_kind != kind:
                raise ValueError(
                    f"{wf_file} event #{idx_wf}: payload.objectKind {kind!r} does not match "
                    f"embedded object kind {obj_kind!r}."
                )
            if (obj_api_ver, obj_kind) not in ALLOWED_WORKFLOW_GVKS:
                raise ValueError(
                    f"{wf_file} event #{idx_wf}: disallowed workflow (apiVersion, kind)=({obj_api_ver!r}, {obj_kind!r}); "
                    f"allowed: {sorted(ALLOWED_WORKFLOW_GVKS)}."
                )
            meta = raw_obj.get("metadata")
            if meta is not None and not isinstance(meta, dict):
                raise ValueError(f"{wf_file} event #{idx_wf}: 'payload.object.metadata' must be a JSON object.")
            meta_dict = meta or {}
            if meta_dict.get("name"):
                meta_name = validate_dns_label(
                    meta_dict.get("name"), f"{wf_file} event #{idx_wf} payload.object.metadata.name"
                )
                if meta_name != wf_name:
                    raise ValueError(
                        f"{wf_file} event #{idx_wf}: payload.name {wf_name!r} does not match "
                        f"payload.object.metadata.name {meta_name!r}."
                    )
            if meta_dict.get("namespace"):
                meta_ns = validate_dns_label(
                    meta_dict.get("namespace"), f"{wf_file} event #{idx_wf} payload.object.metadata.namespace"
                )
                if meta_ns != wf_orig_ns:
                    raise ValueError(
                        f"{wf_file} event #{idx_wf}: payload.namespace {wf_orig_ns!r} does not match "
                        f"payload.object.metadata.namespace {meta_ns!r}."
                    )


def _clean_metadata(meta: dict[str, Any], keep_annotations: bool = True) -> dict[str, Any]:
    cleaned = json.loads(json.dumps(meta or {}))
    for k in SERVER_METADATA_FIELDS:
        cleaned.pop(k, None)
    if not keep_annotations:
        cleaned.pop("annotations", None)
    return cleaned


def extract_node_ready_status(node_obj: dict[str, Any]) -> str | None:
    """Return the Node's Ready condition status ('True', 'False', 'Unknown') if present, else None."""
    conds = ((node_obj.get("status") or {}).get("conditions")) or []
    for cond in conds:
        if isinstance(cond, dict) and cond.get("type") == "Ready" and cond.get("status"):
            return str(cond["status"])
    return None


def is_node_payload_unhealthy(node_obj: dict[str, Any]) -> tuple[bool, bool]:
    """Return (is_not_ready, has_no_execute_taint) for a scraped or synthetic Node object."""
    ready_st = extract_node_ready_status(node_obj)
    ann = ((node_obj.get("metadata") or {}).get("annotations")) or {}
    explicit_failed = str(ann.get("kueue-optimizer.gke.io/node-failed") or "").lower() == "true"
    is_not_ready = explicit_failed or (ready_st in ("False", "Unknown"))
    taints = ((node_obj.get("spec") or {}).get("taints")) or []
    has_no_exec = any(
        isinstance(t, dict) and str(t.get("effect") or "") == "NoExecute"
        for t in taints
    )
    return is_not_ready, has_no_exec


def _prepare_node_manifest(node_obj: dict[str, Any], node_idx: int) -> dict[str, Any]:
    """Prepare a scraped v1/Node manifest so KWOK initializes it with its exact scraped capacity."""
    node = json.loads(json.dumps(node_obj))
    meta = _clean_metadata(node.get("metadata") or {})
    name = str(meta.get("name") or f"kwok-node-{node_idx}")
    meta["name"] = name

    labels = dict(meta.get("labels") or {})
    labels["type"] = "kwok"
    labels.setdefault("kubernetes.io/hostname", name)
    labels.setdefault("kubernetes.io/os", "linux")
    labels.setdefault("kubernetes.io/arch", "amd64")
    labels.setdefault("kubernetes.io/role", "agent")
    labels.setdefault("node-role.kubernetes.io/agent", "")
    meta["labels"] = labels

    st = node.get("status") or {}
    alloc = dict(st.get("allocatable") or st.get("capacity") or {"cpu": "4", "memory": "16Gi", "pods": "110"})
    cap = dict(st.get("capacity") or alloc)

    is_not_ready, _has_no_exec = is_node_payload_unhealthy(node)

    annotations = dict(meta.get("annotations") or {})
    annotations["kwok.x-k8s.io/node"] = "fake"
    annotations["node.alpha.kubernetes.io/ttl"] = "0"
    if is_not_ready:
        annotations["kueue-optimizer.gke.io/node-failed"] = "true"
    else:
        annotations.pop("kueue-optimizer.gke.io/node-failed", None)
    if "cpu" in alloc:
        annotations["kueue-optimizer.gke.io/node-cpu"] = str(alloc["cpu"])
    if "memory" in alloc:
        annotations["kueue-optimizer.gke.io/node-memory"] = str(alloc["memory"])
    if "pods" in alloc:
        annotations["kueue-optimizer.gke.io/node-pods"] = str(alloc["pods"])
    accel_candidates: list[tuple[int, str, Any]] = []
    for r_key, r_val in alloc.items():
        if r_key in ("cpu", "memory", "pods", "ephemeral-storage") or str(r_key).startswith("hugepages-"):
            continue
        if parse_k8s_quantity(r_val, r_key) <= 0:
            continue
        rk_str = str(r_key)
        is_preferred_accel = rk_str.startswith(("google.com/tpu", "nvidia.com/", "amd.com/", "habana.ai/"))
        accel_candidates.append((0 if is_preferred_accel else 1, rk_str, r_val))
    if accel_candidates:
        accel_candidates.sort(key=lambda item: (item[0], item[1]))
        _, chosen_key, chosen_val = accel_candidates[0]
        annotations["kueue-optimizer.gke.io/node-accel-resource"] = chosen_key
        annotations["kueue-optimizer.gke.io/node-accel-count"] = str(chosen_val)
    meta["annotations"] = annotations

    spec = dict(node.get("spec") or {})
    if not spec.get("podCIDR"):
        octet2 = 100 + (node_idx // 256)
        octet3 = node_idx % 256
        cidr = f"10.{octet2}.{octet3}.0/24"
        spec["podCIDR"] = cidr
        spec["podCIDRs"] = [cidr]

    return {
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": meta,
        "spec": spec,
        "status": {
            "allocatable": alloc,
            "capacity": cap,
            "nodeInfo": st.get("nodeInfo")
            or {
                "architecture": "amd64",
                "kubeProxyVersion": "fake",
                "kubeletVersion": "fake",
                "operatingSystem": "linux",
            },
        },
    }


def _prepare_kueue_or_ns_doc(
    obj: dict[str, Any],
    override_namespace: str | None = None,
    override_queueing_strategy: str | None = None,
    override_cq_fair_sharing_preemption: bool = False,
) -> dict[str, Any]:
    doc = json.loads(json.dumps(obj))
    doc.pop("status", None)
    meta = _clean_metadata(doc.get("metadata") or {})
    kind = str(doc.get("kind") or "")
    if override_namespace and kind == "LocalQueue":
        meta["namespace"] = override_namespace
    doc["metadata"] = meta
    if kind == "Namespace":
        doc.pop("spec", None)
    elif kind == "ClusterQueue":
        cq_spec = doc.setdefault("spec", {})
        stripped_checks: list[str] = []
        raw_ac = cq_spec.pop("admissionChecks", None)
        if isinstance(raw_ac, list):
            stripped_checks.extend(str(x) for x in raw_ac if x)
        raw_acs = cq_spec.pop("admissionCheckStrategy", None)
        if isinstance(raw_acs, dict):
            for entry in raw_acs.get("admissionChecks") or []:
                if isinstance(entry, dict) and entry.get("name"):
                    stripped_checks.append(str(entry["name"]))
        if stripped_checks:
            ann = dict(meta.get("annotations") or {})
            ann["kueue-optimizer.gke.io/stripped-admission-checks"] = ",".join(sorted(set(stripped_checks)))
            meta["annotations"] = ann
        if override_queueing_strategy:
            cq_spec["queueingStrategy"] = override_queueing_strategy
        if override_cq_fair_sharing_preemption:
            fs_spec = cq_spec.setdefault("fairSharing", {})
            if fs_spec.get("weight") is None:
                fs_spec["weight"] = 1
            pre_spec = cq_spec.setdefault("preemption", {})
            if pre_spec.get("withinClusterQueue") in (None, "", "Never"):
                pre_spec["withinClusterQueue"] = "LowerPriority"
            if pre_spec.get("reclaimWithinCohort") in (None, "", "Never"):
                pre_spec["reclaimWithinCohort"] = "Any"
            pre_spec.pop("borrowWithinCohort", None)
    return doc


def _extract_existing_pod_exec_seconds(obj: dict[str, Any]) -> float | None:
    """Extract kueue-optimizer.gke.io/pod-execution-time-seconds from an object or its pod templates if present."""
    dur_keys = ("kueue-optimizer.gke.io/pod-execution-time-seconds", "kueue-optimizer.gke.io/real-duration-sec")
    top = parse_positive_duration_annotation((obj.get("metadata") or {}).get("annotations"), *dur_keys)
    if top is not None:
        return top

    spec = obj.get("spec") or {}
    for rj in spec.get("replicatedJobs") or []:
        pod_ann = (((rj.get("template") or {}).get("spec") or {}).get("template") or {}).get(
            "metadata", {}
        ).get("annotations")
        val = parse_positive_duration_annotation(pod_ann, *dur_keys)
        if val is not None:
            return val

    lwt = spec.get("leaderWorkerTemplate") or {}
    if isinstance(lwt, dict):
        for tpl_key in ("leaderTemplate", "workerTemplate"):
            tpl_ann = ((lwt.get(tpl_key) or {}).get("metadata") or {}).get("annotations")
            val = parse_positive_duration_annotation(tpl_ann, *dur_keys)
            if val is not None:
                return val

    for ray_root in (spec, spec.get("rayClusterSpec") or {}):
        if isinstance(ray_root, dict):
            sub_ann = ((ray_root.get("submitterPodTemplate") or {}).get("metadata") or {}).get("annotations")
            val = parse_positive_duration_annotation(sub_ann, *dur_keys)
            if val is not None:
                return val
            head_ann = (((ray_root.get("headGroupSpec") or {}).get("template") or {}).get("metadata") or {}).get(
                "annotations"
            )
            val = parse_positive_duration_annotation(head_ann, *dur_keys)
            if val is not None:
                return val
            for wg in ray_root.get("workerGroupSpecs") or []:
                if isinstance(wg, dict):
                    wg_ann = (((wg.get("template") or {}).get("metadata")) or {}).get("annotations")
                    val = parse_positive_duration_annotation(wg_ann, *dur_keys)
                    if val is not None:
                        return val

    tpl_ann = ((spec.get("template") or {}).get("metadata") or {}).get("annotations")
    return parse_positive_duration_annotation(tpl_ann, *dur_keys)


def _extract_workflow_true_duration_and_multiplier(raw_obj: dict[str, Any] | None) -> tuple[float | None, float | None]:
    """Extract optional true-execution-time-seconds and missing-duration-multiplier annotations from a workflow."""
    if not isinstance(raw_obj, dict):
        return None, None
    ann = (raw_obj.get("metadata") or {}).get("annotations") or {}
    true_dur = parse_positive_duration_annotation(
        ann,
        "kueue-optimizer.gke.io/true-execution-time-seconds",
        "kueue-optimizer.gke.io/expected-execution-time-seconds",
    )
    mult: float | None = None
    raw_mult = ann.get("kueue-optimizer.gke.io/missing-duration-multiplier")
    if raw_mult is not None:
        try:
            mv = float(str(raw_mult).strip())
            if mv > 1.0:
                mult = mv
        except (TypeError, ValueError):
            pass
    return true_dur, mult


def _max_workflow_name_len(obj: dict[str, Any], kind: str) -> int:
    """Return the maximum allowed workflow name length so all generated child/workload names are < 63 chars."""
    spec = obj.get("spec") or {}
    if kind == "JobSet":
        max_extra = 13  # len("jobset-") + len("-abcde")
        for idx, rj in enumerate(spec.get("replicatedJobs") or []):
            rj_name = str(rj.get("name") or f"ps-{idx}")
            replicas = max(1, int(rj.get("replicas") or 1))
            j_spec = ((rj.get("template") or {}).get("spec")) or {}
            completions = max(1, int(j_spec.get("completions") or j_spec.get("parallelism") or 1))
            pod_suffix = f"-{rj_name}-{replicas - 1}-{completions - 1}-abcde"
            if len(pod_suffix) > max_extra:
                max_extra = len(pod_suffix)
        return max(10, MAX_K8S_NAME_LEN - max_extra)
    if kind == "Job":
        completions = max(1, int(spec.get("completions") or spec.get("parallelism") or 1))
        max_extra = max(10, len(f"-{completions - 1}-abcde"))
        return max(10, MAX_K8S_NAME_LEN - max_extra)
    if kind in ("LeaderWorkerSet", "RayJob", "RayCluster", "Deployment"):
        return 45
    return MAX_K8S_NAME_LEN


def _translate_unsupported_workflow_to_jobset(
    obj: dict[str, Any],
    kind: str,
    pause_image: str,
) -> dict[str, Any]:
    """Translate a LeaderWorkerSet, RayJob, or RayCluster into an equivalent gang-scheduled JobSet."""
    spec = obj.get("spec") or {}
    meta = dict(obj.get("metadata") or {})
    ann = dict(meta.get("annotations") or {})
    ann["kueue-optimizer.gke.io/translated-from-kind"] = kind
    meta["annotations"] = ann

    def _ensure_pod_tpl(raw_tpl: dict[str, Any] | None) -> dict[str, Any]:
        tpl = json.loads(json.dumps(raw_tpl or {}))
        p_spec = tpl.setdefault("spec", {})
        if not p_spec.get("containers"):
            p_spec["containers"] = [{"name": "pause", "image": pause_image}]
        return tpl

    replicated_jobs: list[dict[str, Any]] = []
    if kind == "LeaderWorkerSet":
        replicas = max(1, int(spec.get("replicas") or 1))
        lwt = spec.get("leaderWorkerTemplate") or {}
        size = max(1, int(lwt.get("size") or 1))
        has_leader = isinstance(lwt.get("leaderTemplate"), dict) and bool(lwt.get("leaderTemplate"))
        worker_tpl = _ensure_pod_tpl(lwt.get("workerTemplate") or lwt.get("leaderTemplate"))
        if has_leader and size > 1:
            leader_tpl = _ensure_pod_tpl(lwt.get("leaderTemplate"))
            replicated_jobs.append(
                {
                    "name": "leader",
                    "replicas": replicas,
                    "template": {
                        "spec": {
                            "parallelism": 1,
                            "completions": 1,
                            "template": leader_tpl,
                        }
                    },
                }
            )
            replicated_jobs.append(
                {
                    "name": "worker",
                    "replicas": replicas,
                    "template": {
                        "spec": {
                            "parallelism": size - 1,
                            "completions": size - 1,
                            "template": worker_tpl,
                        }
                    },
                }
            )
        else:
            only_tpl = _ensure_pod_tpl(lwt.get("leaderTemplate") if has_leader else worker_tpl)
            replicated_jobs.append(
                {
                    "name": "worker",
                    "replicas": replicas,
                    "template": {
                        "spec": {
                            "parallelism": size,
                            "completions": size,
                            "template": only_tpl,
                        }
                    },
                }
            )
    elif kind in ("RayJob", "RayCluster"):
        rcs = (spec.get("rayClusterSpec") or {}) if kind == "RayJob" else spec
        if isinstance(rcs, dict):
            head_grp = rcs.get("headGroupSpec")
            if isinstance(head_grp, dict) and head_grp:
                replicated_jobs.append(
                    {
                        "name": "head",
                        "replicas": 1,
                        "template": {
                            "spec": {
                                "parallelism": 1,
                                "completions": 1,
                                "template": _ensure_pod_tpl(head_grp.get("template")),
                            }
                        },
                    }
                )
            seen_rj_names = {"head", "submitter"}
            for idx, wg in enumerate(rcs.get("workerGroupSpecs") or []):
                if not isinstance(wg, dict):
                    continue
                raw_wg_name = str(wg.get("groupName") or f"wg{idx}")
                wg_name = sanitize_k8s_name(raw_wg_name, max_len=12) or f"wg{idx}"
                if wg_name in seen_rj_names:
                    wg_name = f"wg{idx}"
                seen_rj_names.add(wg_name)
                wg_pods = max(1, int(wg.get("replicas") or 1)) * max(1, int(wg.get("numOfHosts") or 1))
                replicated_jobs.append(
                    {
                        "name": wg_name,
                        "replicas": 1,
                        "template": {
                            "spec": {
                                "parallelism": wg_pods,
                                "completions": wg_pods,
                                "template": _ensure_pod_tpl(wg.get("template")),
                            }
                        },
                    }
                )
        sub_mode = str(spec.get("submissionMode") or "")
        sub_tpl = spec.get("submitterPodTemplate")
        if kind == "RayJob" and isinstance(sub_tpl, dict) and (sub_mode == "K8sJobMode" or not replicated_jobs):
            replicated_jobs.append(
                {
                    "name": "submitter",
                    "replicas": 1,
                    "template": {
                        "spec": {
                            "parallelism": 1,
                            "completions": 1,
                            "template": _ensure_pod_tpl(sub_tpl),
                        }
                    },
                }
            )

    if not replicated_jobs:
        replicated_jobs.append(
            {
                "name": "main",
                "replicas": 1,
                "template": {
                    "spec": {
                        "parallelism": 1,
                        "completions": 1,
                        "template": _ensure_pod_tpl(None),
                    }
                },
            }
        )

    js_doc: dict[str, Any] = {
        "apiVersion": "jobset.x-k8s.io/v1alpha2",
        "kind": "JobSet",
        "metadata": meta,
        "spec": {
            "suspend": True,
            "replicatedJobs": replicated_jobs,
        },
    }
    max_js_len = _max_workflow_name_len(js_doc, "JobSet")
    cur_name = str(meta.get("name") or "workflow")
    if len(cur_name) > max_js_len or not cur_name[0].isalpha():
        orig = ann.get("kueue-optimizer.gke.io/original-name") or cur_name
        ann["kueue-optimizer.gke.io/original-name"] = str(orig)
        base = cur_name[2:] if cur_name.startswith("w-") else cur_name
        meta["name"] = f"w-{sanitize_k8s_name(base, max_len=max_js_len - 2)}"
    return js_doc


def _inject_pod_execution_time_and_sanitize_workflow(
    raw_obj: dict[str, Any],
    exec_str: str,
    raw_duration_sec: float,
    compressed_duration_sec: float,
    pause_image_override: str | None = None,
    override_namespace: str | None = None,
    extra_pod_annotations: dict[str, str] | None = None,
    speedup: float = 1.0,
    *,
    scale_by_waves: bool = False,
    min_pod_duration_sec: float = 0.05,
    translate_unsupported_crds: bool = True,
) -> tuple[dict[str, Any], int]:
    """Prepare a workflow object for submission and inject pod execution duration into all pod templates.

    Returns (prepared_manifest, total_pod_count).
    """
    eff_speedup = max(1e-6, float(speedup))
    obj = json.loads(json.dumps(raw_obj))
    obj.pop("status", None)
    kind = str(obj.get("kind") or "")

    meta = _clean_metadata(obj.get("metadata") or {})
    orig_name = str(meta.get("name") or "workflow")
    max_name_len = _max_workflow_name_len(obj, kind)
    if len(orig_name) > max_name_len or not orig_name[0].isalpha():
        base = orig_name[2:] if orig_name.startswith("w-") else orig_name
        meta["name"] = f"w-{sanitize_k8s_name(base, max_len=max_name_len - 2)}"
    if override_namespace and meta.get("namespace"):
        meta["namespace"] = override_namespace
    ann = dict(meta.get("annotations") or {})
    if meta.get("name") != orig_name:
        ann["kueue-optimizer.gke.io/original-name"] = orig_name
    if kind == "Deployment":
        ann.pop("kueue-optimizer.gke.io/pod-execution-time-seconds", None)
        ann["kueue-optimizer.gke.io/long-running-pod"] = "true"
        ann["kueue-optimizer.gke.io/infinite-duration"] = "true"
    else:
        ann["kueue-optimizer.gke.io/pod-execution-time-seconds"] = exec_str
        ann["kueue-optimizer.gke.io/real-duration-sec"] = f"{raw_duration_sec:.2f}"
        ann["kueue-optimizer.gke.io/compressed-duration-sec"] = f"{compressed_duration_sec:.3f}"

    def _scale_startup_delay(src_ann: dict[str, Any]) -> str | None:
        raw_delay = parse_positive_duration_annotation(src_ann, "kueue-optimizer.gke.io/pod-startup-delay-seconds")
        if raw_delay is None or raw_delay <= 0:
            return None
        scaled = max(0.05, raw_delay / eff_speedup)
        if abs(scaled - round(scaled)) < 1e-3 and round(scaled) >= 1:
            return str(int(round(scaled)))
        return f"{max(50, int(round(scaled * 1000)))}ms"

    top_scaled_startup = _scale_startup_delay(ann)
    if top_scaled_startup is not None:
        ann["kueue-optimizer.gke.io/pod-startup-delay-seconds"] = top_scaled_startup
    top_never_ready = str(ann.get("kueue-optimizer.gke.io/pod-never-ready") or "").lower() == "true"

    if extra_pod_annotations:
        ann.update({str(k): str(v) for k, v in extra_pod_annotations.items()})
    meta["annotations"] = ann
    labels = dict(meta.get("labels") or {})
    labels["kueue-optimizer.gke.io/testrunner"] = "true"
    meta["labels"] = labels
    obj["metadata"] = meta

    if translate_unsupported_crds and kind in ("LeaderWorkerSet", "RayJob", "RayCluster"):
        obj = _translate_unsupported_workflow_to_jobset(
            obj,
            kind=kind,
            pause_image=pause_image_override or DEFAULT_PAUSE_IMAGE,
        )
        kind = "JobSet"
        meta = obj["metadata"]
        ann = meta["annotations"]

    spec = obj.setdefault("spec", {})

    def _patch_pod_spec(pod_spec: dict[str, Any]) -> None:
        if not isinstance(pod_spec, dict):
            return
        pod_spec.setdefault("restartPolicy", "Never")
        sched_name = str(pod_spec.get("schedulerName") or "").strip()
        if sched_name and sched_name != "default-scheduler":
            pod_spec.pop("schedulerName", None)
        if pod_spec.get("priorityClassName"):
            pod_spec.pop("priority", None)
        overhead_blk = pod_spec.get("overhead")
        if isinstance(overhead_blk, dict) and overhead_blk and not pod_spec.get("runtimeClassName"):
            pod_spec["runtimeClassName"] = "kueue-optimizer-overhead-rc"
        tolerations = list(pod_spec.get("tolerations") or [])
        if not any(t.get("key") == "kwok.x-k8s.io/node" for t in tolerations if isinstance(t, dict)):
            tolerations.append(
                {
                    "key": "kwok.x-k8s.io/node",
                    "operator": "Equal",
                    "value": "fake",
                    "effect": "NoSchedule",
                }
            )
        pod_spec["tolerations"] = tolerations
        for c_list_key in ("containers", "initContainers"):
            for c in pod_spec.get(c_list_key) or []:
                if isinstance(c, dict):
                    if pause_image_override:
                        c["image"] = pause_image_override
                    elif not c.get("image"):
                        c["image"] = DEFAULT_PAUSE_IMAGE
                    res_blk = c.get("resources")
                    if isinstance(res_blk, dict):
                        reqs_blk = res_blk.get("requests")
                        if isinstance(reqs_blk, dict):
                            for rk, rv in reqs_blk.items():
                                rk_s = str(rk)
                                if rk_s not in ("cpu", "memory", "ephemeral-storage", "pods") and (
                                    "/" in rk_s or rk_s.startswith("hugepages-")
                                ):
                                    lims_blk = res_blk.setdefault("limits", {})
                                    if isinstance(lims_blk, dict) and rk_s not in lims_blk:
                                        lims_blk[rk_s] = rv

    def _patch_pod_template(tpl: dict[str, Any], override_exec_str: str | None = None) -> None:
        if not isinstance(tpl, dict):
            return
        t_meta = tpl.setdefault("metadata", {})
        t_ann = dict(t_meta.get("annotations") or {})
        t_ann["kueue-optimizer.gke.io/pod-execution-time-seconds"] = override_exec_str or exec_str
        tpl_scaled_delay = _scale_startup_delay(t_ann) or top_scaled_startup
        if tpl_scaled_delay is not None:
            t_ann["kueue-optimizer.gke.io/pod-startup-delay-seconds"] = tpl_scaled_delay
        if top_never_ready:
            t_ann["kueue-optimizer.gke.io/pod-never-ready"] = "true"
        if extra_pod_annotations:
            t_ann.update({str(k): str(v) for k, v in extra_pod_annotations.items()})
        t_meta["annotations"] = t_ann
        t_labels = dict(t_meta.get("labels") or {})
        for r_lbl in RESERVED_POD_LABELS:
            t_labels.pop(r_lbl, None)
        if t_labels:
            t_meta["labels"] = t_labels
        elif "labels" in t_meta:
            t_meta.pop("labels", None)
        _patch_pod_spec(tpl.setdefault("spec", {}))

    total_pods = 0

    if kind == "JobSet":
        spec["suspend"] = True
        for rj in spec.get("replicatedJobs") or []:
            replicas = max(1, int(rj.get("replicas") or 1))
            job_tpl_spec = (rj.setdefault("template", {})).setdefault("spec", {})
            parallelism = max(1, int(job_tpl_spec.get("parallelism") or job_tpl_spec.get("completions") or 1))
            completions = max(parallelism, int(job_tpl_spec.get("completions") or parallelism))
            rj_waves = math.ceil(completions / parallelism)
            rj_exec_str = exec_str
            if scale_by_waves and rj_waves > 1:
                per_wave_comp = max(min_pod_duration_sec, compressed_duration_sec / rj_waves)
                rj_exec_str = format_pod_execution_seconds(per_wave_comp, min_seconds=min_pod_duration_sec)
            total_pods += replicas * parallelism
            _patch_pod_template(job_tpl_spec.setdefault("template", {}), override_exec_str=rj_exec_str)

    elif kind == "Job":
        spec["suspend"] = True
        # Remove controller-generated UID selectors from scraped Jobs
        spec.pop("selector", None)
        spec.pop("manualSelector", None)
        parallelism = max(1, int(spec.get("parallelism") or spec.get("completions") or 1))
        completions = max(parallelism, int(spec.get("completions") or parallelism))
        waves = math.ceil(completions / parallelism)
        job_exec_str = exec_str
        if scale_by_waves and waves > 1:
            per_wave_comp = max(min_pod_duration_sec, compressed_duration_sec / waves)
            job_exec_str = format_pod_execution_seconds(per_wave_comp, min_seconds=min_pod_duration_sec)
            ann["kueue-optimizer.gke.io/pod-execution-time-seconds"] = job_exec_str
            ann["kueue-optimizer.gke.io/pod-waves"] = str(waves)
        total_pods += parallelism
        _patch_pod_template(spec.setdefault("template", {}), override_exec_str=job_exec_str)

    elif kind == "Deployment":
        raw_replicas = spec.get("replicas")
        replicas = max(0, int(raw_replicas)) if raw_replicas is not None else 1
        spec["replicas"] = replicas
        spec["paused"] = bool(spec.get("paused", False))
        total_pods += replicas
        tpl = spec.setdefault("template", {})
        t_meta = tpl.setdefault("metadata", {})
        t_ann = dict(t_meta.get("annotations") or {})
        t_ann.pop("kueue-optimizer.gke.io/pod-execution-time-seconds", None)
        t_ann["kueue-optimizer.gke.io/long-running-pod"] = "true"
        t_ann["kueue-optimizer.gke.io/infinite-duration"] = "true"
        tpl_scaled_delay = _scale_startup_delay(t_ann) or top_scaled_startup
        if tpl_scaled_delay is not None:
            t_ann["kueue-optimizer.gke.io/pod-startup-delay-seconds"] = tpl_scaled_delay
        if top_never_ready:
            t_ann["kueue-optimizer.gke.io/pod-never-ready"] = "true"
        if extra_pod_annotations:
            t_ann.update({str(k): str(v) for k, v in extra_pod_annotations.items()})
        t_meta["annotations"] = t_ann
        t_labels = dict(t_meta.get("labels") or {})
        for r_lbl in RESERVED_POD_LABELS:
            t_labels.pop(r_lbl, None)
        if t_labels:
            t_meta["labels"] = t_labels
        elif "labels" in t_meta:
            t_meta.pop("labels", None)
        tpl_spec = tpl.setdefault("spec", {})
        _patch_pod_spec(tpl_spec)
        tpl_spec["restartPolicy"] = "Always"

    elif kind == "LeaderWorkerSet":
        replicas = max(1, int(spec.get("replicas") or 1))
        lwt = spec.setdefault("leaderWorkerTemplate", {})
        size = max(1, int(lwt.get("size") or 1))
        total_pods += replicas * size
        if "leaderTemplate" in lwt and isinstance(lwt["leaderTemplate"], dict):
            _patch_pod_template(lwt["leaderTemplate"])
        if "workerTemplate" in lwt and isinstance(lwt["workerTemplate"], dict):
            _patch_pod_template(lwt["workerTemplate"])

    elif kind == "RayJob":
        spec["suspend"] = True
        if "submitterPodTemplate" in spec and isinstance(spec["submitterPodTemplate"], dict):
            _patch_pod_template(spec["submitterPodTemplate"])
            total_pods += 1
        rcs = spec.get("rayClusterSpec") or {}
        if isinstance(rcs, dict):
            if "headGroupSpec" in rcs and isinstance(rcs["headGroupSpec"], dict):
                _patch_pod_template(rcs["headGroupSpec"].setdefault("template", {}))
                total_pods += 1
            for wg in rcs.get("workerGroupSpecs") or []:
                if isinstance(wg, dict):
                    total_pods += max(1, int(wg.get("replicas") or 1))
                    _patch_pod_template(wg.setdefault("template", {}))

    elif kind == "RayCluster":
        spec["suspend"] = True
        if "headGroupSpec" in spec and isinstance(spec["headGroupSpec"], dict):
            _patch_pod_template(spec["headGroupSpec"].setdefault("template", {}))
            total_pods += 1
        for wg in spec.get("workerGroupSpecs") or []:
            if isinstance(wg, dict):
                total_pods += max(1, int(wg.get("replicas") or 1))
                _patch_pod_template(wg.setdefault("template", {}))

    elif kind == "Pod":
        total_pods += 1
        spec.pop("nodeName", None)
        _patch_pod_spec(spec)

    return obj, (total_pods if kind == "Deployment" else max(1, total_pods))
