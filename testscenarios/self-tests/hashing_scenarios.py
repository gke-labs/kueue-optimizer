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

"""Anonymization & Sensitive-Data Hashing (`--hash-names`) self-test scenarios (`st91`–`st95`).

Covers Category 19 (`st91`–`st95`):
- `st91`: Anonymized (`--hash-names`) batch `JobSet` replay with HMAC-SHA256 `h-<32hex>`
  workflow names, independent hashed `workloadName`s, hashed sensitive label values, and
  plaintext infrastructure (paired A/B twin of `st92`).
- `st92`: Plaintext batch `JobSet` control twin of `st91` carrying sensitive organizational
  labels (`owner`, `team`, `project`, `corp.example.com/cost-center`) to prove zero metric
  drift against `st91` and serve as a live `--scrape-hash-names` capture target.
- `st93`: Anonymized (`--hash-names`) `t0` hybrid cluster containing both pre-admitted hashed
  Kueue `JobSet`s (with linked hashed `Pod`s in `infra.json`) and genuine hashed non-Kueue
  unmanaged `infra-pods`, verifying accurate `t0` separation and quota enforcement.
- `st94`: Anonymized (`--hash-names`) `Deployment` lifecycle with hashed Deployment name,
  matching hashed pod selectors, distinct HMAC-salted `templateHash` (`th-<16hex>`) rolling
  `Upgrade` (`maxUnavailable: 1, maxSurge: 0`), `ScaleDown 4 -> 2 -> 0`, and hashed batch `Job`.
- `st95`: Anonymized (`--hash-names`) 7-level TAS cluster combining annotation-only `t0`
  `PodGroup`s (`kueue.x-k8s.io/pod-group-name: h-<32hex>`) and `LowerPriority` preemption of
  hashed TAS workloads.
"""

from __future__ import annotations

import hashlib
import hmac
from typing import Any, Callable


DEFAULT_NS = "st-workloads"
DEFAULT_FLAVOR = "st-tpu-flavor"
DEFAULT_TOPOLOGY = "st-slice-topology"


POD_EXEC_ANNOTATION = "kueue-optimizer.gke.io/pod-execution-time-seconds"


def _drop_pod_exec_annotation(node: Any) -> None:
    """Remove the replay-only pod duration annotation from every `annotations` map below `node`."""
    if isinstance(node, dict):
        anns = node.get("annotations")
        if isinstance(anns, dict) and POD_EXEC_ANNOTATION in anns:
            del anns[POD_EXEC_ANNOTATION]
            if not anns:
                del node["annotations"]
        for child in node.values():
            _drop_pod_exec_annotation(child)
    elif isinstance(node, list):
        for child in node:
            _drop_pod_exec_annotation(child)


def _hmac_name(salt: bytes, raw_val: str) -> str:
    """Match Go `hmacSensitiveDataHasher.HashName`: `'h-' + 32 lowercase hex chars`."""
    digest = hmac.new(salt, raw_val.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"h-{digest[:32]}"


def _hmac_template_hash(salt: bytes, template_tag: str) -> str:
    """Match Go `ComputePodTemplateRevisionHash`: `'th-' + 16 lowercase hex chars`."""
    mac = hmac.new(salt, b"deployment-template:" + template_tag.encode("utf-8"), hashlib.sha256)
    return f"th-{mac.hexdigest()[:16]}"


def _make_batch_job_wf_and_exec(
    *,
    det_uid_fn: Callable[[str], str],
    fmt_ts_fn: Callable[[float], str],
    name: str,
    wl_name: str,
    local_queue: str,
    priority_class: str,
    priority: int,
    tpu_per_pod: int,
    parallelism: int,
    exec_sec: float,
    arrival_sec: float,
    admit_sec: float,
    extra_labels: dict[str, str] | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    labels = {
        "kueue.x-k8s.io/queue-name": local_queue,
        "kueue.x-k8s.io/priority-class": priority_class,
    }
    if extra_labels:
        labels.update(extra_labels)
    job_obj = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": name,
            "namespace": DEFAULT_NS,
            "uid": det_uid_fn(f"job:{name}"),
            "creationTimestamp": fmt_ts_fn(arrival_sec),
            "labels": labels,
        },
        "spec": {
            "suspend": True,
            "parallelism": parallelism,
            "completions": parallelism,
            "template": {
                "metadata": {"labels": {"app": name}},
                "spec": {
                    "restartPolicy": "Never",
                    "containers": [
                        {
                            "name": "worker",
                            "image": "registry.k8s.io/pause:3.10",
                            "resources": {
                                "requests": {
                                    "cpu": str(tpu_per_pod * 2),
                                    "memory": f"{tpu_per_pod * 8}Gi",
                                    "google.com/tpu": str(tpu_per_pod),
                                },
                                "limits": {
                                    "cpu": str(tpu_per_pod * 2),
                                    "memory": f"{tpu_per_pod * 8}Gi",
                                    "google.com/tpu": str(tpu_per_pod),
                                },
                            },
                        }
                    ],
                },
            },
        },
    }
    wf_ev = {
        "timestamp": fmt_ts_fn(arrival_sec),
        "eventType": "New",
        "workloadState": "NotAdmitted",
        "payload": {
            "objectKind": "Job",
            "name": name,
            "namespace": DEFAULT_NS,
            "creationTime": fmt_ts_fn(arrival_sec),
            "localQueueName": local_queue,
            "object": job_obj,
        },
    }
    exec_evs = [
        {
            "timestamp": fmt_ts_fn(admit_sec),
            "eventType": "Admitted",
            "workloadState": "Admitted",
            "payload": {
                "workloadName": wl_name,
                "namespace": DEFAULT_NS,
                "ownerKind": "Job",
                "ownerName": name,
                "localQueueName": local_queue,
                "clusterQueueName": "cq-hash-dep",
                "priority": priority,
                "priorityClassName": priority_class,
                "admittedTime": fmt_ts_fn(admit_sec),
            },
        },
        {
            "timestamp": fmt_ts_fn(admit_sec + exec_sec),
            "eventType": "FullExecution",
            "workloadState": "Admitted",
            "payload": {
                "workloadName": wl_name,
                "namespace": DEFAULT_NS,
                "ownerKind": "Job",
                "ownerName": name,
                "localQueueName": local_queue,
                "clusterQueueName": "cq-hash-dep",
                "priority": priority,
                "priorityClassName": priority_class,
                "admittedTime": fmt_ts_fn(admit_sec),
                "endTime": fmt_ts_fn(admit_sec + exec_sec),
                "executionDurationSeconds": float(exec_sec),
                "executionDuration": f"{int(exec_sec)}s",
                "executionType": "Full",
            },
        },
    ]
    return wf_ev, exec_evs


def _make_hashed_deployment_obj(
    *,
    det_uid_fn: Callable[[str], str],
    fmt_ts_fn: Callable[[float], str],
    name: str,
    selector_val: str,
    local_queue: str,
    priority_class: str,
    replicas: int,
    tpu_per_pod: int,
    template_hash: str,
    sensitive_labels: dict[str, str],
) -> dict[str, Any]:
    dep_labels = {
        "kueue.x-k8s.io/queue-name": local_queue,
        "kueue.x-k8s.io/priority-class": priority_class,
        **sensitive_labels,
    }
    tpl_labels = {"app": selector_val, **sensitive_labels}
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": name,
            "namespace": DEFAULT_NS,
            "uid": det_uid_fn(f"deploy:{name}"),
            "creationTimestamp": fmt_ts_fn(0.0),
            "labels": dep_labels,
        },
        "spec": {
            "replicas": replicas,
            "paused": False,
            "strategy": {
                "type": "RollingUpdate",
                "rollingUpdate": {"maxUnavailable": 1, "maxSurge": 0},
            },
            "selector": {"matchLabels": {"app": selector_val}},
            "template": {
                "metadata": {
                    "labels": tpl_labels,
                    "annotations": {
                        "kueue-optimizer.gke.io/template-hash": template_hash,
                    },
                },
                "spec": {
                    "restartPolicy": "Always",
                    "containers": [
                        {
                            "name": "server",
                            "image": "registry.k8s.io/pause:3.10",
                            "resources": {
                                "requests": {
                                    "cpu": str(tpu_per_pod * 2),
                                    "memory": f"{tpu_per_pod * 8}Gi",
                                    "google.com/tpu": str(tpu_per_pod),
                                },
                                "limits": {
                                    "cpu": str(tpu_per_pod * 2),
                                    "memory": f"{tpu_per_pod * 8}Gi",
                                    "google.com/tpu": str(tpu_per_pod),
                                },
                            },
                        }
                    ],
                },
            },
        },
    }


def _make_t0_pod_infra_event(
    *,
    fmt_ts_fn: Callable[[float], str],
    pod_name: str,
    namespace: str,
    placement: str,
    tpu_per_pod: int,
    workload_name: str = "",
    owner_kind: str = "",
    owner_name: str = "",
    pod_set_name: str = "",
    is_tas: bool = False,
) -> dict[str, Any]:
    pl: dict[str, Any] = {
        "apiVersion": "v1",
        "kind": "Pod",
        "name": pod_name,
        "namespace": namespace,
        "placement": placement,
        "resourceRequest": {
            "cpu": str(tpu_per_pod * 2),
            "memory": f"{tpu_per_pod * 8}Gi",
            "google.com/tpu": str(tpu_per_pod),
        },
        "schedulingTime": fmt_ts_fn(0.0),
        "firstNoticeTime": fmt_ts_fn(0.0),
    }
    if workload_name:
        pl["workloadName"] = workload_name
    if owner_kind:
        pl["ownerKind"] = owner_kind
    if owner_name:
        pl["ownerName"] = owner_name
    if pod_set_name:
        pl["podSetName"] = pod_set_name
    if is_tas:
        pl["isTAS"] = True
    return {
        "timestamp": fmt_ts_fn(0.0),
        "eventType": "New",
        "payload": pl,
    }


def build_hashing_5_scenarios(
    *,
    scenario_spec_cls: Any,
    det_uid_fn: Callable[[str], str],
    fmt_ts_fn: Callable[[float], str],
    make_infra_event_fn: Callable[..., dict[str, Any]],
    make_node_fn: Callable[..., dict[str, Any]],
    make_namespace_fn: Callable[..., dict[str, Any]],
    make_topology_fn: Callable[..., dict[str, Any]],
    make_flavor_fn: Callable[..., dict[str, Any]],
    make_priority_class_fn: Callable[..., dict[str, Any]],
    make_cluster_queue_fn: Callable[..., dict[str, Any]],
    make_local_queue_fn: Callable[..., dict[str, Any]],
    make_jobset_and_exec_events_fn: Callable[..., tuple[dict[str, Any], list[dict[str, Any]]]],
) -> list[Any]:
    """Build Category 19 (`st91`–`st95`) anonymization & sensitive-data hashing scenarios."""
    scenarios: list[Any] = []
    cat = "19. Anonymization & Sensitive-Data Hashing (--hash-names)"

    def _make_scraped_jobset(**kwargs: Any) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        """Build a JobSet as `cluster-scraper` records it, without the replay-only duration annotation.

        The duration of these workflows then comes only from `execution-time.json`, so
        `--missing-duration-policy error` fails when the loader cannot link a hashed owner name.
        """
        wf_ev, ex_list = make_jobset_and_exec_events_fn(**kwargs)
        _drop_pod_exec_annotation(wf_ev)
        return wf_ev, ex_list

    # ------------------------------------------------------------------
    # st91 (Anonymized) & st92 (Plaintext Control): Batch JobSet A/B Pair
    # ------------------------------------------------------------------
    salt_91 = b"st91-kueue-optimizer-hmac-salt-v1"
    raw_sensitive_labels = {
        "owner": "alice-researcher",
        "team": "ml-core",
        "project": "secret-llm-v4",
        "corp.example.com/cost-center": "cc-999",
    }
    hashed_sensitive_labels = {k: _hmac_name(salt_91, v) for k, v in raw_sensitive_labels.items()}

    for is_hashed in (True, False):
        sc_id = "st91" if is_hashed else "st92"
        slug = "hash-names-jobset-batch-anonymized-replay" if is_hashed else "hash-names-jobset-batch-plaintext-control"
        inf: list[dict[str, Any]] = [make_infra_event_fn(make_namespace_fn(DEFAULT_NS))]
        for i in range(4):
            inf.append(make_infra_event_fn(make_node_fn(i, tpu_per_node=4, cpu_per_node=16, mem_gi_per_node=64)))
        inf.append(make_infra_event_fn(make_flavor_fn(DEFAULT_FLAVOR)))
        inf.append(make_infra_event_fn(make_priority_class_fn("p-std", 500)))
        inf.append(
            make_infra_event_fn(
                make_cluster_queue_fn(
                    "cq-hash-batch",
                    tpu_quota=16,
                    cpu_quota=64,
                    mem_gi_quota=256,
                    queueing_strategy="StrictFIFO",
                    within_cq="Never",
                    reclaim_within_cohort="Never",
                )
            )
        )
        inf.append(make_infra_event_fn(make_local_queue_fn("lq-hash-batch", "cq-hash-batch", DEFAULT_NS)))

        wfs: list[dict[str, Any]] = []
        exs: list[dict[str, Any]] = []
        for idx in range(8):
            raw_js_name = f"batch-train-{idx:02d}"
            raw_wl_name = f"jobset-{raw_js_name}-wl"
            js_name = _hmac_name(salt_91, raw_js_name) if is_hashed else raw_js_name
            wl_name = _hmac_name(salt_91, raw_wl_name) if is_hashed else raw_wl_name
            wf_ev, ex_list = _make_scraped_jobset(
                name=js_name,
                arrival_sec=float(idx) * 0.1,
                duration_sec=10.0,
                local_queue="lq-hash-batch",
                cluster_queue="cq-hash-batch",
                num_pods=1,
                tpu_per_pod=4,
                priority_class="p-std",
            )
            # Attach realistic sensitive organizational labels (hashed in st91, plaintext in st92)
            # and set independent hashed workloadName in execution-time.json (matching --hash-names).
            lbls_to_add = hashed_sensitive_labels if is_hashed else raw_sensitive_labels
            wf_ev["payload"]["object"]["metadata"]["labels"].update(lbls_to_add)
            for ex in ex_list:
                ex["payload"]["workloadName"] = wl_name
            wfs.append(wf_ev)
            exs.extend(ex_list)

        if is_hashed:
            scenarios.append(
                scenario_spec_cls(
                    scenario_id=sc_id,
                    slug=slug,
                    category=cat,
                    title="`--hash-names` Anonymized Batch `JobSet` Replay (Independent Hashed Names & Labels)",
                    feature_focus="Verifies `testrunner.py` replays a `--hash-names` trace (`h-<32hex>` `JobSet` names, independent hashed `workloadName`s, hashed sensitive label values, plaintext queues/nodes) with identical metrics to its plaintext twin `st92`",
                    recommended_cmd=f"python3 deploy/scripts/testrunner.py testscenarios/self-tests/{sc_id}-{slug}",
                    setup_summary="4 nodes (`16` TPUs), `cq-hash-batch` (`16` TPUs, `StrictFIFO`). 8 hashed `JobSet`s (`h-<32hex>`, `4` TPUs × `10s`, independent hashed `workloadName`s, hashed `owner`/`team`/`project`/`cost-center` labels) arrive at `t=0s` across 2 waves (`20s` total).",
                    expected_behavior="Loader matches every hashed `JobSet` to its independent hashed `workloadName` via `ownerName`, replays 2 waves of 4 `JobSet`s (`0..10s`, `10..20s`) identically to `st92`, and preserves plaintext queue/priority names in Prometheus metrics.",
                    expected_metrics=[
                        "`Section 5.1`: `AVG ~95%–100%` (`16/16` TPUs across 2 waves).",
                        "`Section 5.2`: `p-std` `ADMISSIONS=8`, Wave 1 `~0s–1s`, Wave 2 `~10s–11s`, `AVG ~5.0s–6.5s`.",
                        "`Section 5.3`: **`0`** evictions.",
                        "`Section 5.4`: `Qualifying Workloads: 8 (completed=8)`, `EFFECTIVE SAT ~90%–100%` (matching `st92`).",
                    ],
                    must_not_happen=[
                        "Falling back to default/imputed durations because hashed `workloadName` differs from hashed `JobSet` name.",
                        "Any plaintext secret/team/project string (`alice-researcher`, `ml-core`, `secret-llm-v4`, `cc-999`) appearing in `st91` files.",
                    ],
                    sec51_target="~95%–100% (16/16 TPUs)",
                    sec52_target="p-std AVG ~5.0s–6.5s (8 WLs)",
                    sec53_target="0 evictions",
                    sec54_target="~90%–100% (8 completed, == st92)",
                    infra_events=inf,
                    workflow_events=wfs,
                    exec_events=exs,
                )
            )
        else:
            scenarios.append(
                scenario_spec_cls(
                    scenario_id=sc_id,
                    slug=slug,
                    category=cat,
                    title="Plaintext Batch `JobSet` Control & Live `--scrape-hash-names` Target (Twin of `st91`)",
                    feature_focus="Exact plaintext counterpart of `st91` carrying realistic sensitive labels (`owner`, `team`, `project`, `corp.example.com/cost-center`) for A/B metric parity and live `--scrape-hash-names` validation",
                    recommended_cmd=f"python3 deploy/scripts/testrunner.py testscenarios/self-tests/{sc_id}-{slug}",
                    setup_summary="Identical to `st91` (`4` nodes / `16` TPUs, `cq-hash-batch`, `8` `JobSet`s × `4` TPUs × `10s`) using plaintext names `batch-train-00..07` and sensitive organizational labels (`alice-researcher`, `ml-core`, `secret-llm-v4`, `cc-999`).",
                    expected_behavior="Replays identically to `st91` (`2` waves, `~20s`). When run with `--scrape-output-dir /tmp/st92-scraped --scrape-hash-names`, `cluster-scraper` must replace every `batch-train-*` name and sensitive label value with deterministic `h-<32hex>` digests.",
                    expected_metrics=[
                        "`Section 5.1`: `AVG ~95%–100%` (matches `st91`).",
                        "`Section 5.2`: `p-std` `ADMISSIONS=8`, `AVG ~5.0s–6.5s` (matches `st91`).",
                        "`Section 5.3`: **`0`** evictions (matches `st91`).",
                        "`Section 5.4`: `Qualifying Workloads: 8 (completed=8)`, `EFFECTIVE SAT ~90%–100%` (matches `st91`).",
                    ],
                    must_not_happen=[
                        "Metric divergence between `st91` (hashed) and `st92` (plaintext) beyond normal `~1s` chamber jitter.",
                    ],
                    sec51_target="~95%–100% (16/16 TPUs)",
                    sec52_target="p-std AVG ~5.0s–6.5s (8 WLs)",
                    sec53_target="0 evictions",
                    sec54_target="~90%–100% (8 completed, == st91)",
                    infra_events=inf,
                    workflow_events=wfs,
                    exec_events=exs,
                )
            )

    # ------------------------------------------------------------------
    # st93: Hashed t0 Mixed Kueue Workloads + True Non-Kueue Infra Pods
    # ------------------------------------------------------------------
    salt_93 = b"st93-kueue-optimizer-hmac-salt-v1"
    inf93: list[dict[str, Any]] = [make_infra_event_fn(make_namespace_fn(DEFAULT_NS))]
    for i in range(4):
        inf93.append(make_infra_event_fn(make_node_fn(i, tpu_per_node=4, cpu_per_node=16, mem_gi_per_node=64)))
    inf93.append(make_infra_event_fn(make_flavor_fn(DEFAULT_FLAVOR)))
    inf93.append(make_infra_event_fn(make_priority_class_fn("p-std", 500)))
    inf93.append(
        make_infra_event_fn(
            make_cluster_queue_fn(
                "cq-hash-t0",
                tpu_quota=12,
                cpu_quota=48,
                mem_gi_quota=192,
                queueing_strategy="StrictFIFO",
                within_cq="Never",
                reclaim_within_cohort="Never",
            )
        )
    )
    inf93.append(make_infra_event_fn(make_local_queue_fn("lq-hash-t0", "cq-hash-t0", DEFAULT_NS)))

    wf93: list[dict[str, Any]] = []
    ex93: list[dict[str, Any]] = []

    # 2 pre-admitted t0 hashed JobSets (4 TPUs each = 8 TPUs, 12s duration) with linked hashed Pods in infra.json.
    for i in range(2):
        h_js = _hmac_name(salt_93, f"t0-train-{i}")
        h_wl = _hmac_name(salt_93, f"jobset-t0-train-{i}-wl")
        h_pod = _hmac_name(salt_93, f"t0-train-{i}-workers-0-abcde")
        wf_ev, ex_list = _make_scraped_jobset(
            name=h_js,
            arrival_sec=0.0,
            duration_sec=12.0,
            local_queue="lq-hash-t0",
            cluster_queue="cq-hash-t0",
            num_pods=1,
            tpu_per_pod=4,
            priority_class="p-std",
        )
        wf_ev["workloadState"] = "Admitted"
        for ex in ex_list:
            ex["payload"]["workloadName"] = h_wl
        wf93.append(wf_ev)
        ex93.extend(ex_list)
        inf93.append(
            _make_t0_pod_infra_event(
                fmt_ts_fn=fmt_ts_fn,
                pod_name=h_pod,
                namespace=DEFAULT_NS,
                placement=f"st-node-{i:02d}",
                tpu_per_pod=4,
                workload_name=h_wl,
                owner_kind="JobSet",
                owner_name=h_js,
                pod_set_name="workers",
            )
        )

    # 2 genuine non-Kueue unmanaged hashed pods (2 TPUs each = 4 TPUs packed on st-node-02 -> MUST enter infra-pods,
    # leaving st-node-03 with 4/4 physical TPUs free for post-t0 JobSet 0!).
    for i in range(2):
        h_infra_pod = _hmac_name(salt_93, f"unmanaged-daemon-{i}")
        inf93.append(
            _make_t0_pod_infra_event(
                fmt_ts_fn=fmt_ts_fn,
                pod_name=h_infra_pod,
                namespace="system-agents",
                placement="st-node-02",
                tpu_per_pod=2,
            )
        )

    # 2 post-t0 hashed JobSets arriving at t=+2s (4 TPUs each, 12s):
    # first fits in remaining 4/12 CQ quota & st-node-03 immediately (wait ~0s),
    # second waits for t0 completion at t=12s (wait ~10s).
    for i in range(2):
        h_js = _hmac_name(salt_93, f"post-t0-train-{i}")
        h_wl = _hmac_name(salt_93, f"jobset-post-t0-train-{i}-wl")
        wf_ev, ex_list = _make_scraped_jobset(
            name=h_js,
            arrival_sec=2.0 + i * 0.2,
            duration_sec=12.0,
            local_queue="lq-hash-t0",
            cluster_queue="cq-hash-t0",
            num_pods=1,
            tpu_per_pod=4,
            priority_class="p-std",
        )
        for ex in ex_list:
            ex["payload"]["workloadName"] = h_wl
        wf93.append(wf_ev)
        ex93.extend(ex_list)

    scenarios.append(
        scenario_spec_cls(
            scenario_id="st93",
            slug="hash-names-t0-mixed-workloads-and-infra-pods",
            category=cat,
            title="`--hash-names` `t0` Mixed Kueue `JobSet`s & Unmanaged `infra-pods` Separation",
            feature_focus="Verifies hashed `t0` workflow pods (`h-<32hex>` linked via `ownerName`/`workloadName`) are filtered out of `infra-pods` and bootstrapped as `InitialWorkloadPodGroup`s while hashed non-Kueue pods remain in `infra-pods`",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st93-hash-names-t0-mixed-workloads-and-infra-pods --no-skip-initial-offset",
            setup_summary="4 nodes (`16` TPUs), `cq-hash-t0` (`12` TPUs). At `t0`: 2 pre-admitted hashed `JobSet`s (`4` TPUs × `12s` = `8` TPUs on `st-node-00..01`) + 2 hashed non-Kueue unmanaged pods (`2` TPUs each = `4` TPUs packed on `st-node-02`, leaving `st-node-03` free). At `t=+2s`: 2 hashed `JobSet`s (`4` TPUs × `12s`) arrive.",
            expected_behavior="Loader produces exact `2` `InitialWorkloadPodGroup`s (occupying `8/12` CQ TPUs) and `2` `InitialPodSpec`s in `infra-pods` (occupying `4` physical TPUs on `st-node-02` without CQ quota). Post-`t0` job 0 admits immediately (`8+4=12/12`, schedules onto `st-node-03`); job 1 waits `~10s` until a `t0` `JobSet` finishes at `t=12s`.",
            expected_metrics=[
                "`Pre-Run Outline`: `Initial Kueue Workloads: 2 (2 pods)`, `Initial Infra Pods: 2`.",
                "`Section 5.1`: `AVG ~75%–95%` (`12/12` TPUs during `t=2..10s`).",
                "`Section 5.2`: `p-std` `ADMISSIONS=2` (post-`t0`), job 0 `wait < 2s`, job 1 `wait ~10s`, `AVG ~5s`.",
                "`Section 5.3`: **`0`** evictions.",
                "`Section 5.4`: `4` Kueue completed workloads + `2` `Initial Infra Pods` (`active_at_end`), `0` double-counted pods.",
            ],
            must_not_happen=[
                "Leaking the 2 hashed `t0` `JobSet` pods into `infra-pods` (`Initial Infra Pods` MUST be `2`, never `4`).",
                "Dropping the 2 genuine hashed unmanaged pods from `infra-pods`.",
            ],
            sec51_target="~75%–95% (12/12 CQ TPUs)",
            sec52_target="p-std AVG ~5s (0s / ~10s)",
            sec53_target="0 evictions",
            sec54_target="4 completed + 2 infra pods",
            infra_events=inf93,
            workflow_events=wf93,
            exec_events=ex93,
        )
    )

    # ------------------------------------------------------------------
    # st94: Hashed Deployment with Salted templateHash Upgrade & ScaleDown -> 0
    # ------------------------------------------------------------------
    salt_94 = b"st94-kueue-optimizer-hmac-salt-v1"
    h_dep = _hmac_name(salt_94, "infer-gateway")
    h_app_lbl = _hmac_name(salt_94, "infer-gateway-app")
    h_dep_sens = {
        "owner": _hmac_name(salt_94, "serving-sre"),
        "project": _hmac_name(salt_94, "llm-serving-prod"),
    }
    th_v1 = _hmac_template_hash(salt_94, "image=corp/llm:v1;API_KEY=secret-v1")
    th_v2 = _hmac_template_hash(salt_94, "image=corp/llm:v2;API_KEY=secret-v2")
    h_batch_job = _hmac_name(salt_94, "offline-eval-01")
    h_batch_wl = _hmac_name(salt_94, "job-offline-eval-01-wl")

    inf94: list[dict[str, Any]] = [make_infra_event_fn(make_namespace_fn(DEFAULT_NS))]
    for i in range(4):
        inf94.append(make_infra_event_fn(make_node_fn(i, tpu_per_node=4, cpu_per_node=16, mem_gi_per_node=64)))
    inf94.append(make_infra_event_fn(make_flavor_fn(DEFAULT_FLAVOR)))
    inf94.append(make_infra_event_fn(make_priority_class_fn("p-std", 500)))
    inf94.append(
        make_infra_event_fn(
            make_cluster_queue_fn(
                "cq-hash-dep",
                tpu_quota=8,
                cpu_quota=32,
                mem_gi_quota=128,
                queueing_strategy="StrictFIFO",
                within_cq="Never",
                reclaim_within_cohort="Never",
            )
        )
    )
    inf94.append(make_infra_event_fn(make_local_queue_fn("lq-hash-dep", "cq-hash-dep", DEFAULT_NS)))

    dep_v1_r4 = _make_hashed_deployment_obj(
        det_uid_fn=det_uid_fn,
        fmt_ts_fn=fmt_ts_fn,
        name=h_dep,
        selector_val=h_app_lbl,
        local_queue="lq-hash-dep",
        priority_class="p-std",
        replicas=4,
        tpu_per_pod=2,
        template_hash=th_v1,
        sensitive_labels=h_dep_sens,
    )
    dep_v2_r4 = _make_hashed_deployment_obj(
        det_uid_fn=det_uid_fn,
        fmt_ts_fn=fmt_ts_fn,
        name=h_dep,
        selector_val=h_app_lbl,
        local_queue="lq-hash-dep",
        priority_class="p-std",
        replicas=4,
        tpu_per_pod=2,
        template_hash=th_v2,
        sensitive_labels=h_dep_sens,
    )
    dep_v2_r2 = _make_hashed_deployment_obj(
        det_uid_fn=det_uid_fn,
        fmt_ts_fn=fmt_ts_fn,
        name=h_dep,
        selector_val=h_app_lbl,
        local_queue="lq-hash-dep",
        priority_class="p-std",
        replicas=2,
        tpu_per_pod=2,
        template_hash=th_v2,
        sensitive_labels=h_dep_sens,
    )
    dep_v2_r0 = _make_hashed_deployment_obj(
        det_uid_fn=det_uid_fn,
        fmt_ts_fn=fmt_ts_fn,
        name=h_dep,
        selector_val=h_app_lbl,
        local_queue="lq-hash-dep",
        priority_class="p-std",
        replicas=0,
        tpu_per_pod=2,
        template_hash=th_v2,
        sensitive_labels=h_dep_sens,
    )

    batch_wf94, batch_ex94 = _make_batch_job_wf_and_exec(
        det_uid_fn=det_uid_fn,
        fmt_ts_fn=fmt_ts_fn,
        name=h_batch_job,
        wl_name=h_batch_wl,
        local_queue="lq-hash-dep",
        priority_class="p-std",
        priority=500,
        tpu_per_pod=4,
        parallelism=1,
        exec_sec=6.0,
        arrival_sec=16.0,
        admit_sec=18.0,
        extra_labels=h_dep_sens,
    )

    wf94: list[dict[str, Any]] = [
        {
            "timestamp": fmt_ts_fn(0.0),
            "eventType": "New",
            "workloadState": "NotAdmitted",
            "payload": {
                "objectKind": "Deployment",
                "name": h_dep,
                "namespace": DEFAULT_NS,
                "creationTime": fmt_ts_fn(0.0),
                "localQueueName": "lq-hash-dep",
                "replicas": 4,
                "readyReplicas": 0,
                "paused": False,
                "templateHash": th_v1,
                "object": dep_v1_r4,
            },
        },
        {
            "timestamp": fmt_ts_fn(8.0),
            "eventType": "Update",
            "workloadState": "Admitted",
            "payload": {
                "objectKind": "Deployment",
                "name": h_dep,
                "namespace": DEFAULT_NS,
                "creationTime": fmt_ts_fn(0.0),
                "localQueueName": "lq-hash-dep",
                "replicas": 4,
                "readyReplicas": 4,
                "paused": False,
                "templateHash": th_v2,
                "updateTypes": ["Upgrade"],
                "object": dep_v2_r4,
            },
        },
        batch_wf94,
        {
            "timestamp": fmt_ts_fn(18.0),
            "eventType": "Update",
            "workloadState": "Admitted",
            "payload": {
                "objectKind": "Deployment",
                "name": h_dep,
                "namespace": DEFAULT_NS,
                "creationTime": fmt_ts_fn(0.0),
                "localQueueName": "lq-hash-dep",
                "replicas": 2,
                "readyReplicas": 4,
                "paused": False,
                "templateHash": th_v2,
                "updateTypes": ["ScaleDown"],
                "object": dep_v2_r2,
            },
        },
        {
            "timestamp": fmt_ts_fn(26.0),
            "eventType": "Update",
            "workloadState": "NotAdmitted",
            "payload": {
                "objectKind": "Deployment",
                "name": h_dep,
                "namespace": DEFAULT_NS,
                "creationTime": fmt_ts_fn(0.0),
                "localQueueName": "lq-hash-dep",
                "replicas": 0,
                "readyReplicas": 0,
                "paused": False,
                "templateHash": th_v2,
                "updateTypes": ["ScaleDown", "StateTransition"],
                "object": dep_v2_r0,
            },
        },
    ]

    scenarios.append(
        scenario_spec_cls(
            scenario_id="st94",
            slug="hash-names-deployment-salted-upgrade-and-scale",
            category=cat,
            title="`--hash-names` `Deployment` Salted `templateHash` Rolling `Upgrade` & `ScaleDown -> 0`",
            feature_focus="Verifies an anonymized `Deployment` (`h-<32hex>` name & matching hashed selectors) rolls over cleanly across distinct HMAC-salted `th-<16hex>` `templateHash` annotations, releases quota on `ScaleDown 4 -> 2` to unblock a hashed batch `Job`, and credits all retired replicas on `ScaleDown -> 0`",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st94-hash-names-deployment-salted-upgrade-and-scale --duration 34s",
            setup_summary=f"4 nodes (`16` TPUs), `cq-hash-dep` (`8` TPUs). Hashed `Deployment` (`replicas: 4`, `2` TPUs/pod = `8` TPUs, `maxUnavailable: 1, maxSurge: 0`) rolls from salted `{th_v1}` to `{th_v2}` at `t=+8s`; hashed batch `Job` (`4` TPUs, `6s`) arrives at `t=+16s`; `ScaleDown 4 -> 2` at `t=+18s` unblocks it; `ScaleDown 2 -> 0` at `t=+26s`.",
            expected_behavior="Distinct salted `templateHash` triggers a real `ReplicaSet` rollover (`4` v1 pods cleanly replaced by `4` v2 pods under `8`-TPU cap). At `t=+18s`, `ScaleDown 4 -> 2` frees `4` TPUs so the hashed batch `Job` admits (`wait ~2s`) and finishes at `~t=24s`. At `t=+26s`, `ScaleDown 2 -> 0` retires the last `2` replicas cleanly (`0` evictions).",
            expected_metrics=[
                "`Execution Summary`: `pods running=0, succeeded=1` (hashed batch `Job`).",
                "`Section 5.2`: `p-std` records `9` admitted workloads (`4` v1 + `4` v2 serving replicas + `1` hashed batch `Job`).",
                "`Section 5.3`: **`0`** evictions.",
                "`Section 5.4`: `Qualifying Workloads: 9 (completed=9 [9 pods], active_at_end=0 [0 pods])`, `Excluded: 0`, `Unadmitted: 0`.",
            ],
            must_not_happen=[
                "Selector mismatch between hashed `spec.selector.matchLabels` and hashed `spec.template.metadata.labels`.",
                "Dropping the rolling `Upgrade` because `templateHash` was stripped or colliding.",
            ],
            sec51_target="~90%–100% (8/8 CQ TPUs)",
            sec52_target="9 WLs (batch wait ~2s)",
            sec53_target="0 evictions",
            sec54_target="completed=9, active=0, excluded=0",
            infra_events=inf94,
            workflow_events=wf94,
            exec_events=batch_ex94,
        )
    )

    # ------------------------------------------------------------------
    # st95: Hashed 7-Level TAS + Annotation-Only t0 PodGroup + Preemption
    # ------------------------------------------------------------------
    salt_95 = b"st95-kueue-optimizer-hmac-salt-v1"
    inf95: list[dict[str, Any]] = [make_infra_event_fn(make_namespace_fn(DEFAULT_NS))]
    for i in range(4):
        inf95.append(
            make_infra_event_fn(
                make_node_fn(i, tpu_per_node=4, cpu_per_node=16, mem_gi_per_node=64, with_topology=True)
            )
        )
    inf95.append(make_infra_event_fn(make_topology_fn(DEFAULT_TOPOLOGY)))
    inf95.append(make_infra_event_fn(make_flavor_fn(DEFAULT_FLAVOR, topology_name=DEFAULT_TOPOLOGY)))
    inf95.append(make_infra_event_fn(make_priority_class_fn("p-low", 100)))
    inf95.append(make_infra_event_fn(make_priority_class_fn("p-high", 1000)))
    inf95.append(
        make_infra_event_fn(
            make_cluster_queue_fn(
                "cq-hash-tas",
                tpu_quota=16,
                cpu_quota=64,
                mem_gi_quota=256,
                queueing_strategy="StrictFIFO",
                within_cq="LowerPriority",
                reclaim_within_cohort="Never",
            )
        )
    )
    inf95.append(make_infra_event_fn(make_local_queue_fn("lq-hash-tas", "cq-hash-tas", DEFAULT_NS)))

    wf95: list[dict[str, Any]] = []
    ex95: list[dict[str, Any]] = []

    # 2 pre-admitted t0 TAS PodGroups using annotation-only kueue.x-k8s.io/pod-group-name: h-<32hex> (4 TPUs each, 12s).
    for i in range(2):
        h_pg = _hmac_name(salt_95, f"t0-tas-pg-{i}")
        h_pod = _hmac_name(salt_95, f"t0-tas-pg-{i}-pod-0")
        pod_obj = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": h_pod,
                "namespace": DEFAULT_NS,
                "uid": det_uid_fn(f"pod:{h_pod}"),
                "creationTimestamp": fmt_ts_fn(0.0),
                "labels": {
                    "kueue.x-k8s.io/queue-name": "lq-hash-tas",
                    "kueue.x-k8s.io/priority-class": "p-low",
                    "owner": _hmac_name(salt_95, f"tas-team-{i}"),
                },
                "annotations": {
                    "kueue.x-k8s.io/pod-group-name": h_pg,
                    "kueue.x-k8s.io/pod-group-total-count": "1",
                    "kueue.x-k8s.io/podset-required-topology": "kubernetes.io/hostname",
                },
            },
            "spec": {
                "restartPolicy": "Never",
                "containers": [
                    {
                        "name": "worker",
                        "image": "registry.k8s.io/pause:3.10",
                        "resources": {
                            "requests": {"cpu": "8", "memory": "32Gi", "google.com/tpu": "4"},
                            "limits": {"cpu": "8", "memory": "32Gi", "google.com/tpu": "4"},
                        },
                    }
                ],
            },
        }
        wf95.append(
            {
                "timestamp": fmt_ts_fn(0.0),
                "eventType": "New",
                "workloadState": "Admitted",
                "payload": {
                    "objectKind": "Pod",
                    "name": h_pod,
                    "namespace": DEFAULT_NS,
                    "creationTime": fmt_ts_fn(0.0),
                    "localQueueName": "lq-hash-tas",
                    "object": pod_obj,
                },
            }
        )
        inf95.append(
            _make_t0_pod_infra_event(
                fmt_ts_fn=fmt_ts_fn,
                pod_name=h_pod,
                namespace=DEFAULT_NS,
                placement=f"st-node-{i:02d}",
                tpu_per_pod=4,
                workload_name=h_pg,
                owner_kind="PodGroup",
                owner_name=h_pg,
                pod_set_name="main",
                is_tas=True,
            )
        )
        ex95.extend(
            [
                {
                    "timestamp": fmt_ts_fn(0.0),
                    "eventType": "Admitted",
                    "workloadState": "Admitted",
                    "payload": {
                        "workloadName": h_pg,
                        "namespace": DEFAULT_NS,
                        "ownerKind": "PodGroup",
                        "ownerName": h_pg,
                        "localQueueName": "lq-hash-tas",
                        "clusterQueueName": "cq-hash-tas",
                        "priority": 100,
                        "priorityClassName": "p-low",
                        "admittedTime": fmt_ts_fn(0.0),
                    },
                },
                {
                    "timestamp": fmt_ts_fn(12.0),
                    "eventType": "FullExecution",
                    "workloadState": "Admitted",
                    "payload": {
                        "workloadName": h_pg,
                        "namespace": DEFAULT_NS,
                        "ownerKind": "PodGroup",
                        "ownerName": h_pg,
                        "localQueueName": "lq-hash-tas",
                        "clusterQueueName": "cq-hash-tas",
                        "priority": 100,
                        "priorityClassName": "p-low",
                        "admittedTime": fmt_ts_fn(0.0),
                        "endTime": fmt_ts_fn(12.0),
                        "executionDurationSeconds": 12.0,
                        "executionDuration": "12s",
                        "executionType": "Full",
                    },
                },
            ]
        )

    # 2 low-priority hashed TAS JobSets at t=0s (4 TPUs each, 16s -> saturates remaining 8 TPUs on nodes 2 & 3).
    for i in range(2):
        h_js = _hmac_name(salt_95, f"tas-low-job-{i}")
        h_wl = _hmac_name(salt_95, f"jobset-tas-low-job-{i}-wl")
        wf_ev, ex_list = _make_scraped_jobset(
            name=h_js,
            arrival_sec=float(i) * 0.1,
            duration_sec=16.0,
            local_queue="lq-hash-tas",
            cluster_queue="cq-hash-tas",
            num_pods=1,
            tpu_per_pod=4,
            priority_class="p-low",
            tas_required_level="kubernetes.io/hostname",
        )
        for ex in ex_list:
            ex["payload"]["workloadName"] = h_wl
        wf95.append(wf_ev)
        ex95.extend(ex_list)

    # 1 high-priority hashed TAS JobSet at t=+4s (4 TPUs, 8s) -> preempts 1 p-low hashed workload!
    h_hi_js = _hmac_name(salt_95, "tas-high-urgent-0")
    h_hi_wl = _hmac_name(salt_95, "jobset-tas-high-urgent-0-wl")
    wf_hi, ex_hi = _make_scraped_jobset(
        name=h_hi_js,
        arrival_sec=4.0,
        duration_sec=8.0,
        local_queue="lq-hash-tas",
        cluster_queue="cq-hash-tas",
        num_pods=1,
        tpu_per_pod=4,
        priority_class="p-high",
        tas_required_level="kubernetes.io/hostname",
    )
    for ex in ex_hi:
        ex["payload"]["workloadName"] = h_hi_wl
    wf95.append(wf_hi)
    ex95.extend(ex_hi)

    scenarios.append(
        scenario_spec_cls(
            scenario_id="st95",
            slug="hash-names-tas-podgroup-annotation-and-preemption",
            category=cat,
            title="`--hash-names` 7-Level TAS, Annotation-Only `t0` `PodGroup` & `LowerPriority` Preemption",
            feature_focus="Verifies `--hash-names` works end-to-end with 7-level TAS topology placement, annotation-only `kueue.x-k8s.io/pod-group-name: h-<32hex>` `t0` `PodGroup`s, and `LowerPriority` preemption of hashed workloads",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st95-hash-names-tas-podgroup-annotation-and-preemption --no-skip-initial-offset",
            setup_summary="4 TAS nodes (`16` TPUs, `st-slice-topology`), `cq-hash-tas` (`16` TPUs, `withinClusterQueue: LowerPriority`). At `t0`: 2 annotation-only hashed `PodGroup`s (`p-low`, `4` TPUs × `12s`, `st-node-00..01`). At `t=0s`: 2 hashed `p-low` TAS `JobSet`s (`4` TPUs × `16s`) fill `16/16` TPUs. At `t=+4s`: 1 hashed `p-high` TAS `JobSet` (`4` TPUs × `8s`) arrives.",
            expected_behavior="Loader groups the 2 annotation-only hashed `Pod`s into 2 pre-admitted TAS `InitialWorkloadPodGroup`s (occupying `st-node-00..01`). At `t=+4s`, the `p-high` hashed TAS `JobSet` preempts 1 `p-low` hashed workload (`wait < 2.5s`), runs for `8s`, and the preempted `p-low` workload re-admits and completes cleanly.",
            expected_metrics=[
                "`Pre-Run Outline`: `Initial Kueue Workloads: 2 (2 pods)` (both `[TAS]`), `Initial Infra Pods: 0`.",
                "`Section 5.1`: `AVG ~85%–100%` (`16/16` TPUs).",
                "`Section 5.2`: `p-high` `wait < 2.5s` (1 WL); `p-low` records the 2 initial arrivals + 1 re-admission.",
                "`Section 5.3`: **`1`** `Preempted` eviction (`p-low`).",
                "`Section 5.4`: all `5` hashed workloads complete (`0` unadmitted).",
            ],
            must_not_happen=[
                "Failing to group annotation-only `kueue.x-k8s.io/pod-group-name: h-<32hex>` pods into `InitialWorkloadPodGroup`s (`Initial Infra Pods` MUST be `0`).",
                "`p-high` waiting `>= 8s` instead of preempting a `p-low` hashed workload.",
            ],
            sec51_target="~85%–100% (16/16 TAS TPUs)",
            sec52_target="p-high < 2.5s; p-low re-admits",
            sec53_target="1 Preempted (p-low)",
            sec54_target="5 completed (TAS + annotation PG)",
            infra_events=inf95,
            workflow_events=wf95,
            exec_events=ex95,
        )
    )

    return scenarios


HASHING_SCENARIO_PAIRINGS: dict[str, list[dict[str, str]]] = {
    "st91": [
        {
            "peer_id": "st92",
            "relationship": "Exact `--hash-names` vs. Plaintext Batch `JobSet` A/B Twin (`st91` Hashed vs. `st92` Plaintext)",
            "knob_changed": "HMAC-SHA256 `h-<32hex>` workflow names, independent hashed `workloadName`s, and hashed sensitive label values (`st91`) vs. plaintext names/labels (`st92`) on identical cluster, queues, and durations.",
            "cross_check": "`st91` and `st92` MUST produce matching `Sec 5.1 (~95%–100%)`, `Sec 5.2 (p-std AVG ~5.0s–6.5s)`, `Sec 5.3 (0 evictions)`, and `Sec 5.4 (8 completed, ~90%–100%)`, proving `--hash-names` anonymization preserves replay fidelity.",
        },
    ],
    "st92": [
        {
            "peer_id": "st91",
            "relationship": "Plaintext Control Twin of `st91` & Live `--scrape-hash-names` Capture Source",
            "knob_changed": "Plaintext names & sensitive organizational labels (`st92`) vs. pre-hashed `--hash-names` bundle (`st91`).",
            "cross_check": "Replay metrics MUST match `st91`, and attaching `--scrape-hash-names` to `st92` MUST strip all raw names and sensitive label values (`alice-researcher`, `ml-core`, `secret-llm-v4`, `cc-999`).",
        },
    ],
    "st93": [
        {
            "peer_id": "st32",
            "relationship": "Hashed `t0` Hybrid `JobSet` + `infra-pods` (`st93`) vs. Plaintext `t0` Hybrid Baseline (`st32`)",
            "knob_changed": "All `t0` workflow, workload, and unmanaged pod names replaced with independent `h-<32hex>` digests (`st93`) vs. plaintext names (`st32`).",
            "cross_check": "`st93` MUST separate exact `2` `InitialWorkloadPodGroup`s and `2` `Initial Infra Pods` (zero leakage of hashed workflow pods into `infra-pods`), matching `st32`'s hybrid separation behavior.",
        },
    ],
    "st94": [
        {
            "peer_id": "st85",
            "relationship": "Hashed `Deployment` Salted `templateHash` Upgrade & ScaleDown (`st94`) vs. Plaintext `Deployment` Rolling Upgrade (`st85`)",
            "knob_changed": "`h-<32hex>` Deployment name/selectors + HMAC-salted `th-<16hex>` `templateHash` + `ScaleDown 4 -> 2 -> 0` with hashed batch `Job` (`st94`) vs. plaintext rolling upgrade (`st85`).",
            "cross_check": "`st94` rolls all `4` v1 pods to `4` v2 pods cleanly (`0` evictions), unblocks the hashed batch `Job` at `t=+18s`, and credits all `9` workloads (`completed=9, active=0, excluded=0`).",
        },
    ],
    "st95": [
        {
            "peer_id": "st40",
            "relationship": "Hashed TAS + Annotation-Only `t0` `PodGroup` + Preemption (`st95`) vs. Plaintext `t0` TAS Baseline (`st40`)",
            "knob_changed": "Annotation-only `kueue.x-k8s.io/pod-group-name: h-<32hex>` `t0` `PodGroup`s + hashed TAS `JobSet`s with `LowerPriority` preemption (`st95`) vs. plaintext `t0` TAS (`st40`).",
            "cross_check": "`st95` groups both annotation-only hashed `PodGroup`s into `[TAS]` `InitialWorkloadPodGroup`s (`Initial Infra Pods: 0`) and triggers `1` `Preempted` eviction so `p-high` admits in `< 2.5s`.",
        },
    ],
}
