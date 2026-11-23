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

"""Kueue Controller `Configuration` scenarios (`st76`–`st80`).

Covers Category 16 (`st76`–`st80`):
- `st76`: Enabling global Fair Sharing (`fairSharing.preemptionStrategies:
  [LessThanOrEqualToFinalShare, LessThanInitialShare]`) via a `Configuration` statement in `infra.json`.
- `st77`: Disabling Fair Sharing globally (`fairSharing` omitted in `Configuration` -> Classical
  Preemption) on the exact same cluster, Cohort, ClusterQueues (`reclaimWithinCohort: Any`), and workloads.
- `st78`: Changing Fair Sharing rules (`fairSharing.preemptionStrategies: [LessThanOrEqualToFinalShare]`
  only, removing `LessThanInitialShare`) on the exact same cluster, Cohort, ClusterQueues, and workloads.
- `st79`: Enabling Admission-Based Fair Sharing (`admissionFairSharing` configured in `Configuration` with
  `usageHalfLifeTime`, `usageSamplingInterval`, and `resourceWeights` alongside
  `ClusterQueue.spec.admissionScope.admissionMode: UsageBasedAdmissionFairSharing` and `LocalQueue` weights).
- `st80`: Not using Admission-Based Fair Sharing (`admissionFairSharing` omitted in `Configuration`) on the
  exact same ClusterQueue, LocalQueues, and workloads as `st79`.
"""

from __future__ import annotations

from typing import Any, Callable


DEFAULT_NS = "st-workloads"
DEFAULT_FLAVOR = "st-tpu-flavor"
DEFAULT_ACCEL_LABEL = "tpu-v7x-st"


def _make_kueue_configuration(
    *,
    fair_sharing_strategies: list[str] | None,
    admission_fair_sharing: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a `config.kueue.x-k8s.io/v1beta2` `Configuration` object for `infra.json`."""
    cfg: dict[str, Any] = {
        "apiVersion": "config.kueue.x-k8s.io/v1beta2",
        "kind": "Configuration",
    }
    if fair_sharing_strategies is not None:
        cfg["fairSharing"] = {
            "preemptionStrategies": list(fair_sharing_strategies),
        }
    if admission_fair_sharing is not None:
        cfg["admissionFairSharing"] = dict(admission_fair_sharing)
    return cfg


def _make_node(
    idx: int,
    det_uid_fn: Callable[[str], str],
    *,
    tpu_per_node: int = 4,
    cpu_per_node: int = 16,
    mem_gi_per_node: int = 64,
) -> dict[str, Any]:
    name = f"st-node-{idx:02d}"
    return {
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {
            "name": name,
            "uid": det_uid_fn(f"node:{name}"),
            "labels": {
                "beta.kubernetes.io/arch": "amd64",
                "beta.kubernetes.io/os": "linux",
                "kubernetes.io/arch": "amd64",
                "kubernetes.io/hostname": name,
                "kubernetes.io/os": "linux",
                "kubernetes.io/role": "agent",
                "node-role.kubernetes.io/agent": "",
                "type": "kwok",
                "kueue-optimizer.gke.io/node-role": "agent",
                "cloud.google.com/gke-tpu-accelerator": DEFAULT_ACCEL_LABEL,
            },
            "annotations": {
                "kwok.x-k8s.io/node": "fake",
                "node.alpha.kubernetes.io/ttl": "0",
            },
        },
        "spec": {},
        "status": {
            "phase": "Running",
            "capacity": {
                "cpu": str(cpu_per_node),
                "memory": f"{mem_gi_per_node}Gi",
                "pods": "110",
                "google.com/tpu": str(tpu_per_node),
            },
            "allocatable": {
                "cpu": str(cpu_per_node),
                "memory": f"{mem_gi_per_node}Gi",
                "pods": "110",
                "google.com/tpu": str(tpu_per_node),
            },
            "conditions": [
                {
                    "type": "Ready",
                    "status": "True",
                    "reason": "KubeletReady",
                    "message": "kubelet is posting ready status",
                }
            ],
        },
    }


def _make_afs_cluster_queue(
    name: str,
    det_uid_fn: Callable[[str], str],
    *,
    tpu_quota: int,
    cpu_quota: int,
    mem_gi_quota: int,
    admission_mode: str = "UsageBasedAdmissionFairSharing",
) -> dict[str, Any]:
    return {
        "apiVersion": "kueue.x-k8s.io/v1beta2",
        "kind": "ClusterQueue",
        "metadata": {
            "name": name,
            "uid": det_uid_fn(f"cq:{name}"),
        },
        "spec": {
            "namespaceSelector": {},
            "queueingStrategy": "BestEffortFIFO",
            "preemption": {
                "withinClusterQueue": "Never",
                "reclaimWithinCohort": "Never",
            },
            "admissionScope": {
                "admissionMode": admission_mode,
            },
            "resourceGroups": [
                {
                    "coveredResources": ["cpu", "memory", "google.com/tpu"],
                    "flavors": [
                        {
                            "name": DEFAULT_FLAVOR,
                            "resources": [
                                {"name": "cpu", "nominalQuota": str(cpu_quota)},
                                {"name": "memory", "nominalQuota": f"{mem_gi_quota}Gi"},
                                {"name": "google.com/tpu", "nominalQuota": str(tpu_quota)},
                            ],
                        }
                    ],
                }
            ],
        },
    }


def _make_weighted_local_queue(
    name: str,
    cluster_queue: str,
    det_uid_fn: Callable[[str], str],
    *,
    weight: str = "1",
    namespace: str = DEFAULT_NS,
) -> dict[str, Any]:
    return {
        "apiVersion": "kueue.x-k8s.io/v1beta2",
        "kind": "LocalQueue",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "uid": det_uid_fn(f"lq:{namespace}/{name}"),
        },
        "spec": {
            "clusterQueue": cluster_queue,
            "fairSharing": {
                "weight": str(weight),
            },
        },
    }


def build_kueue_config_5_scenarios(
    *,
    scenario_spec_cls: Any,
    det_uid_fn: Callable[[str], str],
    make_infra_event_fn: Callable[..., dict[str, Any]],
    make_namespace_fn: Callable[..., dict[str, Any]],
    make_flavor_fn: Callable[..., dict[str, Any]],
    make_priority_class_fn: Callable[..., dict[str, Any]],
    make_cohort_fn: Callable[..., dict[str, Any]],
    make_cluster_queue_fn: Callable[..., dict[str, Any]],
    make_local_queue_fn: Callable[..., dict[str, Any]],
    make_jobset_and_exec_events_fn: Callable[..., tuple[dict[str, Any], list[dict[str, Any]]]],
) -> list[Any]:
    """Generate `st76` through `st80` focused on declarative Kueue `Configuration` statements."""
    scenarios: list[Any] = []

    def _build_triad_fixture(
        config_obj: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        """Build identical 8-node (`32`-TPU) Cohort + ClusterQueues + Workloads for `st76`, `st77`, and `st78`."""
        inf: list[dict[str, Any]] = [make_infra_event_fn(config_obj)]
        for i in range(8):
            inf.append(make_infra_event_fn(_make_node(i, det_uid_fn)))
        inf.append(make_infra_event_fn(make_namespace_fn(DEFAULT_NS)))
        inf.append(make_infra_event_fn(make_flavor_fn(DEFAULT_FLAVOR)))
        inf.append(make_infra_event_fn(make_priority_class_fn("p-beta-small", 100)))
        inf.append(make_infra_event_fn(make_priority_class_fn("p-beta-large", 200)))
        inf.append(make_infra_event_fn(make_priority_class_fn("p-alpha", 300)))
        inf.append(
            make_infra_event_fn(
                make_cohort_fn("cohort-fs-cfg", tpu_quota=32, cpu_quota=128, mem_gi_quota=512)
            )
        )
        for cq_n in ("cq-beta", "cq-alpha"):
            inf.append(
                make_infra_event_fn(
                    make_cluster_queue_fn(
                        cq_n,
                        tpu_quota=0,
                        cpu_quota=0,
                        mem_gi_quota=0,
                        cohort_name="cohort-fs-cfg",
                        within_cq="Never",
                        reclaim_within_cohort="Any",
                        borrow_policy="Never",
                        fair_sharing_weight="1",
                    )
                )
            )
        inf.append(make_infra_event_fn(make_local_queue_fn("lq-beta", "cq-beta")))
        inf.append(make_infra_event_fn(make_local_queue_fn("lq-alpha", "cq-alpha")))

        wf: list[dict[str, Any]] = []
        ex: list[dict[str, Any]] = []
        # Incumbents at t=0s in cq-beta: 2 x 1-pod (4 TPU, p-beta-small=100, 24s) + 1 x 6-pod (24 TPU, p-beta-large=200, 24s) = 32/32 TPUs
        for i in range(2):
            w_ev, e_evs = make_jobset_and_exec_events_fn(
                name=f"js-beta-small-{i}",
                arrival_sec=0.0,
                duration_sec=24.0,
                local_queue="lq-beta",
                cluster_queue="cq-beta",
                num_pods=1,
                priority_class="p-beta-small",
            )
            wf.append(w_ev)
            ex.extend(e_evs)
        w_big, e_big = make_jobset_and_exec_events_fn(
            name="js-beta-large-0",
            arrival_sec=0.0,
            duration_sec=24.0,
            local_queue="lq-beta",
            cluster_queue="cq-beta",
            num_pods=6,
            priority_class="p-beta-large",
        )
        wf.append(w_big)
        ex.extend(e_big)

        # Challengers at t=5s in cq-alpha: 4 x 1-pod (4 TPU, p-alpha=300, 20s -> finishes at t=25s > 24s) = 16 TPUs (50% Cohort fair share)
        for i in range(4):
            w_a, e_a = make_jobset_and_exec_events_fn(
                name=f"js-alpha-{i}",
                arrival_sec=5.0,
                duration_sec=20.0,
                local_queue="lq-alpha",
                cluster_queue="cq-alpha",
                num_pods=1,
                priority_class="p-alpha",
            )
            wf.append(w_a)
            ex.extend(e_a)
        return inf, wf, ex

    # st76: Enabling Global Fair Sharing ([LessThanOrEqualToFinalShare, LessThanInitialShare])
    inf76, wf76, ex76 = _build_triad_fixture(
        _make_kueue_configuration(
            fair_sharing_strategies=["LessThanOrEqualToFinalShare", "LessThanInitialShare"]
        )
    )
    scenarios.append(
        scenario_spec_cls(
            scenario_id="st76",
            slug="config-fair-sharing-enabled-default",
            category="16. Kueue Controller Configuration (`FairSharing` & `AdmissionFairSharing`)",
            title="Declarative Kueue `Configuration` Enabling Global Fair Sharing (`[LessThanOrEqualToFinalShare, LessThanInitialShare]`)",
            feature_focus="Declarative `kind: Configuration` in `infra.json` enabling global Fair Sharing with both `LessThanOrEqualToFinalShare` (S2-a) and `LessThanInitialShare` (S2-b), evicting all `3` `cq-beta` incumbents (`2×4` TPUs + `1×24` TPUs) so all `4` `cq-alpha` challengers start immediately",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st76-config-fair-sharing-enabled-default",
            setup_summary="8-node (`32`-TPU) cluster with `kind: Configuration` (`fairSharing.preemptionStrategies: [LessThanOrEqualToFinalShare, LessThanInitialShare]`). `cohort-fs-cfg` holds `32` TPUs; `cq-beta` and `cq-alpha` both have `nominalQuota: 0`, `fairSharing.weight: 1`, `reclaimWithinCohort: Any`, `borrowWithinCohort.policy: Never`. At `t=0s`, `cq-beta` starts `2` `4`-TPU jobs (`p-beta-small=100`, `24s`) + `1` `24`-TPU job (`p-beta-large=200`, `24s`). At `t=5s`, `cq-alpha` submits `4` `4`-TPU jobs (`p-alpha=300`, `20s`).",
            expected_behavior="`testrunner.py` reads `Configuration` from `infra.json` and deploys `kueue-controller-manager` with both S2-a and S2-b active. At `t=5s`, `js-alpha-0` and `js-alpha-1` evict `js-beta-small-0/1` via `LessThanOrEqualToFinalShare`, while `js-alpha-2` falls back to `LessThanInitialShare` (`0.375 < 0.75`) to evict `js-beta-large-0`, admitting all `4/4` `p-alpha` jobs in `< 2.5s`.",
            expected_metrics=[
                "`Section 5.3 (Total Evictions & Preemptions)`: **`3` `FairSharing` preemptions** (`Total Evictions = 3`, `Unique workloads evicted once = 3`, `FairSharing = 3`: `2` `p-beta-small` + `1` `p-beta-large`).",
                "`Section 5.2 (Wait Time)`: `p-alpha` admits immediately (**`P50 < 2.5s, P90 < 3.5s, AVG < 2.5s`**).",
                "`Section 1 Gauge`: `kueue_cluster_queue_weighted_share` actively emitted for `cq-alpha` and `cq-beta`.",
                "`cluster-scraper`: Records sanitized `kind: Configuration` (`fairSharing.preemptionStrategies: [LessThanOrEqualToFinalShare, LessThanInitialShare]`) in `infra.json`.",
            ],
            must_not_happen=[
                "`p-alpha` waiting `~19s–23s` (would mean `Configuration` failed to enable Fair Sharing).",
                "Fewer than `3` `FairSharing` preemptions (would mean `LessThanInitialShare` fallback was not configured).",
            ],
            sec51_target="Cohort 78%–100%",
            sec52_target="p-alpha AVG < 2.5s (4/4 immediate)",
            sec53_target="3 evictions (FairSharing=3)",
            sec54_target="~60%–78% (all 7 jobs finish)",
            infra_events=inf76,
            workflow_events=wf76,
            exec_events=ex76,
        )
    )

    # st77: Disabling Fair Sharing Globally via Kueue Configuration (Exact Triad Twin of st76 & st78)
    inf77, wf77, ex77 = _build_triad_fixture(
        _make_kueue_configuration(fair_sharing_strategies=None)
    )
    scenarios.append(
        scenario_spec_cls(
            scenario_id="st77",
            slug="config-fair-sharing-disabled-globally",
            category="16. Kueue Controller Configuration (`FairSharing` & `AdmissionFairSharing`)",
            title="Declarative Kueue `Configuration` Disabling Fair Sharing Globally (Classical Preemption — Twin of st76)",
            feature_focus="Declarative `kind: Configuration` (with `fairSharing` omitted) automatically deploying the `kueue-optimizer` chamber with global Fair Sharing disabled — without `--no-fair-sharing` CLI flags — switching Kueue to Classical Preemption (`0` evictions, `~19s–23s` wait)",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st77-config-fair-sharing-disabled-globally",
            setup_summary="Identical 8-node (`32`-TPU) cluster, `cohort-fs-cfg`, `ClusterQueue`s (`reclaimWithinCohort: Any`, `fairSharing.weight: 1`), and 7 workloads as `st76`, changing ONLY `kind: Configuration` in `infra.json` to omit `fairSharing` (`cfg.FairSharing == nil`).",
            expected_behavior="`testrunner.py` reads `Configuration` from `infra.json` and deploys `kueue-controller-manager` with `fairSharing` removed from `ConfigMap/kueue-manager-config`. Under Classical Preemption, `cq-alpha` (`nominalQuota: 0`, `borrowWithinCohort.policy: Never`) cannot preempt while borrowing even with `reclaimWithinCohort: Any` and `p-alpha=300`, so `0` jobs are evicted and all `4` `p-alpha` jobs wait `~19s` until `t=24s`.",
            expected_metrics=[
                "`Section 5.3 (Total Evictions & Preemptions)`: Strictly **`0`** (`Total Evictions = 0, FairSharing = 0`, vs. `3` in `st76`).",
                "`Section 5.2 (Wait Time)`: `p-alpha` wait jumps from `< 2.5s` (`st76`) to **`AVG ≈ 19s–23s`** (`P50 ≈ 30s` bucket interpolation over `[20s, 40s]`).",
                "`Section 4 (Not Emitted)`: `kueue_cluster_queue_weighted_share` is absent because global Fair Sharing is disabled.",
                "`Section 5.4 (Effective Saturation)`: **`~72%–85%`** (`Excluded: 0`, zero wasted compute).",
                "`cluster-scraper`: Records `kind: Configuration` without `fairSharing` in `infra.json`.",
            ],
            must_not_happen=[
                "Any `FairSharing` eviction (`Section 5.3 > 0`), which would prove the chamber deployed with default `fairSharing` instead of obeying the scenario's `Configuration` statement.",
            ],
            sec51_target="Cohort ~75%–92%",
            sec52_target="p-alpha AVG ~19s–23s (0/4 immediate)",
            sec53_target="0 evictions (Classical Preemption)",
            sec54_target="~72%–85% (Excluded: 0)",
            infra_events=inf77,
            workflow_events=wf77,
            exec_events=ex77,
        )
    )

    # st78: Changing Fair Sharing Rules ([LessThanOrEqualToFinalShare] Only — Exact Triad Twin of st76 & st77)
    inf78, wf78, ex78 = _build_triad_fixture(
        _make_kueue_configuration(
            fair_sharing_strategies=["LessThanOrEqualToFinalShare"]
        )
    )
    scenarios.append(
        scenario_spec_cls(
            scenario_id="st78",
            slug="config-fair-sharing-final-share-rule-only",
            category="16. Kueue Controller Configuration (`FairSharing` & `AdmissionFairSharing`)",
            title="Declarative Kueue `Configuration` Changing Fair Sharing Rules (`[LessThanOrEqualToFinalShare]` Only — Twin of st76)",
            feature_focus="Declarative `kind: Configuration` restricting `fairSharing.preemptionStrategies` to `[LessThanOrEqualToFinalShare]` (Rule S2-a only): evicts the `2` small `4`-TPU jobs (`2` evictions) while protecting the lumpy `24`-TPU job (`TargetNewShare=0.00 < PreemptorNewShare=0.375`)",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st78-config-fair-sharing-final-share-rule-only",
            setup_summary="Identical 8-node (`32`-TPU) cluster, `cohort-fs-cfg`, `ClusterQueue`s, and 7 workloads as `st76` and `st77`, changing ONLY `Configuration.fairSharing.preemptionStrategies` in `infra.json` to `['LessThanOrEqualToFinalShare']`.",
            expected_behavior="At `t=5s`, `js-alpha-0` (`0.125 <= 0.875`) and `js-alpha-1` (`0.25 <= 0.75`) satisfy `LessThanOrEqualToFinalShare` and evict `js-beta-small-0/1` (`2` evictions, `2` `p-alpha` jobs start in `< 2s` and hold `8` TPUs until `t=25.5s`). `js-alpha-2/3` (`0.375 > TargetNewShare=0.00`) fail `LessThanOrEqualToFinalShare`, and because `LessThanInitialShare` is disabled in `Configuration`, `js-beta-large-0` (`24` TPUs) runs uninterrupted to `t=24s` (`0` `p-beta-large` evictions) while `js-alpha-2/3` wait `~19s`.",
            expected_metrics=[
                "`Section 5.3 (Total Evictions & Preemptions)`: Exact **`2` `FairSharing` preemptions** (`Total Evictions = 2`, `Unique workloads evicted once = 2`, `0` `p-beta-large` evictions — strictly between `0` in `st77` and `3` in `st76`!).",
                "`Section 5.2 (Wait Time)`: `p-alpha` shows bimodal wait — `2` jobs `< 2.5s` and `2` jobs `~19s` (**`AVG ≈ 9s–12s`, `P50 <= 2.5s`, `P90 ≈ 18s–25s`**); `p-beta-large` has `WORKLOADS = 1` (`AVG < 1.5s`).",
                "`cluster-scraper`: Preserves `preemptionStrategies: ['LessThanOrEqualToFinalShare']` in `infra.json`.",
            ],
            must_not_happen=[
                "Any eviction of `p-beta-large` (`js-beta-large-0` evicted would mean `LessThanInitialShare` was still active despite `Configuration` removing it).",
                "`0` evictions (would mean `LessThanOrEqualToFinalShare` failed to run).",
            ],
            sec51_target="Cohort 72%–95%",
            sec52_target="p-alpha bimodal AVG ~9.5s–12s, P50 <= 2.5s",
            sec53_target="2 evictions (FairSharing=2, 24-TPU protected)",
            sec54_target="~60%–78%",
            infra_events=inf78,
            workflow_events=wf78,
            exec_events=ex78,
        )
    )

    def _build_afs_pair_fixture(
        config_obj: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        """Build identical 4-node (`16`-TPU) ClusterQueue + 2 LocalQueues + 16 Workloads for `st79` and `st80`."""
        inf: list[dict[str, Any]] = [make_infra_event_fn(config_obj)]
        for i in range(4):
            inf.append(make_infra_event_fn(_make_node(i, det_uid_fn)))
        inf.append(make_infra_event_fn(make_namespace_fn(DEFAULT_NS)))
        inf.append(make_infra_event_fn(make_flavor_fn(DEFAULT_FLAVOR)))
        # Equal numeric priority (100 == 100) so priority never biases ordering, while splitting Section 5.2 by queue!
        inf.append(make_infra_event_fn(make_priority_class_fn("p-heavy", 100)))
        inf.append(make_infra_event_fn(make_priority_class_fn("p-light", 100)))
        inf.append(
            make_infra_event_fn(
                _make_afs_cluster_queue(
                    "cq-afs",
                    det_uid_fn,
                    tpu_quota=16,
                    cpu_quota=64,
                    mem_gi_quota=256,
                    admission_mode="UsageBasedAdmissionFairSharing",
                )
            )
        )
        inf.append(make_infra_event_fn(_make_weighted_local_queue("lq-heavy", "cq-afs", det_uid_fn, weight="1")))
        inf.append(make_infra_event_fn(_make_weighted_local_queue("lq-light", "cq-afs", det_uid_fn, weight="1")))

        wf: list[dict[str, Any]] = []
        ex: list[dict[str, Any]] = []
        # lq-heavy submits 12 x 1-pod (4 TPU, 8s) jobs at t=0s (3 full cluster waves)
        for i in range(12):
            w_h, e_h = make_jobset_and_exec_events_fn(
                name=f"js-heavy-{i:02d}",
                arrival_sec=0.0,
                duration_sec=8.0,
                local_queue="lq-heavy",
                cluster_queue="cq-afs",
                num_pods=1,
                priority_class="p-heavy",
            )
            wf.append(w_h)
            ex.extend(e_h)
        # lq-light submits 4 x 1-pod (4 TPU, 8s) jobs 3 seconds later at t=3s (1 cluster wave)
        for i in range(4):
            w_l, e_l = make_jobset_and_exec_events_fn(
                name=f"js-light-{i:02d}",
                arrival_sec=3.0,
                duration_sec=8.0,
                local_queue="lq-light",
                cluster_queue="cq-afs",
                num_pods=1,
                priority_class="p-light",
            )
            wf.append(w_l)
            ex.extend(e_l)
        return inf, wf, ex

    # st79: Using Admission-Based Fair Sharing (AdmissionFairSharing Enabled in Configuration)
    inf79, wf79, ex79 = _build_afs_pair_fixture(
        _make_kueue_configuration(
            fair_sharing_strategies=["LessThanOrEqualToFinalShare", "LessThanInitialShare"],
            admission_fair_sharing={
                "usageHalfLifeTime": "20s",
                "usageSamplingInterval": "1s",
                "resourceWeights": {
                    "google.com/tpu": 1,
                    "cpu": 0,
                    "memory": 0,
                },
            },
        )
    )
    scenarios.append(
        scenario_spec_cls(
            scenario_id="st79",
            slug="config-admission-fair-sharing-enabled",
            category="16. Kueue Controller Configuration (`FairSharing` & `AdmissionFairSharing`)",
            title="Declarative Kueue `Configuration` Enabling Admission-Based Fair Sharing (`AdmissionFairSharing`)",
            feature_focus="Declarative `kind: Configuration` enabling `admissionFairSharing` (`usageHalfLifeTime: 20s`, `usageSamplingInterval: 1s`, `google.com/tpu: 1`): `lq-light` (`t=3s`, `0` prior usage) jumps ahead of `8` older `t=0s` `lq-heavy` backlog jobs at Wave 2 (`t=8s`, `wait ≈ 5s`)",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st79-config-admission-fair-sharing-enabled",
            setup_summary="4-node (`16`-TPU) cluster with `kind: Configuration` setting `admissionFairSharing` (`usageHalfLifeTime: 20s`, `usageSamplingInterval: 1s`, `resourceWeights: {google.com/tpu: 1, cpu: 0, memory: 0}`). `cq-afs` (`16` TPUs) sets `admissionScope.admissionMode: UsageBasedAdmissionFairSharing`; `lq-heavy` (`weight: 1`, `p-heavy=100`) submits `12` `4`-TPU `8s` jobs at `t=0s`; `lq-light` (`weight: 1`, `p-light=100`) submits `4` `4`-TPU `8s` jobs at `t=3s`.",
            expected_behavior="During Wave 1 (`0s–8s`), `lq-heavy` consumes `16` TPUs (`kueue_local_queue_admission_fair_sharing_usage{name='lq-heavy'} > 0`) while `lq-light` usage stays `0`. When Wave 1 finishes at `t=8s`, `AdmissionFairSharing` orders `lq-light` ahead of `lq-heavy`'s `8` older `t=0s` jobs (`0.0 + penalty < ~3.88`), running all `4` `lq-light` jobs in Wave 2 (`8s–16s`, `wait ≈ 5s`) before `lq-heavy` finishes in Waves 3–4 (`16s–32s`).",
            expected_metrics=[
                "`Section 1 Gauge (kueue_local_queue_admission_fair_sharing_usage)`: Actively emitted (`> 0`) for `lq-heavy` and `lq-light`.",
                "`Section 5.2 (Wait Time)` & `Section 3 (kueue_local_queue_admission_wait_time_seconds)`: **`p-light` (`lq-light`) waits `~5.5s–9s`** (`P50 ≈ 7.5s, AVG ≈ 7s–9s`), overtaking `p-heavy` (`AVG ≈ 16s–19.5s`).",
                "`Section 5.3`: **`0`** evictions (pure admission-time ordering, zero preemptions).",
                "`Section 5.4`: **`~75%–92%`** (`16/16` jobs complete in `~38s–45s`).",
            ],
            must_not_happen=[
                "`p-light` waiting `~25s–28s` behind all `12` `lq-heavy` jobs (would mean `admissionFairSharing` in `Configuration` was not applied).",
                "`kueue_local_queue_admission_fair_sharing_usage` missing or remaining `0` for `lq-heavy`.",
            ],
            sec51_target="75%–100% (16/16 TPUs across 4 waves)",
            sec52_target="p-light AVG ~7.5s < p-heavy AVG ~17.3s",
            sec53_target="0 evictions (admission-time ordering)",
            sec54_target="75%–92% (16 completed)",
            infra_events=inf79,
            workflow_events=wf79,
            exec_events=ex79,
        )
    )

    # st80: Not Using Admission-Based Fair Sharing (Exact A/B Twin of st79)
    inf80, wf80, ex80 = _build_afs_pair_fixture(
        _make_kueue_configuration(
            fair_sharing_strategies=["LessThanOrEqualToFinalShare", "LessThanInitialShare"],
            admission_fair_sharing=None,
        )
    )
    scenarios.append(
        scenario_spec_cls(
            scenario_id="st80",
            slug="config-admission-fair-sharing-disabled",
            category="16. Kueue Controller Configuration (`FairSharing` & `AdmissionFairSharing`)",
            title="Declarative Kueue `Configuration` Without Admission-Based Fair Sharing (FIFO Control — Twin of st79)",
            feature_focus="Direct A/B twin of `st79` omitting `admissionFairSharing` in `kind: Configuration`: even though `cq-afs` declares `UsageBasedAdmissionFairSharing`, Kueue disables AFS globally and falls back to FIFO (`p-light` waits `~27s–28s` in Wave 4)",
            recommended_cmd="python3 deploy/scripts/testrunner.py testscenarios/self-tests/st80-config-admission-fair-sharing-disabled",
            setup_summary="Identical 4-node (`16`-TPU) cluster, `cq-afs` (`admissionScope.admissionMode: UsageBasedAdmissionFairSharing`), `lq-heavy` / `lq-light` (`weight: 1`), and 16 workloads (`12` `p-heavy` at `t=0s`, `4` `p-light` at `t=3s`) as `st79`, changing ONLY `kind: Configuration` in `infra.json` to omit `admissionFairSharing`.",
            expected_behavior="Because `cfg.AdmissionFairSharing == nil` in the chamber's deployed `Configuration`, Kueue ignores `LocalQueue` AFS usage and orders equal-priority (`100 == 100`) workloads strictly by creation timestamp (`t=0s < t=3s`). All `12` `lq-heavy` jobs run first across Waves 1–3 (`0s–24s`), pushing all `4` `lq-light` jobs to Wave 4 (`24s–32s`, `AVG ≈ 27s–28s`).",
            expected_metrics=[
                "`Section 5.2 (Wait Time)` & `Section 3 (kueue_local_queue_admission_wait_time_seconds)`: **`p-light` (`lq-light`) wait jumps from `~7.5s` (`st79`) to `AVG ≈ 25s–29s` (`P50 ≈ 30s`)**, while `p-heavy` `AVG` wait drops to `~9.5s–11.5s`.",
                "`Section 4 (Not Emitted)`: `kueue_local_queue_admission_fair_sharing_usage` is unrecorded (AFS sampling inactive).",
                "`Section 5.3`: **`0`** evictions.",
                "`Section 5.4`: **`~75%–92%`** (`16/16` jobs complete in `~38s–45s`).",
            ],
            must_not_happen=[
                "`p-light` jumping ahead of `t=0s` `p-heavy` jobs into Wave 2 (`wait < 12s`) when `admissionFairSharing` is omitted from `Configuration`.",
            ],
            sec51_target="75%–100% (16/16 TPUs across 4 waves)",
            sec52_target="p-light AVG ~27.8s > p-heavy AVG ~10.4s",
            sec53_target="0 evictions",
            sec54_target="75%–92% (16 completed)",
            infra_events=inf80,
            workflow_events=wf80,
            exec_events=ex80,
        )
    )

    return scenarios


KUEUE_CONFIG_PAIRINGS: dict[str, list[dict[str, str]]] = {
    "st76": [
        {
            "peer_id": "st77",
            "relationship": "Exact Global Fair-Sharing A/B Twin (`fairSharing` Enabled `st76` vs. Disabled Globally `st77` via `Configuration`)",
            "knob_changed": "`Configuration.fairSharing` enabled (`[LessThanOrEqualToFinalShare, LessThanInitialShare]`, `st76`) vs. omitted (`cfg.FairSharing == nil`, `st77`) in `infra.json` on identical queues (`reclaimWithinCohort: Any`) and workloads.",
            "cross_check": "`Sec 5.3` flips from **`3` `FairSharing` preemptions (`st76`)** to **`0` evictions (`st77`)**, and `Sec 5.2` `p-alpha` wait jumps from **`< 2.5s` (`st76`)** to **`~19s` (`st77`)** without any CLI flags.",
        },
        {
            "peer_id": "st78",
            "relationship": "Exact Fair-Sharing Strategy Rule Twin (`[LessThanOrEqualToFinalShare, LessThanInitialShare]` `st76` vs. `[LessThanOrEqualToFinalShare]` Only `st78`)",
            "knob_changed": "Removing `LessThanInitialShare` from `Configuration.fairSharing.preemptionStrategies` in `st78`.",
            "cross_check": "`Sec 5.3` drops from **`3` preemptions (`st76`)** to **`2` preemptions (`st78`)**, protecting `cq-beta`'s `24`-TPU job (`TargetNewShare=0.00 < PreemptorNewShare=0.375`) while shifting `p-alpha` wait from `< 2.5s` (`4/4` immediate) to bimodal `AVG ~10.5s` (`2/4` immediate, `2/4` `~19s`).",
        },
    ],
    "st77": [
        {
            "peer_id": "st76",
            "relationship": "Exact Global Fair-Sharing A/B Twin (`fairSharing` Disabled Globally `st77` vs. Enabled `st76`)",
            "knob_changed": "`Configuration` omits `fairSharing` (`st77`) vs. enables `fairSharing` (`st76`).",
            "cross_check": "`st77` proves the `Configuration` statement in `infra.json` disables Fair Sharing in the deployed chamber (`0` evictions, `p-alpha` wait `~19s`) even when `ClusterQueue`s specify `reclaimWithinCohort: Any` and `fairSharing.weight: 1`.",
        },
        {
            "peer_id": "st78",
            "relationship": "Global Fair-Sharing Disabled (`st77`) vs. Strict `LessThanOrEqualToFinalShare` Only (`st78`)",
            "knob_changed": "`fairSharing` omitted (`0` strategies, `st77`) vs. `preemptionStrategies: [LessThanOrEqualToFinalShare]` (`st78`).",
            "cross_check": "Across `st77 -> st78 -> st76`, `FairSharing` preemptions step monotonically **`0 -> 2 -> 3`** and immediate `p-alpha` admissions step **`0/4 -> 2/4 -> 4/4`**.",
        },
    ],
    "st78": [
        {
            "peer_id": "st76",
            "relationship": "Exact Fair-Sharing Strategy Rule Twin (`[LessThanOrEqualToFinalShare]` Only `st78` vs. Default Dual-Rule `st76`)",
            "knob_changed": "`Configuration.fairSharing.preemptionStrategies` restricted to `['LessThanOrEqualToFinalShare']` (`st78`) vs. `['LessThanOrEqualToFinalShare', 'LessThanInitialShare']` (`st76`).",
            "cross_check": "`st78` evicts only the `2` `4`-TPU jobs (`FairSharing=2`) and protects the `24`-TPU job, whereas `st76` additionally evicts the `24`-TPU job via `LessThanInitialShare` (`FairSharing=3`).",
        },
        {
            "peer_id": "st77",
            "relationship": "Strict `LessThanOrEqualToFinalShare` Fair Sharing (`st78`) vs. Global Fair Sharing Disabled (`st77`)",
            "knob_changed": "`preemptionStrategies: [LessThanOrEqualToFinalShare]` (`st78`) vs. `fairSharing` disabled (`st77`).",
            "cross_check": "`st78` reclaims `8` TPUs (`2` evictions) immediately for `cq-alpha`, whereas `st77` reclaims `0` TPUs (`0` evictions).",
        },
    ],
    "st79": [
        {
            "peer_id": "st80",
            "relationship": "Exact Admission-Based Fair Sharing A/B Twin (`admissionFairSharing` Enabled `st79` vs. Disabled `st80` via `Configuration`)",
            "knob_changed": "`Configuration.admissionFairSharing` configured (`usageHalfLifeTime: 20s, usageSamplingInterval: 1s, google.com/tpu: 1`, `st79`) vs. omitted (`st80`) on identical `cq-afs` (`UsageBasedAdmissionFairSharing`), `LocalQueue`s, and 16 workloads.",
            "cross_check": "In `st79`, `kueue_local_queue_admission_fair_sharing_usage` is `> 0` and all `4` `lq-light` (`p-light`) jobs jump into Wave 2 (**`wait ≈ 5s–6.5s`**); in `st80`, FIFO holds `p-light` until Wave 4 (**`wait ≈ 21s–24s`**, `+16s` delta) with `0` evictions in both.",
        },
    ],
    "st80": [
        {
            "peer_id": "st79",
            "relationship": "Exact Admission-Based Fair Sharing A/B Twin (`admissionFairSharing` Disabled `st80` vs. Enabled `st79` via `Configuration`)",
            "knob_changed": "`Configuration` omits `admissionFairSharing` (`st80`) vs. enables `admissionFairSharing` (`st79`).",
            "cross_check": "Proves `ClusterQueue.spec.admissionScope.admissionMode: UsageBasedAdmissionFairSharing` is inert without `Configuration.admissionFairSharing`: `p-light` waits `~21s–24s` (`st80`) vs. `~5s–6.5s` (`st79`).",
        },
    ],
}
