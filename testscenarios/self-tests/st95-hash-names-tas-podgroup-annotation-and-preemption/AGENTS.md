# Agent Execution & Cross-Examination Guide — `st95-hash-names-tas-podgroup-annotation-and-preemption`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st95-hash-names-tas-podgroup-annotation-and-preemption`
- **Category**: 19. Anonymization & Sensitive-Data Hashing (--hash-names)
- **Title**: `--hash-names` 7-Level TAS, Annotation-Only `t0` `PodGroup` & `LowerPriority` Preemption
- **Primary Verification Target**: Verifies `--hash-names` works end-to-end with 7-level TAS topology placement, annotation-only `kueue.x-k8s.io/pod-group-name: h-<32hex>` `t0` `PodGroup`s, and `LowerPriority` preemption of hashed workloads
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st95` (`--namespace kueue-optimizer-chamber-st95`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16495` | Prometheus `19142` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st40-t0-tas-topology-domain-occupancy`](../st40-t0-tas-topology-domain-occupancy/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST95 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st95' (API port 16495, Prom port 19142):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st95-hash-names-tas-podgroup-annotation-and-preemption --no-skip-initial-offset --namespace kueue-optimizer-chamber-st95 --port 16495 > /tmp/st95.log 2>&1 &
PID_ST95=$!

# Simultaneously launch paired ST40 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st40' (API port 16440, Prom port 19087):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st40-t0-tas-topology-domain-occupancy --namespace kueue-optimizer-chamber-st40 --port 16440 > /tmp/st40.log 2>&1 &
PID_ST40=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST95" "$PID_ST40"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST95 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st95' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st95-hash-names-tas-podgroup-annotation-and-preemption --no-skip-initial-offset --namespace kueue-optimizer-chamber-st95 --port 16495 | tee /tmp/st95.log

# 2. Run paired ST40 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st40':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st40-t0-tas-topology-domain-occupancy --namespace kueue-optimizer-chamber-st40 --port 16440 | tee /tmp/st40.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st95-hash-names-tas-podgroup-annotation-and-preemption --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st95`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st95-hash-names-tas-podgroup-annotation-and-preemption` (`kueue-optimizer-chamber-st95`) ↔ [`st40-t0-tas-topology-domain-occupancy`](../st40-t0-tas-topology-domain-occupancy/AGENTS.md) (`kueue-optimizer-chamber-st40`) — Hashed TAS + Annotation-Only `t0` `PodGroup` + Preemption (`st95`) vs. Plaintext `t0` TAS Baseline (`st40`)

- **Controlled Variable Changed**: Annotation-only `kueue.x-k8s.io/pod-group-name: h-<32hex>` `t0` `PodGroup`s + hashed TAS `JobSet`s with `LowerPriority` preemption (`st95`) vs. plaintext `t0` TAS (`st40`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st40-t0-tas-topology-domain-occupancy --namespace kueue-optimizer-chamber-st40 --port 16440`
- **Cross-Examination Assertion**: `st95` groups both annotation-only hashed `PodGroup`s into `[TAS]` `InitialWorkloadPodGroup`s (`Initial Infra Pods: 0`) and triggers `1` `Preempted` eviction so `p-high` admits in `< 2.5s`.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `~85%–100% (16/16 TAS TPUs)` (`st95`) vs. `~72%–82% (100% during 0s–20s)` (`st40`)
  - `Sec 5.2 Wait Time`: `p-high < 2.5s; p-low re-admits` (`st95`) vs. `4n < 1.5s, 8n ~26s (AVG ~13s)` (`st40`)
  - `Sec 5.3 Evictions`: `1 Preempted (p-low)` (`st95`) vs. `0 evictions` (`st40`)
  - `Sec 5.4 Effective Sat`: `5 completed (TAS + annotation PG)` (`st95`) vs. `~70%–82% (4 TAS workloads, 24 pods)` (`st40`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 4 TAS nodes (`16` TPUs, `st-slice-topology`), `cq-hash-tas` (`16` TPUs, `withinClusterQueue: LowerPriority`). At `t0`: 2 annotation-only hashed `PodGroup`s (`p-low`, `4` TPUs × `12s`, `st-node-00..01`). At `t=0s`: 2 hashed `p-low` TAS `JobSet`s (`4` TPUs × `16s`) fill `16/16` TPUs. At `t=+4s`: 1 hashed `p-high` TAS `JobSet` (`4` TPUs × `8s`) arrives.
- **Expected Runtime Behavior**: Loader groups the 2 annotation-only hashed `Pod`s into 2 pre-admitted TAS `InitialWorkloadPodGroup`s (occupying `st-node-00..01`). At `t=+4s`, the `p-high` hashed TAS `JobSet` preempts 1 `p-low` hashed workload (`wait < 2.5s`), runs for `8s`, and the preempted `p-low` workload re-admits and completes cleanly.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `~85%–100% (16/16 TAS TPUs)` |
| **Section `5.2` Queue Wait Time** | `p-high < 2.5s; p-low re-admits` |
| **Section `5.3` Evictions & Reclaims** | `1 Preempted (p-low)` |
| **Section `5.4` Effective Saturation** | `5 completed (TAS + annotation PG)` |

### Detailed Metric Checkpoints

- `Pre-Run Outline`: `Initial Kueue Workloads: 2 (2 pods)` (both `[TAS]`), `Initial Infra Pods: 0`.
- `Section 5.1`: `AVG ~85%–100%` (`16/16` TPUs).
- `Section 5.2`: `p-high` `wait < 2.5s` (1 WL); `p-low` records the 2 initial arrivals + 1 re-admission.
- `Section 5.3`: **`1`** `Preempted` eviction (`p-low`).
- `Section 5.4`: all `5` hashed workloads complete (`0` unadmitted).

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ Failing to group annotation-only `kueue.x-k8s.io/pod-group-name: h-<32hex>` pods into `InitialWorkloadPodGroup`s (`Initial Infra Pods` MUST be `0`).
- ❌ `p-high` waiting `>= 8s` instead of preempting a `p-low` hashed workload.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
