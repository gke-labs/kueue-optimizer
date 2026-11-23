# Agent Execution & Cross-Examination Guide — `st11-preempt-within-cq-never`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st11-preempt-within-cq-never`
- **Category**: 3. Intra-CQ Preemption Matrix
- **Title**: Intra-CQ Preemption Disabled Control (`withinClusterQueue: Never`)
- **Primary Verification Target**: Baseline control for `st12`: high-priority arrivals wait patiently without preempting running low-priority jobs
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st11` (`--namespace kueue-optimizer-chamber-st11`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16411` | Prometheus `19058` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st12-preempt-within-cq-lower-priority`](../st12-preempt-within-cq-lower-priority/AGENTS.md), [`st36-t0-preempt-within-cq-never`](../st36-t0-preempt-within-cq-never/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST11 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st11' (API port 16411, Prom port 19058):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st11-preempt-within-cq-never --namespace kueue-optimizer-chamber-st11 --port 16411 > /tmp/st11.log 2>&1 &
PID_ST11=$!

# Simultaneously launch paired ST12 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st12' (API port 16412, Prom port 19059):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st12-preempt-within-cq-lower-priority --namespace kueue-optimizer-chamber-st12 --port 16412 > /tmp/st12.log 2>&1 &
PID_ST12=$!

# Simultaneously launch paired ST36 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st36' (API port 16436, Prom port 19083):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st36-t0-preempt-within-cq-never --no-skip-initial-offset --namespace kueue-optimizer-chamber-st36 --port 16436 > /tmp/st36.log 2>&1 &
PID_ST36=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST11" "$PID_ST12" "$PID_ST36"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST11 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st11' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st11-preempt-within-cq-never --namespace kueue-optimizer-chamber-st11 --port 16411 | tee /tmp/st11.log

# 2. Run paired ST12 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st12':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st12-preempt-within-cq-lower-priority --namespace kueue-optimizer-chamber-st12 --port 16412 | tee /tmp/st12.log

# 3. Run paired ST36 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st36':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st36-t0-preempt-within-cq-never --no-skip-initial-offset --namespace kueue-optimizer-chamber-st36 --port 16436 | tee /tmp/st36.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st11-preempt-within-cq-never --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st11`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st11-preempt-within-cq-never` (`kueue-optimizer-chamber-st11`) ↔ [`st12-preempt-within-cq-lower-priority`](../st12-preempt-within-cq-lower-priority/AGENTS.md) (`kueue-optimizer-chamber-st12`) — Exact A/B Twin (`withinClusterQueue: Never` vs. `LowerPriority`)

- **Controlled Variable Changed**: ONLY `ClusterQueue.spec.preemption.withinClusterQueue` changes (`Never` in `st11` vs. `LowerPriority` in `st12`); all 8 nodes, 8 `p-low` jobs (`t=0s`), and 4 `p-high` jobs (`t=8s`) are 100% identical.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st12-preempt-within-cq-lower-priority --namespace kueue-optimizer-chamber-st12 --port 16412`
- **Cross-Examination Assertion**: `Sec 5.3 Evictions` flips from `0` (`st11`) to `4` (`st12`), and `Sec 5.2` wait time for `p-high (1000)` drops from `~22s` (`st11`) to `< 2.0s` (`st12`).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `~77%–82% (1,280/1,600 TPU-s)` (`st11`) vs. `80%–95%` (`st12`)
  - `Sec 5.2 Wait Time`: `p-high wait ~22s` (`st11`) vs. `p-high wait < 2.0s` (`st12`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st11`) vs. `4 evictions (4 unique)` (`st12`)
  - `Sec 5.4 Effective Sat`: `~77%–82% (Excluded: 0)` (`st11`) vs. `~68%–88% (after requeue)` (`st12`)

### Pair: `st11-preempt-within-cq-never` (`kueue-optimizer-chamber-st11`) ↔ [`st36-t0-preempt-within-cq-never`](../st36-t0-preempt-within-cq-never/AGENTS.md) (`kueue-optimizer-chamber-st36`) — Fresh `t=0s` Non-Preemptive Control (`st11`) vs. Bootstrapped `t0` `PodGroup` Control (`st36`) Pair

- **Controlled Variable Changed**: `st11` submits 8 `NotAdmitted` `p-low` JobSets at `t=0s`, whereas `st36` bootstraps 8 pre-admitted `t0` `p-low` `PodGroup`s.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st36-t0-preempt-within-cq-never --no-skip-initial-offset --namespace kueue-optimizer-chamber-st36 --port 16436`
- **Cross-Examination Assertion**: Both scenarios report `0` evictions and hold incoming `p-high` workloads in queue until the low-priority incumbents finish.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `~77%–82% (1,280/1,600 TPU-s)` (`st11`) vs. `~75%–82%` (`st36`)
  - `Sec 5.2 Wait Time`: `p-high wait ~22s` (`st11`) vs. `p-high wait ~18s–20s` (`st36`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st11`) vs. `0 evictions` (`st36`)
  - `Sec 5.4 Effective Sat`: `~77%–82% (Excluded: 0)` (`st11`) vs. `~74%–82% (12 completed, Excluded: 0)` (`st36`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 8-node (`32`-TPU) cluster, `withinClusterQueue: Never`. 8 `p-low (100)` jobs (`30s`) fill all `32` TPUs at `t=0s`. 4 `p-high (1000)` jobs (`20s`) arrive at `t=8s`.
- **Expected Runtime Behavior**: All 8 `p-low` jobs run uninterrupted from `0s` to `30s`. All 4 `p-high` jobs wait `22s` (`t=8s -> t=30s`), then run from `30s` to `50s`. Zero work is lost.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `~77%–82% (1,280/1,600 TPU-s)` |
| **Section `5.2` Queue Wait Time** | `p-high wait ~22s` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `~77%–82% (Excluded: 0)` |

### Detailed Metric Checkpoints

- `Section 5.2 (Wait Time)`: `p-low` waits `~0s`, while `p-high` waits **`~22s`** (intentional non-preemptive wait).
- `Section 5.3 (Evictions)`: Strictly **`0`**.
- `Section 5.4 (Effective Saturation)`: All 12 jobs complete (`1,280 TPU-s` over `50s × 32 = 1,600 TPU-s`) → **`EFFECTIVE SAT ≈ 77%–82%`** with `Excluded (preempted/reclaimed/failed): 0 [0 pods]`.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ Any eviction (`Section 5.3 > 0`) when `withinClusterQueue: Never`.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
