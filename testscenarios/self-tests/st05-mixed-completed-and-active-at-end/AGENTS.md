# Agent Execution & Cross-Examination Guide — `st05-mixed-completed-and-active-at-end`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st05-mixed-completed-and-active-at-end`
- **Category**: 1. Baseline Saturation & Accounting
- **Title**: 50% Completed Workloads + 50% Active-at-End Workloads Decomposition
- **Primary Verification Target**: Exact additive split `COMPLETED ONLY (~48%) + ACTIVE AT END (~50%) = EFFECTIVE SAT (~98%)` in Section 5.4
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st05` (`--namespace kueue-optimizer-chamber-st05`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16405` | Prometheus `19052` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st35-t0-infinite-workload-and-active-at-end`](../st35-t0-infinite-workload-and-active-at-end/AGENTS.md), [`st15-preemption-badput-collapse-stress`](../st15-preemption-badput-collapse-stress/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST05 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st05' (API port 16405, Prom port 19052):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st05-mixed-completed-and-active-at-end --duration 42s --namespace kueue-optimizer-chamber-st05 --port 16405 > /tmp/st05.log 2>&1 &
PID_ST05=$!

# Simultaneously launch paired ST35 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st35' (API port 16435, Prom port 19082):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st35-t0-infinite-workload-and-active-at-end --duration 40s --namespace kueue-optimizer-chamber-st35 --port 16435 > /tmp/st35.log 2>&1 &
PID_ST35=$!

# Simultaneously launch paired ST15 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st15' (API port 16415, Prom port 19062):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st15-preemption-badput-collapse-stress --duration 41s --namespace kueue-optimizer-chamber-st15 --port 16415 > /tmp/st15.log 2>&1 &
PID_ST15=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST05" "$PID_ST35" "$PID_ST15"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST05 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st05' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st05-mixed-completed-and-active-at-end --duration 42s --namespace kueue-optimizer-chamber-st05 --port 16405 | tee /tmp/st05.log

# 2. Run paired ST35 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st35':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st35-t0-infinite-workload-and-active-at-end --duration 40s --namespace kueue-optimizer-chamber-st35 --port 16435 | tee /tmp/st35.log

# 3. Run paired ST15 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st15':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st15-preemption-badput-collapse-stress --duration 41s --namespace kueue-optimizer-chamber-st15 --port 16415 | tee /tmp/st15.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st05-mixed-completed-and-active-at-end --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st05`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st05-mixed-completed-and-active-at-end` (`kueue-optimizer-chamber-st05`) ↔ [`st35-t0-infinite-workload-and-active-at-end`](../st35-t0-infinite-workload-and-active-at-end/AGENTS.md) (`kueue-optimizer-chamber-st35`) — Scheduled Active-at-End (`st05`) vs. Infinite `t0` `PodGroup` Active-at-End (`st35`) Pair

- **Controlled Variable Changed**: `st05` (`--duration 42s`) runs 4 long `120s` scheduled JobSets active at `t_end`, whereas `st35` (`--duration 40s`) runs 4 infinite `t0` `PodGroup`s (`is_infinite=True`) active at `t_end`.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st35-t0-infinite-workload-and-active-at-end --duration 40s --namespace kueue-optimizer-chamber-st35 --port 16435`
- **Cross-Examination Assertion**: Both scenarios split `Sec 5.4` into `COMPLETED ONLY ~45%–48% + ACTIVE AT END ~48%–50% ≈ 95%–98%`, while `st35` additionally keeps `FULL-DUR SAT ≈ 95%` without `864000s` inflation.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `95%–100%` (`st05`) vs. `~92%–100%` (`st35`)
  - `Sec 5.2 Wait Time`: `< 1.5s` (`st05`) vs. `< 1.5s` (`st35`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st05`) vs. `0 evictions` (`st35`)
  - `Sec 5.4 Effective Sat`: `COMPLETED ~47% + ACTIVE ~49% = ~96%` (`st05`) vs. `COMPLETED ~45% + ACTIVE ~50% = ~95% (FULL-DUR ~95%)` (`st35`)

### Pair: `st05-mixed-completed-and-active-at-end` (`kueue-optimizer-chamber-st05`) ↔ [`st15-preemption-badput-collapse-stress`](../st15-preemption-badput-collapse-stress/AGENTS.md) (`kueue-optimizer-chamber-st15`) — Active-at-End Uninterrupted Work (`st05`) vs. Evicted Incomplete Work (`st15`) Pair

- **Controlled Variable Changed**: Incomplete jobs in `st05` are still actively running at `t_end` (counted in `ACTIVE AT END`), whereas incomplete jobs in `st15` were preempted at `t=26s` (excluded as badput).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st15-preemption-badput-collapse-stress --duration 41s --namespace kueue-optimizer-chamber-st15 --port 16415`
- **Cross-Examination Assertion**: `Sec 5.4(st05) ≈ 93%–100%` vs. `Sec 5.4(st15) ≈ 32%–40%` (`>50pp` delta), proving `Sec 5.4` cleanly separates healthy in-flight work at test end from aborted/preempted work.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `95%–100%` (`st05`) vs. `93%–99% (raw occupancy)` (`st15`)
  - `Sec 5.2 Wait Time`: `< 1.5s` (`st05`) vs. `p-urgent wait < 1.5s` (`st15`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st05`) vs. `8 evictions` (`st15`)
  - `Sec 5.4 Effective Sat`: `COMPLETED ~47% + ACTIVE ~49% = ~96%` (`st05`) vs. `~32%–40% (~60pp badput gap)` (`st15`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 8-node cluster (`32` TPUs). Half the cluster (`4` nodes = `16` TPUs) runs two waves of short completed jobs (`20s` + `18s`); the other half (`4` nodes = `16` TPUs) runs 4 long `120s` jobs that are still actively running when `--duration 42s` ends the evaluation window.
- **Expected Runtime Behavior**: 8 short jobs complete cleanly before `t=42s` (`completed=8 [8 pods]`), while all 4 long jobs remain actively running at the exact end of the test (`active_at_end=4 [4 pods]`). Both groups contribute to `EFFECTIVE SAT`.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `95%–100%` |
| **Section `5.2` Queue Wait Time** | `< 1.5s` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `COMPLETED ~47% + ACTIVE ~49% = ~96%` |

### Detailed Metric Checkpoints

- `Section 5.4` columns for `google.com/tpu`: `COMPLETED ONLY ≈ 44%–50%`, `ACTIVE AT END ≈ 47%–50%`, and `EFFECTIVE SAT = COMPLETED ONLY + ACTIVE AT END ≈ 92%–100%`.
- `Section 5.4` subtitle: `Qualifying Workloads: 12 (completed=8 [8 pods], active_at_end=4 [4 pods]) | Excluded (preempted/reclaimed/failed): 0 [0 pods]`.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ Long jobs still actively running at `t_end` being misclassified as `Excluded` (`ACTIVE AT END == 0%`).
- ❌ Active jobs credited with their full `120s` declared duration in `EFFECTIVE SAT` instead of elapsed runtime `t_end - admit_time` (`42s`), which would falsely push `EFFECTIVE SAT` above `100%`.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
