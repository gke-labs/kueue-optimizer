# Agent Execution & Cross-Examination Guide — `st12-preempt-within-cq-lower-priority`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st12-preempt-within-cq-lower-priority`
- **Category**: 3. Intra-CQ Preemption Matrix
- **Title**: Immediate Intra-CQ Preemption (`withinClusterQueue: LowerPriority` — Twin of st11)
- **Primary Verification Target**: Direct A/B twin of `st11` enabling `withinClusterQueue: LowerPriority`: verifies eviction counters, `p-high` near-zero wait, and requeued completion
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st12` (`--namespace kueue-optimizer-chamber-st12`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16412` | Prometheus `19059` (collision-free across all 95 scenarios `st01`–`st95`)
- **Mandatory Paired Counterpart(s)**: [`st11-preempt-within-cq-never`](../st11-preempt-within-cq-never/AGENTS.md), [`st37-t0-preempt-within-cq-lower-priority-resubmit`](../st37-t0-preempt-within-cq-lower-priority-resubmit/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st95`) and non-overlapping port pair (`16401/19048` .. `16495/19142`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST12 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st12' (API port 16412, Prom port 19059):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st12-preempt-within-cq-lower-priority --namespace kueue-optimizer-chamber-st12 --port 16412 > /tmp/st12.log 2>&1 &
PID_ST12=$!

# Simultaneously launch paired ST11 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st11' (API port 16411, Prom port 19058):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st11-preempt-within-cq-never --namespace kueue-optimizer-chamber-st11 --port 16411 > /tmp/st11.log 2>&1 &
PID_ST11=$!

# Simultaneously launch paired ST37 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st37' (API port 16437, Prom port 19084):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st37-t0-preempt-within-cq-lower-priority-resubmit --no-skip-initial-offset --namespace kueue-optimizer-chamber-st37 --port 16437 > /tmp/st37.log 2>&1 &
PID_ST37=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST12" "$PID_ST11" "$PID_ST37"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST12 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st12' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st12-preempt-within-cq-lower-priority --namespace kueue-optimizer-chamber-st12 --port 16412 | tee /tmp/st12.log

# 2. Run paired ST11 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st11':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st11-preempt-within-cq-never --namespace kueue-optimizer-chamber-st11 --port 16411 | tee /tmp/st11.log

# 3. Run paired ST37 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st37':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st37-t0-preempt-within-cq-lower-priority-resubmit --no-skip-initial-offset --namespace kueue-optimizer-chamber-st37 --port 16437 | tee /tmp/st37.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st12-preempt-within-cq-lower-priority --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st12`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st12-preempt-within-cq-lower-priority` (`kueue-optimizer-chamber-st12`) ↔ [`st11-preempt-within-cq-never`](../st11-preempt-within-cq-never/AGENTS.md) (`kueue-optimizer-chamber-st11`) — Exact A/B Twin (`withinClusterQueue: LowerPriority` vs. `Never`)

- **Controlled Variable Changed**: ONLY `withinClusterQueue` changes (`LowerPriority` in `st12` vs. `Never` in `st11`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st11-preempt-within-cq-never --namespace kueue-optimizer-chamber-st11 --port 16411`
- **Cross-Examination Assertion**: `st12` MUST emit exact `4` evictions (`Unique workloads evicted once = 4`) and `< 2.0s` wait for `p-high` vs. `0` evictions and `~22s` wait in `st11`.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `80%–95%` (`st12`) vs. `~77%–82% (1,280/1,600 TPU-s)` (`st11`)
  - `Sec 5.2 Wait Time`: `p-high wait < 2.0s` (`st12`) vs. `p-high wait ~22s` (`st11`)
  - `Sec 5.3 Evictions`: `4 evictions (4 unique)` (`st12`) vs. `0 evictions` (`st11`)
  - `Sec 5.4 Effective Sat`: `~68%–88% (after requeue)` (`st12`) vs. `~77%–82% (Excluded: 0)` (`st11`)

### Pair: `st12-preempt-within-cq-lower-priority` (`kueue-optimizer-chamber-st12`) ↔ [`st37-t0-preempt-within-cq-lower-priority-resubmit`](../st37-t0-preempt-within-cq-lower-priority-resubmit/AGENTS.md) (`kueue-optimizer-chamber-st37`) — Scheduled `JobSet` Preemption (`st12`) vs. Bootstrapped `t0` `PodGroup` Preemption + Resubmit (`st37`) Pair

- **Controlled Variable Changed**: `st12` preempts controller-owned `JobSet` Workloads (in-place requeue), whereas `st37` preempts bootstrapped `t0` `PodGroup` Workloads (GC + dynamic parent `JobSet` resubmission).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st37-t0-preempt-within-cq-lower-priority-resubmit --no-skip-initial-offset --namespace kueue-optimizer-chamber-st37 --port 16437`
- **Cross-Examination Assertion**: Both `st12` and `st37` record exact `4` `InClusterQueue` evictions, `< 2.5s` `p-high` wait, and `12` completed workloads (`Excluded: 0`).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `80%–95%` (`st12`) vs. `~82%–96%` (`st37`)
  - `Sec 5.2 Wait Time`: `p-high wait < 2.0s` (`st12`) vs. `p-high wait < 2.5s` (`st37`)
  - `Sec 5.3 Evictions`: `4 evictions (4 unique)` (`st12`) vs. `4 evictions (InClusterQueue, p-low)` (`st37`)
  - `Sec 5.4 Effective Sat`: `~68%–88% (after requeue)` (`st12`) vs. `~70%–86% (12 completed after resubmit)` (`st37`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: Identical 8-node cluster and identical 12 workloads as `st11`, changing ONLY `withinClusterQueue` from `Never` to `LowerPriority`.
- **Expected Runtime Behavior**: At `t=8s`, arrival of the 4 `p-high (1000)` jobs immediately preempts 4 of the 8 running `p-low (100)` jobs (`kueue_evicted_workloads_total` increments by `4`). The 4 `p-high` jobs start immediately (`wait < 2s`), finish at `t=28s`, and the 4 preempted `p-low` jobs are automatically re-admitted and run to completion.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `80%–95%` |
| **Section `5.2` Queue Wait Time** | `p-high wait < 2.0s` |
| **Section `5.3` Evictions & Reclaims** | `4 evictions (4 unique)` |
| **Section `5.4` Effective Saturation** | `~68%–88% (after requeue)` |

### Detailed Metric Checkpoints

- `Section 2 Counter (kueue_evicted_workloads_total)`: **`4`** evictions with `reason='Preempted'`.
- `Section 3 Histogram (kueue_pods_ready_to_evicted_time_seconds)`: `P50 ≈ 8s` (evicted `8s` after starting at `t=0s`).
- `Section 5.2 (Wait Time)`: `p-high` wait drops from `~22s` (`st11`) down to **`< 2.0s`**.
- `Section 5.3 (Total Evictions and Reclaims)`: `Total Evictions = 4`, `Unique workloads evicted once = 4` (no thrashing).

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ `Section 5.3 Total Evictions == 0` (would mean intra-CQ preemption failed to trigger).
- ❌ `p-high` waiting `> 5s` while `p-low` jobs occupy the queue.
- ❌ `Total Evictions > 6` (would indicate spurious extra evictions beyond the 4 required slots).

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
