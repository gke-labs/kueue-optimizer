# Agent Execution & Cross-Examination Guide — `st37-t0-preempt-within-cq-lower-priority-resubmit`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st37-t0-preempt-within-cq-lower-priority-resubmit`
- **Category**: 8. Initial t0 Workloads — Preemption & TAS
- **Title**: Active Intra-CQ Preemption of `t0` `PodGroup`s & Dynamic Parent `JobSet` Resubmission (Twin of st36)
- **Primary Verification Target**: Direct A/B twin of `st36` with `withinClusterQueue: LowerPriority`: verifies Kueue preempts 4 `t0` `PodGroup`s, `testrunner.py` dynamically resubmits their 4 parent `JobSet`s without name collisions, and all 12 complete
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st37` (`--namespace kueue-optimizer-chamber-st37`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16437` | Prometheus `19084` (collision-free across all 95 scenarios `st01`–`st95`)
- **Mandatory Paired Counterpart(s)**: [`st36-t0-preempt-within-cq-never`](../st36-t0-preempt-within-cq-never/AGENTS.md), [`st12-preempt-within-cq-lower-priority`](../st12-preempt-within-cq-lower-priority/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st95`) and non-overlapping port pair (`16401/19048` .. `16495/19142`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST37 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st37' (API port 16437, Prom port 19084):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st37-t0-preempt-within-cq-lower-priority-resubmit --no-skip-initial-offset --namespace kueue-optimizer-chamber-st37 --port 16437 > /tmp/st37.log 2>&1 &
PID_ST37=$!

# Simultaneously launch paired ST36 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st36' (API port 16436, Prom port 19083):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st36-t0-preempt-within-cq-never --no-skip-initial-offset --namespace kueue-optimizer-chamber-st36 --port 16436 > /tmp/st36.log 2>&1 &
PID_ST36=$!

# Simultaneously launch paired ST12 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st12' (API port 16412, Prom port 19059):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st12-preempt-within-cq-lower-priority --namespace kueue-optimizer-chamber-st12 --port 16412 > /tmp/st12.log 2>&1 &
PID_ST12=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST37" "$PID_ST36" "$PID_ST12"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST37 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st37' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st37-t0-preempt-within-cq-lower-priority-resubmit --no-skip-initial-offset --namespace kueue-optimizer-chamber-st37 --port 16437 | tee /tmp/st37.log

# 2. Run paired ST36 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st36':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st36-t0-preempt-within-cq-never --no-skip-initial-offset --namespace kueue-optimizer-chamber-st36 --port 16436 | tee /tmp/st36.log

# 3. Run paired ST12 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st12':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st12-preempt-within-cq-lower-priority --namespace kueue-optimizer-chamber-st12 --port 16412 | tee /tmp/st12.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st37-t0-preempt-within-cq-lower-priority-resubmit --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st37`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st37-t0-preempt-within-cq-lower-priority-resubmit` (`kueue-optimizer-chamber-st37`) ↔ [`st36-t0-preempt-within-cq-never`](../st36-t0-preempt-within-cq-never/AGENTS.md) (`kueue-optimizer-chamber-st36`) — Exact A/B Twin (`t0` `withinClusterQueue: LowerPriority` + Dynamic Resubmit vs. `Never`)

- **Controlled Variable Changed**: ONLY `withinClusterQueue` changes (`LowerPriority` in `st37` vs. `Never` in `st36`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st36-t0-preempt-within-cq-never --no-skip-initial-offset --namespace kueue-optimizer-chamber-st36 --port 16436`
- **Cross-Examination Assertion**: `st37` preempts exact `4` `t0` `PodGroup`s at `t=11s`, dynamically resubmits all 4 parent `JobSet`s with zero `Workload` name collisions, and completes all `12` workloads.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `~82%–96%` (`st37`) vs. `~75%–82%` (`st36`)
  - `Sec 5.2 Wait Time`: `p-high wait < 2.5s` (`st37`) vs. `p-high wait ~18s–20s` (`st36`)
  - `Sec 5.3 Evictions`: `4 evictions (InClusterQueue, p-low)` (`st37`) vs. `0 evictions` (`st36`)
  - `Sec 5.4 Effective Sat`: `~70%–86% (12 completed after resubmit)` (`st37`) vs. `~74%–82% (12 completed, Excluded: 0)` (`st36`)

### Pair: `st37-t0-preempt-within-cq-lower-priority-resubmit` (`kueue-optimizer-chamber-st37`) ↔ [`st12-preempt-within-cq-lower-priority`](../st12-preempt-within-cq-lower-priority/AGENTS.md) (`kueue-optimizer-chamber-st12`) — `t0` `PodGroup` Preemption (`st37`) vs. Scheduled `JobSet` Preemption (`st12`) Pair

- **Controlled Variable Changed**: Bootstrapped `t0` `PodGroup` victims (`st37`) vs. `t=0s` `JobSet` victims (`st12`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st12-preempt-within-cq-lower-priority --namespace kueue-optimizer-chamber-st12 --port 16412`
- **Cross-Examination Assertion**: Both emit `4` `InClusterQueue` evictions, `< 2.5s` `p-high` wait, and `12` completed workloads.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `~82%–96%` (`st37`) vs. `80%–95%` (`st12`)
  - `Sec 5.2 Wait Time`: `p-high wait < 2.5s` (`st37`) vs. `p-high wait < 2.0s` (`st12`)
  - `Sec 5.3 Evictions`: `4 evictions (InClusterQueue, p-low)` (`st37`) vs. `4 evictions (4 unique)` (`st12`)
  - `Sec 5.4 Effective Sat`: `~70%–86% (12 completed after resubmit)` (`st37`) vs. `~68%–88% (after requeue)` (`st12`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: Identical 8 `t0` `p-low (100)` incumbents (`30s` remaining, `24s` full restart duration) and 4 `p-high (1000)` arrivals at `t=11s` (`18s`) as `st36`, changing ONLY `withinClusterQueue` from `Never` to `LowerPriority`.
- **Expected Runtime Behavior**: At `t=11s`, Kueue preempts **exact 4** of the 8 `t0` `p-low` `PodGroup` `Workload`s (`InClusterQueue = 4`). `arr-high-0..3` admit in `< 2.5s` and finish at `t=29s`. `testrunner.py` detects all 4 evicted/GC'd `t0pg-init-low-*` `PodGroup`s, dynamically submits `JobSet/init-low-*` (`[+4 preempted-t0 resubmit]`), and Kueue re-admits all 4 to full completion.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `~82%–96%` |
| **Section `5.2` Queue Wait Time** | `p-high wait < 2.5s` |
| **Section `5.3` Evictions & Reclaims** | `4 evictions (InClusterQueue, p-low)` |
| **Section `5.4` Effective Saturation** | `~70%–86% (12 completed after resubmit)` |

### Detailed Metric Checkpoints

- `Execution Summary`: `Workflows Submitted: 8/8 [+4 preempted-t0 resubmit]`, `KWOK Pods Status: ... failed=0`.
- `Section 5.2`: `p-high` wait drops from `~19s` (`st36`) down to **`< 2.5s`**.
- `Section 5.3`: `Total Evictions = 4`, `InClusterQueue = 4` (`priority_class=p-low`).
- `Section 5.4`: `Qualifying Workloads: 12 (completed=12 [12 pods])`, `Excluded: 0` (the 4 aborted `0s–11s` `PodGroup` runs are GC'd with zero credit while their 4 uninterrupted resubmitted `JobSet` runs complete).

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ `Section 5.3 Total Evictions == 0` (would mean `t0` `PodGroup`s were immune to preemption or missing `priority: 100`).
- ❌ Workload name collision or hang when resubmitting `JobSet/init-low-*` after `PodGroup` eviction.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
