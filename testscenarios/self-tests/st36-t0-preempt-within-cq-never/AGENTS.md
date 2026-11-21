# Agent Execution & Cross-Examination Guide — `st36-t0-preempt-within-cq-never`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and re-run:
> ```bash
> python3 testscenarios/self-tests/generate_self_test_suite.py
> python3 deploy/scripts/generate_scenario_readme.py --all
> python3 testscenarios/self-tests/generate_self_test_suite.py --check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st36-t0-preempt-within-cq-never`
- **Category**: 8. Initial t0 Workloads — Preemption & TAS
- **Title**: Non-Preemptive Protection of `t0` `PodGroup` Incumbents (`withinClusterQueue: Never`)
- **Primary Verification Target**: Baseline control twin of `st37`: high-priority arrivals wait for `t0` low-priority `PodGroup`s to finish without eviction
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st36` (`--namespace kueue-optimizer-chamber-st36`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16436` | Prometheus `19083` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st37-t0-preempt-within-cq-lower-priority-resubmit`](../st37-t0-preempt-within-cq-lower-priority-resubmit/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST36 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st36' (API port 16436, Prom port 19083):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st36-t0-preempt-within-cq-never --no-skip-initial-offset --namespace kueue-optimizer-chamber-st36 --port 16436 > /tmp/st36.log 2>&1 &
PID_ST36=$!

# Simultaneously launch paired ST37 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st37' (API port 16437, Prom port 19084):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st37-t0-preempt-within-cq-lower-priority-resubmit --no-skip-initial-offset --namespace kueue-optimizer-chamber-st37 --port 16437 > /tmp/st37.log 2>&1 &
PID_ST37=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST36" "$PID_ST37"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST36 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st36' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st36-t0-preempt-within-cq-never --no-skip-initial-offset --namespace kueue-optimizer-chamber-st36 --port 16436 | tee /tmp/st36.log

# 2. Run paired ST37 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st37':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st37-t0-preempt-within-cq-lower-priority-resubmit --no-skip-initial-offset --namespace kueue-optimizer-chamber-st37 --port 16437 | tee /tmp/st37.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st36-t0-preempt-within-cq-never --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st36`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st36-t0-preempt-within-cq-never` (`kueue-optimizer-chamber-st36`) ↔ [`st37-t0-preempt-within-cq-lower-priority-resubmit`](../st37-t0-preempt-within-cq-lower-priority-resubmit/AGENTS.md) (`kueue-optimizer-chamber-st37`) — Exact A/B Twin (`t0` `withinClusterQueue: Never` vs. `LowerPriority`)

- **Controlled Variable Changed**: ONLY `withinClusterQueue` changes (`Never` in `st36` vs. `LowerPriority` in `st37`); all 8 `t0` `p-low` `PodGroup`s and 4 `t=11s` `p-high` arrivals are identical.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st37-t0-preempt-within-cq-lower-priority-resubmit --no-skip-initial-offset --namespace kueue-optimizer-chamber-st37 --port 16437`
- **Cross-Examination Assertion**: `Sec 5.3 Evictions` flips from `0` (`st36`) to `4` (`st37`, `[+4 preempted-t0 resubmit]`), and `p-high` wait drops from `~19s` (`st36`) to `< 2.5s` (`st37`).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `~75%–82%` (`st36`) vs. `~82%–96%` (`st37`)
  - `Sec 5.2 Wait Time`: `p-high wait ~18s–20s` (`st36`) vs. `p-high wait < 2.5s` (`st37`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st36`) vs. `4 evictions (InClusterQueue, p-low)` (`st37`)
  - `Sec 5.4 Effective Sat`: `~74%–82% (12 completed, Excluded: 0)` (`st36`) vs. `~70%–86% (12 completed after resubmit)` (`st37`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 8-node (`32`-TPU) cluster, `withinClusterQueue: Never`. At `t0`, 8 `p-low (100)` `Admitted` `JobSet`s (`init-low-0..7`, `30s` remaining) fill all `32` TPUs. At `t=11s` (`--no-skip-initial-offset`), 4 `p-high (1000)` `JobSet`s (`18s`) arrive.
- **Expected Runtime Behavior**: All 8 `t0` `p-low` `PodGroup`s run uninterrupted until `t≈30s` (`0` evictions). All 4 `p-high` arrivals wait `~19s` (`t=11s -> t=30s`) and finish at `t≈48s`.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `~75%–82%` |
| **Section `5.2` Queue Wait Time** | `p-high wait ~18s–20s` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `~74%–82% (12 completed, Excluded: 0)` |

### Detailed Metric Checkpoints

- `Section 5.2`: `p-high` waits **`~16s–21s`**.
- `Section 5.3`: **`0`** evictions (`0` preempted-t0 resubmits).
- `Section 5.4`: `Qualifying Workloads: 12 (completed=12 [12 pods])`, `Excluded: 0`, **`EFFECTIVE SAT ≈ 74%–82%`**.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ Any eviction (`Section 5.3 > 0`) of a `t0` `PodGroup` when `withinClusterQueue: Never`.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
python3 testscenarios/self-tests/generate_self_test_suite.py
python3 deploy/scripts/generate_scenario_readme.py --all
python3 testscenarios/self-tests/generate_self_test_suite.py --check
```
