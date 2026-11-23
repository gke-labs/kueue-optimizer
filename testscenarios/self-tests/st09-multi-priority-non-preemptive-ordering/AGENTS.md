# Agent Execution & Cross-Examination Guide — `st09-multi-priority-non-preemptive-ordering`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st09-multi-priority-non-preemptive-ordering`
- **Category**: 2. Queueing, Backfill & Topology
- **Title**: Multi-Priority Queue Reordering Without Preemption (`withinClusterQueue: Never`)
- **Primary Verification Target**: Per-priority wait-time breakdown in `Section 5.2` when `withinClusterQueue: Never`
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st09` (`--namespace kueue-optimizer-chamber-st09`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16409` | Prometheus `19056` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st08-single-priority-deep-backlog-waves`](../st08-single-priority-deep-backlog-waves/AGENTS.md), [`st12-preempt-within-cq-lower-priority`](../st12-preempt-within-cq-lower-priority/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST09 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st09' (API port 16409, Prom port 19056):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st09-multi-priority-non-preemptive-ordering --namespace kueue-optimizer-chamber-st09 --port 16409 > /tmp/st09.log 2>&1 &
PID_ST09=$!

# Simultaneously launch paired ST08 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st08' (API port 16408, Prom port 19055):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st08-single-priority-deep-backlog-waves --namespace kueue-optimizer-chamber-st08 --port 16408 > /tmp/st08.log 2>&1 &
PID_ST08=$!

# Simultaneously launch paired ST12 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st12' (API port 16412, Prom port 19059):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st12-preempt-within-cq-lower-priority --namespace kueue-optimizer-chamber-st12 --port 16412 > /tmp/st12.log 2>&1 &
PID_ST12=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST09" "$PID_ST08" "$PID_ST12"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST09 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st09' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st09-multi-priority-non-preemptive-ordering --namespace kueue-optimizer-chamber-st09 --port 16409 | tee /tmp/st09.log

# 2. Run paired ST08 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st08':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st08-single-priority-deep-backlog-waves --namespace kueue-optimizer-chamber-st08 --port 16408 | tee /tmp/st08.log

# 3. Run paired ST12 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st12':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st12-preempt-within-cq-lower-priority --namespace kueue-optimizer-chamber-st12 --port 16412 | tee /tmp/st12.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st09-multi-priority-non-preemptive-ordering --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st09`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st09-multi-priority-non-preemptive-ordering` (`kueue-optimizer-chamber-st09`) ↔ [`st08-single-priority-deep-backlog-waves`](../st08-single-priority-deep-backlog-waves/AGENTS.md) (`kueue-optimizer-chamber-st08`) — Multi-Priority Non-Preemptive Reordering (`st09`) vs. Single-Priority Staircase (`st08`) Pair

- **Controlled Variable Changed**: Introduces `WorkloadPriorityClass` tiers arriving in reverse priority order while keeping `withinClusterQueue: Never`.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st08-single-priority-deep-backlog-waves --namespace kueue-optimizer-chamber-st08 --port 16408`
- **Cross-Examination Assertion**: `Sec 5.3 Evictions` MUST remain `0` (matching `st08`), while `Sec 5.2` proves `p-critical (1000)` (`wait ~11s`) overtakes both `p-high (500)` (`wait ~24s`) and queued `p-low (50)` (`wait ~37s`).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `93%–100%` (`st09`) vs. `92%–100%` (`st08`)
  - `Sec 5.2 Wait Time`: `Wait(p-critical)<Wait(p-high)<Wait(p-low,queued)` (`st09`) vs. `AVG ~18s, P90 ~36s` (`st08`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st09`) vs. `0 evictions` (`st08`)
  - `Sec 5.4 Effective Sat`: `92%–99%` (`st09`) vs. `91%–99%` (`st08`)

### Pair: `st09-multi-priority-non-preemptive-ordering` (`kueue-optimizer-chamber-st09`) ↔ [`st12-preempt-within-cq-lower-priority`](../st12-preempt-within-cq-lower-priority/AGENTS.md) (`kueue-optimizer-chamber-st12`) — Non-Preemptive Queue Reordering (`st09`) vs. Active Intra-CQ Preemption (`st12`) Pair

- **Controlled Variable Changed**: `withinClusterQueue: Never` (`st09`) vs. `withinClusterQueue: LowerPriority` (`st12`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st12-preempt-within-cq-lower-priority --namespace kueue-optimizer-chamber-st12 --port 16412`
- **Cross-Examination Assertion**: In `st09`, high-priority jobs wait for the running `t=0` wave to finish naturally (`wait ~11s`, `0` evictions); in `st12`, high-priority jobs evict running low-priority incumbents immediately (`wait < 2s`, `4` evictions).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `93%–100%` (`st09`) vs. `80%–95%` (`st12`)
  - `Sec 5.2 Wait Time`: `Wait(p-critical)<Wait(p-high)<Wait(p-low,queued)` (`st09`) vs. `p-high wait < 2.0s` (`st12`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st09`) vs. `4 evictions (4 unique)` (`st12`)
  - `Sec 5.4 Effective Sat`: `92%–99%` (`st09`) vs. `~68%–88% (after requeue)` (`st12`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 8-node (`32`-TPU) cluster, `withinClusterQueue: Never`. Initial wave of 4 `p-low (50)` jobs (`15s`) occupies `32/32` TPUs at `t=0s`. While full, 4 `p-low (50)` arrive at `t=2s`, 4 `p-high (500)` arrive at `t=3s`, and 4 `p-critical (1000)` arrive at `t=4s`.
- **Expected Runtime Behavior**: Zero running `t=0` jobs are preempted (`withinClusterQueue: Never`). When the initial wave finishes at `t=15s`, `p-critical (1000)` jumps ahead of `p-high` and `p-low` (`wait ≈ 11s`), followed by `p-high (500)` at `t=27s` (`wait ≈ 24s`), and finally the queued `p-low (50)` jobs at `t=39s` (`wait ≈ 37s`).

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `93%–100%` |
| **Section `5.2` Queue Wait Time** | `Wait(p-critical)<Wait(p-high)<Wait(p-low,queued)` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `92%–99%` |

### Detailed Metric Checkpoints

- `Section 5.2 (Wait Time per Priority)`: Strict monotonic ordering: **`Wait(p-critical) ≈ 11s < Wait(p-high) ≈ 24s`**, and queued `p-low` finishes last (`AVG(p-low) ≈ 18.5s` across 4 immediate + 4 `~37s` queued jobs).
- `Section 5.3 (Evictions)`: Strictly **`0`** (non-preemptive queue reordering only).
- `Section 5.4 (Effective Saturation)`: **`~92%–99%`** (100% uninterrupted execution across all 4 waves, zero wasted work).

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ Any eviction (`Section 5.3 > 0`) when `withinClusterQueue == Never`.
- ❌ Queued `p-low` jobs admitting ahead of `p-critical` or `p-high` after `t=15s`.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
