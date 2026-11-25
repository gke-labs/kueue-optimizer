# Agent Execution & Cross-Examination Guide — `st25-borrow-within-cohort-priority-threshold`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st25-borrow-within-cohort-priority-threshold`
- **Category**: 5. Fair Sharing & Priority Thresholds
- **Title**: Classical Cohort Borrowing Preemption With `maxPriorityThreshold: 500` (`--no-fair-sharing`)
- **Primary Verification Target**: Tests Classical Preemption (`--no-fair-sharing`) `InCohortReclaimWhileBorrowing` + `maxPriorityThreshold: 500` ceiling across peer ClusterQueues
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st25` (`--namespace kueue-optimizer-chamber-st25`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16425` | Prometheus `19072` (collision-free across all 95 scenarios `st01`–`st95`)
- **Mandatory Paired Counterpart(s)**: [`st18-cohort-reclaim-on-lender-arrival`](../st18-cohort-reclaim-on-lender-arrival/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st95`) and non-overlapping port pair (`16401/19048` .. `16495/19142`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST25 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st25' (API port 16425, Prom port 19072):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st25-borrow-within-cohort-priority-threshold --no-fair-sharing --namespace kueue-optimizer-chamber-st25 --port 16425 > /tmp/st25.log 2>&1 &
PID_ST25=$!

# Simultaneously launch paired ST18 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st18' (API port 16418, Prom port 19065):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st18-cohort-reclaim-on-lender-arrival --namespace kueue-optimizer-chamber-st18 --port 16418 > /tmp/st18.log 2>&1 &
PID_ST18=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST25" "$PID_ST18"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST25 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st25' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st25-borrow-within-cohort-priority-threshold --no-fair-sharing --namespace kueue-optimizer-chamber-st25 --port 16425 | tee /tmp/st25.log

# 2. Run paired ST18 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st18':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st18-cohort-reclaim-on-lender-arrival --namespace kueue-optimizer-chamber-st18 --port 16418 | tee /tmp/st18.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st25-borrow-within-cohort-priority-threshold --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st25`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st25-borrow-within-cohort-priority-threshold` (`kueue-optimizer-chamber-st25`) ↔ [`st18-cohort-reclaim-on-lender-arrival`](../st18-cohort-reclaim-on-lender-arrival/AGENTS.md) (`kueue-optimizer-chamber-st18`) — Classical Priority-Threshold Cohort Preemption (`st25`) vs. Nominal Quota Reclaim (`st18`) Pair

- **Controlled Variable Changed**: In `st18`, `cq-lender` reclaims only its own `16` nominal TPUs (`InCohortReclamation = 4`); in `st25` (`--no-fair-sharing`), `cq-main` (`8` nominal TPUs + `16` shared Cohort TPUs) uses `borrowWithinCohort: {policy: LowerPriority, maxPriorityThreshold: 500}` to reclaim `8` nominal TPUs (`InCohortReclamation = 2`) AND preempt 2 `p-low (100 <= 500)` jobs while borrowing (`InCohortReclaimWhileBorrowing = 2`), while `maxPriorityThreshold: 500` protects `cq-peer`'s `p-mid (600 > 500)` jobs from borrowing preemption by `p-high (800)`.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st18-cohort-reclaim-on-lender-arrival --namespace kueue-optimizer-chamber-st18 --port 16418`
- **Cross-Examination Assertion**: `cq-main` reaches `P90/P99 = 200.00%` (`16/8` TPUs), `p-vip (900)` achieves `< 3.0s` wait (`4` jobs) while `p-high (800)` waits `~16s` (`2` jobs), and `p-mid (600)` suffers `0` evictions.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `cq-main P90/P99 = 200%, Cohort ~85%–100%` (`st25`) vs. `Cohort 85%–100% throughout` (`st18`)
  - `Sec 5.2 Wait Time`: `p-vip < 3.0s vs p-high ~16s` (`st25`) vs. `cq-lender wait < 2.0s` (`st18`)
  - `Sec 5.3 Evictions`: `4 p-low evictions (2 Reclaim + 2 ReclaimWhileBorrowing), 0 p-mid` (`st25`) vs. `4 reclaims (4 unique)` (`st18`)
  - `Sec 5.4 Effective Sat`: `~75%–92%` (`st25`) vs. `~72%–90%` (`st18`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 8-node (`32`-TPU) cluster, `cohort-thresh` (`16` shared Cohort TPUs + `cq-main`: `8` TPUs + `cq-peer`: `8` TPUs = `32` TPUs) configured with `borrowWithinCohort: {policy: LowerPriority, maxPriorityThreshold: 500}` under `--no-fair-sharing`. At `t=0s`, `cq-peer` starts 4 `p-mid (600 > 500)` + 4 `p-low (100 <= 500)` jobs (`32` TPUs = `400%` of `cq-peer` nominal, `28s`). At `t=8s`, `cq-main` submits 4 `p-vip (900)` jobs (`16` TPUs = `200%` of `cq-main` nominal, `18s`), followed at `t=10s` by 2 `p-high (800)` jobs (`8` TPUs, `18s`).
- **Expected Runtime Behavior**: At `t=8s`, `cq-main`'s first 2 `p-vip` jobs reclaim `cq-main`'s `8` nominal TPUs (`InCohortReclamation = 2`), AND its next 2 `p-vip` jobs borrow `8` shared Cohort TPUs (`cq-main = 200%`) by preempting `cq-peer`'s remaining 2 `p-low (100 <= 500)` jobs via **`InCohortReclaimWhileBorrowing = 2`**. At `t=10s`, `cq-main`'s 2 `p-high (800)` jobs attempt to borrow further (`24/8 = 300%`) against `cq-peer` (`16/8 = 200%`), but because `p-mid (600) > maxPriorityThreshold (500)`, `p-mid` is protected from preemption while `cq-main` borrows — forcing `p-high (800)` to wait `~16s` until `t=26s`.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `cq-main P90/P99 = 200%, Cohort ~85%–100%` |
| **Section `5.2` Queue Wait Time** | `p-vip < 3.0s vs p-high ~16s` |
| **Section `5.3` Evictions & Reclaims** | `4 p-low evictions (2 Reclaim + 2 ReclaimWhileBorrowing), 0 p-mid` |
| **Section `5.4` Effective Saturation** | `~75%–92%` |

### Detailed Metric Checkpoints

- `Section 5.1 (ClusterQueue: cq-main)`: Reaches **`P90 / P99 = 200.00%`** (`16/8` TPUs = `8` nominal + `8` borrowed via preemption).
- `Section 5.2`: `p-vip (900)` wait `< 3.0s` across all 4 jobs, whereas `p-high (800)` waits `~15s–19s` behind threshold-protected `p-mid (600)`.
- `Section 5.3 (Evictions)`: **`4`** evictions of `p-low` (`2` `InCohortReclamation` + `2` `InCohortReclaimWhileBorrowing`) and **`0`** evictions of `p-mid`.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ `cq-main` capped at `100%` (`8` TPUs) failing to emit `InCohortReclaimWhileBorrowing` against `p-low (100)`.
- ❌ Any `p-mid (600 > 500)` workload being evicted while `cq-main` is borrowing.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
