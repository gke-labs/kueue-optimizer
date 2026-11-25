# Agent Execution & Cross-Examination Guide — `st65-nontas-node-failure-reschedule-preemption-and-recovery`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st65-nontas-node-failure-reschedule-preemption-and-recovery`
- **Category**: 13. Mid-Scenario Node Failures & Placement Hot-Swap
- **Title**: Non-TAS `NoExecute` Node Failure Pod Rescheduling + `LowerPriority` Preemption + Node Recovery
- **Primary Verification Target**: Quota-only (non-TAS) `NoExecute` node failure reschedules replacement pod onto spare node (`0` node-failure evictions), followed by `p-high` arrival preempting `2` `p-low` jobs (`40 > 32` TPUs) and `st-node-00` recovery
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st65` (`--namespace kueue-optimizer-chamber-st65`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16465` | Prometheus `19112` (collision-free across all 95 scenarios `st01`–`st95`)
- **Mandatory Paired Counterpart(s)**: [`st61-tas-single-node-failure-hotswap-same-rack`](../st61-tas-single-node-failure-hotswap-same-rack/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st95`) and non-overlapping port pair (`16401/19048` .. `16495/19142`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST65 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st65' (API port 16465, Prom port 19112):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st65-nontas-node-failure-reschedule-preemption-and-recovery --namespace kueue-optimizer-chamber-st65 --port 16465 > /tmp/st65.log 2>&1 &
PID_ST65=$!

# Simultaneously launch paired ST61 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st61' (API port 16461, Prom port 19108):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st61-tas-single-node-failure-hotswap-same-rack --namespace kueue-optimizer-chamber-st61 --port 16461 > /tmp/st61.log 2>&1 &
PID_ST61=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST65" "$PID_ST61"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST65 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st65' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st65-nontas-node-failure-reschedule-preemption-and-recovery --namespace kueue-optimizer-chamber-st65 --port 16465 | tee /tmp/st65.log

# 2. Run paired ST61 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st61':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st61-tas-single-node-failure-hotswap-same-rack --namespace kueue-optimizer-chamber-st61 --port 16461 | tee /tmp/st61.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st65-nontas-node-failure-reschedule-preemption-and-recovery --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st65`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st65-nontas-node-failure-reschedule-preemption-and-recovery` (`kueue-optimizer-chamber-st65`) ↔ [`st61-tas-single-node-failure-hotswap-same-rack`](../st61-tas-single-node-failure-hotswap-same-rack/AGENTS.md) (`kueue-optimizer-chamber-st61`) — Non-TAS `NoExecute` Pod Rescheduling + Preemption (`st65`) vs. TAS In-Place Hot-Swap (`st61`)

- **Controlled Variable Changed**: Quota-only non-TAS cluster with `NoExecute` node taint + `p-high` preemption (`st65`) vs. 7-level TAS rack hot-swap (`st61`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st61-tas-single-node-failure-hotswap-same-rack --namespace kueue-optimizer-chamber-st61 --port 16461`
- **Cross-Examination Assertion**: In `st65`, the `t=5s` node failure causes `0` Kueue evictions (pod reschedules to spare node), while the `t=9s` `p-high` wave causes exact `2` `InClusterQueue` preemption evictions.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `~75%–92%` (`st65`) vs. `~65%–78% (24/32 TPUs)` (`st61`)
  - `Sec 5.2 Wait Time`: `p-high wait < 2.0s` (`st65`) vs. `< 1.5s` (`st61`)
  - `Sec 5.3 Evictions`: `2 evictions (InClusterQueue)` (`st65`) vs. `0 evictions (in-place hot-swap)` (`st61`)
  - `Sec 5.4 Effective Sat`: `~65%–82% (10 completed)` (`st65`) vs. `~62%–76% (2 completed, 0 evicted)` (`st61`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 8-node (`32`-TPU) non-TAS cluster, `withinClusterQueue: LowerPriority`. Six `1`-node `p-low (100)` `Job`s (`24` TPUs, `16s`) start at `t=0s` (`2` spare nodes). At `t=5s`, `st-node-00` fails with `NoExecute` taint (pod reschedules to spare node). At `t=9s`, four `1`-node `p-high (1000)` `Job`s (`16` TPUs, `11s`) arrive (`24 + 16 = 40 > 32` quota). At `t=16s`, `st-node-00` recovers.
- **Expected Runtime Behavior**: The `t=5s` node failure kills `1` pod, which `Job` controller reschedules onto an idle healthy node without Workload eviction. At `t=9s`, `4` `p-high` jobs preempt exact **`2` `p-low` jobs** (`InClusterQueue`). At `t=16s`, `st-node-00` recovers and all `10` jobs complete.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `~75%–92%` |
| **Section `5.2` Queue Wait Time** | `p-high wait < 2.0s` |
| **Section `5.3` Evictions & Reclaims** | `2 evictions (InClusterQueue)` |
| **Section `5.4` Effective Saturation** | `~65%–82% (10 completed)` |

### Detailed Metric Checkpoints

- `Section 5.3 (Total Evictions)`: **`2` evictions** (`InClusterQueue` preemption of `2` `p-low` jobs at `t=9s`).
- `Section 5.2`: `p-high (1000)` wait `< 2.0s`.
- `Execution Summary`: All **`10/10` `Job`s** finish (`workloads finished = 10`).

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ `p-high` jobs stalling behind `NotReady` `st-node-00` or failing to preempt `2` `p-low` jobs.
- ❌ `st-node-00` failing to recover at `t=16s`.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
