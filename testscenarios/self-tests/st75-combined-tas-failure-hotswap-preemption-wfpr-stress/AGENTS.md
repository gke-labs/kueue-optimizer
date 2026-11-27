# Agent Execution & Cross-Examination Guide — `st75-combined-tas-failure-hotswap-preemption-wfpr-stress`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st75-combined-tas-failure-hotswap-preemption-wfpr-stress`
- **Category**: 15. Interrupted-Workload Recording, Multiplier & True-Length Replay
- **Title**: Combined Multi-Fault TAS Hot-Swap + `t0` Preemption Resubmit (`2.0x` Partial Multiplier) + `waitForPodsReady` Timeout
- **Primary Verification Target**: End-to-end integration of all 4 capabilities: TAS failed-node hot-swap (`st-node-08` -> `st-node-11` with `0` node-failure evictions), `p-high` intra-CQ preemption of `2` `t0` `PodGroup`s, `2.0x` `--interrupted-duration-multiplier` (`8s -> 16s`) on resubmitted `t0` jobs, and `8s` `waitForPodsReady` deactivation of a stuck `JobSet`
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st75` (`--namespace kueue-optimizer-chamber-st75`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16475` | Prometheus `19122` (collision-free across all 95 scenarios `st01`–`st95`)
- **Mandatory Paired Counterpart(s)**: [`st61-tas-single-node-failure-hotswap-same-rack`](../st61-tas-single-node-failure-hotswap-same-rack/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st95`) and non-overlapping port pair (`16401/19048` .. `16495/19142`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST75 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st75' (API port 16475, Prom port 19122):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st75-combined-tas-failure-hotswap-preemption-wfpr-stress --no-skip-initial-offset --interrupted-duration-multiplier 2.0 --wait-for-pods-ready-timeout 8s --wait-for-pods-ready-recovery-timeout 18s --wait-for-pods-ready-backoff-limit 0 --namespace kueue-optimizer-chamber-st75 --port 16475 > /tmp/st75.log 2>&1 &
PID_ST75=$!

# Simultaneously launch paired ST61 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st61' (API port 16461, Prom port 19108):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st61-tas-single-node-failure-hotswap-same-rack --namespace kueue-optimizer-chamber-st61 --port 16461 > /tmp/st61.log 2>&1 &
PID_ST61=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST75" "$PID_ST61"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST75 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st75' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st75-combined-tas-failure-hotswap-preemption-wfpr-stress --no-skip-initial-offset --interrupted-duration-multiplier 2.0 --wait-for-pods-ready-timeout 8s --wait-for-pods-ready-recovery-timeout 18s --wait-for-pods-ready-backoff-limit 0 --namespace kueue-optimizer-chamber-st75 --port 16475 | tee /tmp/st75.log

# 2. Run paired ST61 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st61':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st61-tas-single-node-failure-hotswap-same-rack --namespace kueue-optimizer-chamber-st61 --port 16461 | tee /tmp/st61.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st75-combined-tas-failure-hotswap-preemption-wfpr-stress --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st75`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st75-combined-tas-failure-hotswap-preemption-wfpr-stress` (`kueue-optimizer-chamber-st75`) ↔ [`st61-tas-single-node-failure-hotswap-same-rack`](../st61-tas-single-node-failure-hotswap-same-rack/AGENTS.md) (`kueue-optimizer-chamber-st61`) — Combined Multi-Fault Stress (`st75`) vs. Isolated TAS Hot-Swap (`st61`)

- **Controlled Variable Changed**: `st75` combines `st61`'s TAS rack hot-swap (`st-node-08 -> st-node-11`) with `p-high` preemption of `2` `t0` `PodGroup`s (`2.0x` partial multiplier `8s -> 16s`) and `8s` `waitForPodsReady` deactivation.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st61-tas-single-node-failure-hotswap-same-rack --namespace kueue-optimizer-chamber-st61 --port 16461`
- **Cross-Examination Assertion**: `st75` records `3` total evictions (`2` `InClusterQueue` + `1` `PodsReadyTimeout`, **`0` `NodeFailures`**) and completes all `6` healthy workflows.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `~65%–85%` (`st75`) vs. `~65%–78% (24/32 TPUs)` (`st61`)
  - `Sec 5.2 Wait Time`: `p-high < 2.5s` (`st75`) vs. `< 1.5s` (`st61`)
  - `Sec 5.3 Evictions`: `3 evictions (2 Preempt + 1 WFPR, 0 NodeFailure)` (`st75`) vs. `0 evictions (in-place hot-swap)` (`st61`)
  - `Sec 5.4 Effective Sat`: `~55%–75% (6 healthy completed)` (`st75`) vs. `~62%–76% (2 completed, 0 evicted)` (`st61`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 16-node (`64`-TPU, `4` racks of `4` nodes) TAS cluster (`timeout=8s`, `recoveryTimeout=18s` exceeding `batch/v1` `Job`'s `10s` pod-recreation backoff, `backoffLimit=0`): `2` `t0` `p-low` `4`-node `PodGroup`s (`PartialExecution=8s`, scaled `2.0x -> 16s`), `2` `t=0s` `p-med` `3`-node `Job`s (`14s`), `st-node-08` fails at `t=4s`, `2` `t=5s` `p-high` `4`-node `JobSet`s (`10s`) preempt `t0` `p-low`, and `1` `t=6s` unready `p-low` `JobSet` hits `waitForPodsReady` (`8s`).
- **Expected Runtime Behavior**: `st-node-08` failure at `t=4s` hot-swaps `p-med` onto `st-node-11` within `recoveryTimeout=18s` (`0` node-failure or recovery evictions); `p-high` preempts both `t0` `p-low` `PodGroup`s (`2` `InClusterQueue` evictions) which dynamically resubmit with **`16.0s`** (`8s * 2.0`) duration and complete on racks `0` & `1` after `p-high` finishes; `js-low-stuck-wfpr` never reaches `PodsReady=True` and gets evicted & deactivated after `timeout=8s` (`1` `PodsReadyTimeout`/`Deactivated` eviction).

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `~65%–85%` |
| **Section `5.2` Queue Wait Time** | `p-high < 2.5s` |
| **Section `5.3` Evictions & Reclaims** | `3 evictions (2 Preempt + 1 WFPR, 0 NodeFailure)` |
| **Section `5.4` Effective Saturation** | `~55%–75% (6 healthy completed)` |

### Detailed Metric Checkpoints

- `Section 5.3 (Total Evictions)`: **`3` evictions** (`2` `InClusterQueue` preemptions + `1` `PodsReadyTimeout`/`Deactivated` eviction, and **`0` `NodeFailures` evictions** thanks to TAS hot-swap!).
- `Execution Summary`: All `6` healthy workflows (`2` `p-med` + `2` `p-high` + `2` resubmitted `t0` `p-low`) succeed and `1` stuck workflow deactivates cleanly.
- `cluster-scraper`: Captures `Node` `Update` (`st-node-08`), hot-swapped pod on `st-node-11`, `Preempted` + `PodsReadyTimeout` `PartialExecution`s, and `6` `FullExecution`s (`~16s` on resubmitted `p-low`).

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ `p-med` suffering a `NodeFailures` eviction when `st-node-11` is available in rack `2`.
- ❌ Resubmitted `t0` `p-low` jobs running only `8s` instead of `16s` (`2.0x`).

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
