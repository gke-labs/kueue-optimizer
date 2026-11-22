# Agent Execution & Cross-Examination Guide — `st63-tas-node-failure-no-spare-failfast-evict`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and re-run:
> ```bash
> python3 testscenarios/self-tests/generate_self_test_suite.py
> python3 deploy/scripts/generate_scenario_readme.py --all
> python3 testscenarios/self-tests/generate_self_test_suite.py --check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st63-tas-node-failure-no-spare-failfast-evict`
- **Category**: 13. Mid-Scenario Node Failures & Placement Hot-Swap
- **Title**: TAS Node Failure With Zero Spare Nodes — `FailFast` `NodeFailures` Eviction & Recovery (Twin of st61)
- **Primary Verification Target**: Direct A/B twin of `st61` using `4`-pod full-rack `Job`s (`0` spare nodes in `p224-0`): `TASFailedNodeReplacementFailFast` immediately evicts with `NodeFailures`, then re-admits once `st-node-00` recovers at `t=13s`
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st63` (`--namespace kueue-optimizer-chamber-st63`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16463` | Prometheus `19110` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st61-tas-single-node-failure-hotswap-same-rack`](../st61-tas-single-node-failure-hotswap-same-rack/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST63 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st63' (API port 16463, Prom port 19110):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st63-tas-node-failure-no-spare-failfast-evict --namespace kueue-optimizer-chamber-st63 --port 16463 > /tmp/st63.log 2>&1 &
PID_ST63=$!

# Simultaneously launch paired ST61 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st61' (API port 16461, Prom port 19108):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st61-tas-single-node-failure-hotswap-same-rack --namespace kueue-optimizer-chamber-st61 --port 16461 > /tmp/st61.log 2>&1 &
PID_ST61=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST63" "$PID_ST61"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST63 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st63' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st63-tas-node-failure-no-spare-failfast-evict --namespace kueue-optimizer-chamber-st63 --port 16463 | tee /tmp/st63.log

# 2. Run paired ST61 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st61':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st61-tas-single-node-failure-hotswap-same-rack --namespace kueue-optimizer-chamber-st61 --port 16461 | tee /tmp/st61.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st63-tas-node-failure-no-spare-failfast-evict --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st63`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st63-tas-node-failure-no-spare-failfast-evict` (`kueue-optimizer-chamber-st63`) ↔ [`st61-tas-single-node-failure-hotswap-same-rack`](../st61-tas-single-node-failure-hotswap-same-rack/AGENTS.md) (`kueue-optimizer-chamber-st61`) — Exact TAS Rack Node-Failure A/B Twin (`0` Spare Nodes FailFast Eviction `st63` vs. `1` Spare Node Hot-Swap `st61`)

- **Controlled Variable Changed**: `4`-pod full-rack `Job` (`0` spares, `st63`) vs. `3`-pod rack `Job` (`1` spare, `st61`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st61-tas-single-node-failure-hotswap-same-rack --namespace kueue-optimizer-chamber-st61 --port 16461`
- **Cross-Examination Assertion**: `st63` records `1` `NodeFailures` eviction + `PartialExecution (interruptionReason='NodeFailure')` before re-admitting on `st-node-00` recovery at `t=13s`, whereas `st61` records `0` evictions.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `~70%–86%` (`st63`) vs. `~65%–78% (24/32 TPUs)` (`st61`)
  - `Sec 5.2 Wait Time`: `< 2.0s initial, re-admit at ~13s` (`st63`) vs. `< 1.5s` (`st61`)
  - `Sec 5.3 Evictions`: `1 eviction (NodeFailures)` (`st63`) vs. `0 evictions (in-place hot-swap)` (`st61`)
  - `Sec 5.4 Effective Sat`: `~62%–76% (6s aborted slice excluded)` (`st63`) vs. `~62%–76% (2 completed, 0 evicted)` (`st61`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: Identical 8-node (`32`-TPU, `2` racks of `4` nodes) TAS cluster and `t=6s` `st-node-00` failure as `st61`, except both `Job`s request **all `4` nodes (`16` TPUs) per rack** (`0` spare nodes). At `t=13s`, `st-node-00` recovers (`Ready=True`).
- **Expected Runtime Behavior**: Healthy rack `p224-1`'s `Job` finishes uninterrupted at `t≈14s`. Affected rack `p224-0`'s `Job` cannot hot-swap (`0` spare nodes in `p224-0`), so Kueue immediately evicts its Workload (`NodeFailures`) at `t≈6s`, holds it suspended while `st-node-00` is down (`6s–13s`), and re-admits it at `t≈13s` to run `14s` to completion.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `~70%–86%` |
| **Section `5.2` Queue Wait Time** | `< 2.0s initial, re-admit at ~13s` |
| **Section `5.3` Evictions & Reclaims** | `1 eviction (NodeFailures)` |
| **Section `5.4` Effective Saturation** | `~62%–76% (6s aborted slice excluded)` |

### Detailed Metric Checkpoints

- `Section 5.3 (Total Evictions)`: **`1` eviction** (`NodeFailures` — contrasting with `0` in `st61`).
- `Execution Summary`: Both `2/2` `Job`s finish (`workloads finished = 2`).
- `cluster-scraper`: Emits `PartialExecution` (`interruptionReason: 'NodeFailure'`, `preempted: false`) at `t≈6s`, followed by re-`Admitted` at `t≈13s` and `FullExecution` (`~14s`).

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ `Section 5.3` reporting `0` evictions when `0` spare nodes exist in the required `2x2x4-id` rack.
- ❌ `cluster-scraper` mislabeling a `NodeFailures` eviction as `preempted: true`.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
python3 testscenarios/self-tests/generate_self_test_suite.py
python3 deploy/scripts/generate_scenario_readme.py --all
python3 testscenarios/self-tests/generate_self_test_suite.py --check
```
