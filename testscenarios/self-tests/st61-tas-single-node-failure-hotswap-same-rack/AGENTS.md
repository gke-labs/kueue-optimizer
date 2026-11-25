# Agent Execution & Cross-Examination Guide — `st61-tas-single-node-failure-hotswap-same-rack`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st61-tas-single-node-failure-hotswap-same-rack`
- **Category**: 13. Mid-Scenario Node Failures & Placement Hot-Swap
- **Title**: TAS Single-Node Failure In-Place Placement Hot-Swap (`0` Workload Evictions)
- **Primary Verification Target**: Kueue `TASFailedNodeReplacement` + `TASReplaceNodeOnPodTermination` hot-swaps a failed rack node (`st-node-00`) to a healthy spare node (`st-node-03`) in the same `2x2x4-id` rack without evicting the Workload
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st61` (`--namespace kueue-optimizer-chamber-st61`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16461` | Prometheus `19108` (collision-free across all 95 scenarios `st01`–`st95`)
- **Mandatory Paired Counterpart(s)**: [`st63-tas-node-failure-no-spare-failfast-evict`](../st63-tas-node-failure-no-spare-failfast-evict/AGENTS.md), [`st62-tas-node-deleted-hotswap-and-scaleup`](../st62-tas-node-deleted-hotswap-and-scaleup/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st95`) and non-overlapping port pair (`16401/19048` .. `16495/19142`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST61 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st61' (API port 16461, Prom port 19108):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st61-tas-single-node-failure-hotswap-same-rack --namespace kueue-optimizer-chamber-st61 --port 16461 > /tmp/st61.log 2>&1 &
PID_ST61=$!

# Simultaneously launch paired ST63 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st63' (API port 16463, Prom port 19110):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st63-tas-node-failure-no-spare-failfast-evict --namespace kueue-optimizer-chamber-st63 --port 16463 > /tmp/st63.log 2>&1 &
PID_ST63=$!

# Simultaneously launch paired ST62 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st62' (API port 16462, Prom port 19109):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st62-tas-node-deleted-hotswap-and-scaleup --namespace kueue-optimizer-chamber-st62 --port 16462 > /tmp/st62.log 2>&1 &
PID_ST62=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST61" "$PID_ST63" "$PID_ST62"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST61 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st61' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st61-tas-single-node-failure-hotswap-same-rack --namespace kueue-optimizer-chamber-st61 --port 16461 | tee /tmp/st61.log

# 2. Run paired ST63 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st63':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st63-tas-node-failure-no-spare-failfast-evict --namespace kueue-optimizer-chamber-st63 --port 16463 | tee /tmp/st63.log

# 3. Run paired ST62 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st62':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st62-tas-node-deleted-hotswap-and-scaleup --namespace kueue-optimizer-chamber-st62 --port 16462 | tee /tmp/st62.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st61-tas-single-node-failure-hotswap-same-rack --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st61`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st61-tas-single-node-failure-hotswap-same-rack` (`kueue-optimizer-chamber-st61`) ↔ [`st63-tas-node-failure-no-spare-failfast-evict`](../st63-tas-node-failure-no-spare-failfast-evict/AGENTS.md) (`kueue-optimizer-chamber-st63`) — Exact TAS Rack Node-Failure A/B Twin (`1` Spare Node Hot-Swap `st61` vs. `0` Spare Nodes FailFast Eviction `st63`)

- **Controlled Variable Changed**: `3`-pod (`12`-TPU) rack `Job`s leaving `st-node-03` spare in `p224-0` (`st61`) vs. `4`-pod (`16`-TPU) full-rack `Job`s leaving `0` spare nodes (`st63`) when `st-node-00` fails at `t=6s`.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st63-tas-node-failure-no-spare-failfast-evict --namespace kueue-optimizer-chamber-st63 --port 16463`
- **Cross-Examination Assertion**: `Sec 5.3 Evictions` flips from **`0` (`st61`, in-place TAS hot-swap)** to **`1` (`st63`, `NodeFailures` FailFast eviction)**, proving Kueue replaces failed TAS nodes without eviction iff a healthy spare exists in the required topology domain.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `~65%–78% (24/32 TPUs)` (`st61`) vs. `~70%–86%` (`st63`)
  - `Sec 5.2 Wait Time`: `< 1.5s` (`st61`) vs. `< 2.0s initial, re-admit at ~13s` (`st63`)
  - `Sec 5.3 Evictions`: `0 evictions (in-place hot-swap)` (`st61`) vs. `1 eviction (NodeFailures)` (`st63`)
  - `Sec 5.4 Effective Sat`: `~62%–76% (2 completed, 0 evicted)` (`st61`) vs. `~62%–76% (6s aborted slice excluded)` (`st63`)

### Pair: `st61-tas-single-node-failure-hotswap-same-rack` (`kueue-optimizer-chamber-st61`) ↔ [`st62-tas-node-deleted-hotswap-and-scaleup`](../st62-tas-node-deleted-hotswap-and-scaleup/AGENTS.md) (`kueue-optimizer-chamber-st62`) — `Node` `NotReady` Hot-Swap (`st61`) vs. `Node` `Deleted` Hot-Swap + Scale-Up (`st62`)

- **Controlled Variable Changed**: `Node` `Update` (`Ready=False`) in `st61` vs. `Node` `Deleted` (`!nodeExists`) + post-`t0` `Node` `New` (`st-node-08`) in `st62`.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st62-tas-node-deleted-hotswap-and-scaleup --namespace kueue-optimizer-chamber-st62 --port 16462`
- **Cross-Examination Assertion**: Both `st61` and `st62` complete Wave 1 with **`0` Workload evictions** via TAS hot-swap, while `st62` additionally unblocks `4`-node full-rack Wave-2 `JobSet`s once `st-node-08` joins.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `~65%–78% (24/32 TPUs)` (`st61`) vs. `~75%–88%` (`st62`)
  - `Sec 5.2 Wait Time`: `< 1.5s` (`st61`) vs. `Wave 1 < 1.5s, Wave 2 ~10s–12s` (`st62`)
  - `Sec 5.3 Evictions`: `0 evictions (in-place hot-swap)` (`st61`) vs. `0 evictions` (`st62`)
  - `Sec 5.4 Effective Sat`: `~62%–76% (2 completed, 0 evicted)` (`st61`) vs. `~72%–86% (4 completed)` (`st62`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 8-node (`32`-TPU, `2` racks of `4` nodes: `p224-0` = `st-node-00..03`, `p224-1` = `st-node-04..07`) 7-level TAS cluster. Two `3`-pod (`12`-TPU) `Indexed` `Job`s (`backoffLimit: 4`, `podset-required-topology: 2x2x4-id`, `16s`) admit at `t=0s`, leaving `1` spare node per rack. At `t=6s`, `st-node-00` fails (`Ready=False`).
- **Expected Runtime Behavior**: Pod on `st-node-00` fails immediately (`NodeFailure`); `tas-node-reconciler` sets `unhealthyNodes=[st-node-00]`, Kueue hot-swaps `TopologyAssignment` in-place to spare `st-node-03` in `p224-0` with **zero Workload evictions**, `Job` controller creates 1 replacement pod ungated onto `st-node-03`, and both `Job`s complete cleanly.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `~65%–78% (24/32 TPUs)` |
| **Section `5.2` Queue Wait Time** | `< 1.5s` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions (in-place hot-swap)` |
| **Section `5.4` Effective Saturation** | `~62%–76% (2 completed, 0 evicted)` |

### Detailed Metric Checkpoints

- `Section 5.3 (Total Evictions)`: Strictly **`0`** (in-place TAS failed-node hot-swap, NOT a Workload eviction).
- `Execution Summary`: `workloads finished = 2`, `pods failed = 1`, `pods succeeded = 6` (`3 + 3`).
- `Section 5.4`: Both `Job`s (`24` TPUs total) complete uninterrupted from Kueue's Workload perspective (`Excluded: 0`).
- `cluster-scraper`: Captures `Node` `Update` (`Ready=False`) + failed pod `Deleted` + replacement pod `New` on `st-node-03` in `infra.json`.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ `Section 5.3` reporting `>= 1` Workload eviction when a healthy spare node (`st-node-03`) exists in the same `2x2x4-id` rack.
- ❌ Replacement pod getting stuck `Pending` on `st-node-00` or failing to ungate onto `st-node-03`.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
