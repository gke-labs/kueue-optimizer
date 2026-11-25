# Agent Execution & Cross-Examination Guide — `st62-tas-node-deleted-hotswap-and-scaleup`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st62-tas-node-deleted-hotswap-and-scaleup`
- **Category**: 13. Mid-Scenario Node Failures & Placement Hot-Swap
- **Title**: TAS Mid-Run Node Deletion Hot-Swap + Post-`t0` Replacement Node Scale-Up
- **Primary Verification Target**: `Node` `Deleted` hot-swap (`!nodeExists` immediate replacement onto spare `st-node-03`) followed by post-`t0` `Node` `New` (`st-node-08` joining rack `p224-0`) unblocking full-rack `4`-node Wave-2 `JobSet`s
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st62` (`--namespace kueue-optimizer-chamber-st62`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16462` | Prometheus `19109` (collision-free across all 95 scenarios `st01`–`st95`)
- **Mandatory Paired Counterpart(s)**: [`st61-tas-single-node-failure-hotswap-same-rack`](../st61-tas-single-node-failure-hotswap-same-rack/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st95`) and non-overlapping port pair (`16401/19048` .. `16495/19142`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST62 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st62' (API port 16462, Prom port 19109):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st62-tas-node-deleted-hotswap-and-scaleup --namespace kueue-optimizer-chamber-st62 --port 16462 > /tmp/st62.log 2>&1 &
PID_ST62=$!

# Simultaneously launch paired ST61 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st61' (API port 16461, Prom port 19108):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st61-tas-single-node-failure-hotswap-same-rack --namespace kueue-optimizer-chamber-st61 --port 16461 > /tmp/st61.log 2>&1 &
PID_ST61=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST62" "$PID_ST61"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST62 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st62' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st62-tas-node-deleted-hotswap-and-scaleup --namespace kueue-optimizer-chamber-st62 --port 16462 | tee /tmp/st62.log

# 2. Run paired ST61 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st61':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st61-tas-single-node-failure-hotswap-same-rack --namespace kueue-optimizer-chamber-st61 --port 16461 | tee /tmp/st61.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st62-tas-node-deleted-hotswap-and-scaleup --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st62`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st62-tas-node-deleted-hotswap-and-scaleup` (`kueue-optimizer-chamber-st62`) ↔ [`st61-tas-single-node-failure-hotswap-same-rack`](../st61-tas-single-node-failure-hotswap-same-rack/AGENTS.md) (`kueue-optimizer-chamber-st61`) — `Node` `Deleted` + Scale-Up (`st62`) vs. `Node` `NotReady` (`st61`) Hot-Swap Pair

- **Controlled Variable Changed**: Mid-run `Node` deletion + replacement node addition (`st62`) vs. mid-run `Ready=False` transition (`st61`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st61-tas-single-node-failure-hotswap-same-rack --namespace kueue-optimizer-chamber-st61 --port 16461`
- **Cross-Examination Assertion**: Both achieve `0` Workload evictions; `st62` completes `4/4` `JobSet`s including `2` full-rack Wave-2 jobs requiring the newly added `st-node-08`.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `~75%–88%` (`st62`) vs. `~65%–78% (24/32 TPUs)` (`st61`)
  - `Sec 5.2 Wait Time`: `Wave 1 < 1.5s, Wave 2 ~10s–12s` (`st62`) vs. `< 1.5s` (`st61`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st62`) vs. `0 evictions (in-place hot-swap)` (`st61`)
  - `Sec 5.4 Effective Sat`: `~72%–86% (4 completed)` (`st62`) vs. `~62%–76% (2 completed, 0 evicted)` (`st61`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 8-node (`32`-TPU) TAS cluster (`2` racks of `4` nodes). Wave 1 (`t=0s`): two `3`-pod TAS `JobSet`s (`backoffLimit: 4`, `14s`). At `t=5s`, `st-node-00` is **Deleted** (`EventDeleted`), triggering hot-swap to `st-node-03`. Wave 2 (`t=9s`): two `4`-pod full-rack TAS `JobSet`s (`10s`) arrive; at `t=11s`, replacement `st-node-08` joins `p224-0` (`EventNew`).
- **Expected Runtime Behavior**: Wave 1 survives `st-node-00` deletion with `0` Workload evictions via hot-swap to `st-node-03`. Adding `st-node-08` restores `p224-0` to `4` healthy nodes (`st-node-01,02,03,08`), allowing both `4`-pod full-rack Wave-2 `JobSet`s to admit and succeed once Wave 1 finishes.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `~75%–88%` |
| **Section `5.2` Queue Wait Time** | `Wave 1 < 1.5s, Wave 2 ~10s–12s` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `~72%–86% (4 completed)` |

### Detailed Metric Checkpoints

- `Section 5.3 (Total Evictions)`: Strictly **`0`**.
- `Execution Summary`: All **`4/4` `JobSet`s** complete (`workloads finished = 4`).
- `cluster-scraper`: `infra.json` records `Node` `Deleted` (`st-node-00`) at `t≈5s` and `Node` `New` (`st-node-08`) at `t≈11s` via real-time `watchNodesLoop`.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ Wave-2 `4`-node full-rack `JobSet` deadlocking in `Pending` (would happen if post-`t0` `Node` `New` `st-node-08` were ignored).
- ❌ Wave-1 `JobSet` getting evicted when `st-node-00` is deleted while `st-node-03` is idle in `p224-0`.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
