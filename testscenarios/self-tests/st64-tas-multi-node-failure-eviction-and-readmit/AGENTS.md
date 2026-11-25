# Agent Execution & Cross-Examination Guide — `st64-tas-multi-node-failure-eviction-and-readmit`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st64-tas-multi-node-failure-eviction-and-readmit`
- **Category**: 13. Mid-Scenario Node Failures & Placement Hot-Swap
- **Title**: TAS Sequential Dual-Node Failure — 1st Node Hot-Swaps, 2nd Node Evicts & Re-Admits on Healthy Spares
- **Primary Verification Target**: Sequential two-node failure (`st-node-00` at `t=4s`, `st-node-01` at `t=8s`) on a `4`-pod block-level TAS `Job` with `8` block nodes (`6` remain healthy): verifies multi-node `NodeFailures` eviction and immediate re-admission onto the `6` remaining healthy nodes
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st64` (`--namespace kueue-optimizer-chamber-st64`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16464` | Prometheus `19111` (collision-free across all 95 scenarios `st01`–`st95`)
- **Mandatory Paired Counterpart(s)**: [`st61-tas-single-node-failure-hotswap-same-rack`](../st61-tas-single-node-failure-hotswap-same-rack/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st95`) and non-overlapping port pair (`16401/19048` .. `16495/19142`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST64 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st64' (API port 16464, Prom port 19111):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st64-tas-multi-node-failure-eviction-and-readmit --namespace kueue-optimizer-chamber-st64 --port 16464 > /tmp/st64.log 2>&1 &
PID_ST64=$!

# Simultaneously launch paired ST61 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st61' (API port 16461, Prom port 19108):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st61-tas-single-node-failure-hotswap-same-rack --namespace kueue-optimizer-chamber-st61 --port 16461 > /tmp/st61.log 2>&1 &
PID_ST61=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST64" "$PID_ST61"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST64 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st64' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st64-tas-multi-node-failure-eviction-and-readmit --namespace kueue-optimizer-chamber-st64 --port 16464 | tee /tmp/st64.log

# 2. Run paired ST61 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st61':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st61-tas-single-node-failure-hotswap-same-rack --namespace kueue-optimizer-chamber-st61 --port 16461 | tee /tmp/st61.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st64-tas-multi-node-failure-eviction-and-readmit --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st64`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st64-tas-multi-node-failure-eviction-and-readmit` (`kueue-optimizer-chamber-st64`) ↔ [`st61-tas-single-node-failure-hotswap-same-rack`](../st61-tas-single-node-failure-hotswap-same-rack/AGENTS.md) (`kueue-optimizer-chamber-st61`) — Sequential Dual-Node Failure (`st64`) vs. Single-Node Failure (`st61`) Pair

- **Controlled Variable Changed**: Two nodes (`st-node-00` at `t=4s`, `st-node-01` at `t=8s`) failing on the same `4`-pod block `Job` (`st64`) vs. single node failure (`st61`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st61-tas-single-node-failure-hotswap-same-rack --namespace kueue-optimizer-chamber-st61 --port 16461`
- **Cross-Examination Assertion**: Both `st-node-00` and `st-node-01` remain `Ready=False` throughout `st64` while the `4`-pod `Job` recovers onto healthy spares `st-node-02..07` and finishes cleanly.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `~45%–55% (16/32 TPUs)` (`st64`) vs. `~65%–78% (24/32 TPUs)` (`st61`)
  - `Sec 5.2 Wait Time`: `< 1.5s` (`st64`) vs. `< 1.5s` (`st61`)
  - `Sec 5.3 Evictions`: `0–1 evictions (timing of 1st swap vs 2nd fault)` (`st64`) vs. `0 evictions (in-place hot-swap)` (`st61`)
  - `Sec 5.4 Effective Sat`: `~32%–52%` (`st64`) vs. `~62%–76% (2 completed, 0 evicted)` (`st61`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 8-node (`32`-TPU) single-block (`st-block-00`) TAS cluster. One `4`-pod (`16`-TPU) `Job` (`backoffLimit: 8`, `podset-required-topology: gce-topology-block`, `13s`) admits at `t=0s`. `st-node-00` fails at `t=4s`; `st-node-01` fails at `t=8s` (`6` healthy nodes = `24` TPUs remain available in `st-block-00`).
- **Expected Runtime Behavior**: At `t=4s`, `st-node-00` hot-swaps in-place to a spare node. Wait: if Kueue clears `unhealthyNodes` before `t=8s`, `st-node-01` ALSO hot-swaps in-place (`0` evictions), OR if both overlap in `unhealthyNodes`, Kueue evicts (`1` `NodeFailures` eviction) and immediately re-admits onto 4 of the 6 healthy nodes (`st-node-02..07`) — in both cases completing cleanly while `st-node-00/01` stay `NotReady`!

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `~45%–55% (16/32 TPUs)` |
| **Section `5.2` Queue Wait Time** | `< 1.5s` |
| **Section `5.3` Evictions & Reclaims** | `0–1 evictions (timing of 1st swap vs 2nd fault)` |
| **Section `5.4` Effective Saturation** | `~32%–52%` |

### Detailed Metric Checkpoints

- `Execution Summary`: `workloads finished = 1` despite `2` permanently failed nodes (`st-node-00`, `st-node-01`).
- `KWOK Nodes`: `st-node-00` and `st-node-01` remain `Ready=False` (`node-failed=true`), proving `Stage/node-initialize` never resurrects failed nodes prematurely.
- `cluster-scraper`: Records both `Node` `Update` (`Ready=False`) events in `infra.json` and pod replacements onto `st-node-02..07`.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ KWOK `Stage/node-initialize` flipping `st-node-00` or `st-node-01` back to `Ready=True` while `kueue-optimizer.gke.io/node-failed=true` is set.
- ❌ `job-tas-block-4n` failing to finish when `6` healthy nodes (`24` TPUs >= `16` TPUs) remain in `st-block-00`.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
