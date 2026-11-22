# Agent Execution & Cross-Examination Guide — `st52-scale-tas-mixed-slices-160n-1120w`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and re-run:
> ```bash
> python3 testscenarios/self-tests/generate_self_test_suite.py
> python3 deploy/scripts/generate_scenario_readme.py --all
> python3 testscenarios/self-tests/generate_self_test_suite.py --check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st52-scale-tas-mixed-slices-160n-1120w`
- **Category**: 11. Large-Scale TAS Packing, Slices & Multi-Pool (>100 Nodes, >1,000 Workloads)
- **Title**: 160-Node, 1,120-Workload Mixed `4`-Node Rack, `2`-Node Pair & `1`-Node TAS Slices
- **Primary Verification Target**: Co-schedules `40` `4`-pod `t0` rack `JobSet`s (`160` pods), `80` `2`-pod pair `JobSet`s (`160` pods), and `1,000` `1`-pod TAS `Job`s across `40` racks (`160` nodes)
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st52` (`--namespace kueue-optimizer-chamber-st52`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16452` | Prometheus `19099` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st51-scale-tas-rack-packing-128n-1088w`](../st51-scale-tas-rack-packing-128n-1088w/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST52 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st52' (API port 16452, Prom port 19099):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st52-scale-tas-mixed-slices-160n-1120w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st52 --port 16452 > /tmp/st52.log 2>&1 &
PID_ST52=$!

# Simultaneously launch paired ST51 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st51' (API port 16451, Prom port 19098):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st51-scale-tas-rack-packing-128n-1088w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st51 --port 16451 > /tmp/st51.log 2>&1 &
PID_ST51=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST52" "$PID_ST51"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST52 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st52' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st52-scale-tas-mixed-slices-160n-1120w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st52 --port 16452 | tee /tmp/st52.log

# 2. Run paired ST51 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st51':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st51-scale-tas-rack-packing-128n-1088w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st51 --port 16451 | tee /tmp/st51.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st52-scale-tas-mixed-slices-160n-1120w --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st52`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st52-scale-tas-mixed-slices-160n-1120w` (`kueue-optimizer-chamber-st52`) ↔ [`st51-scale-tas-rack-packing-128n-1088w`](../st51-scale-tas-rack-packing-128n-1088w/AGENTS.md) (`kueue-optimizer-chamber-st51`) — 160-Node Multi-Slice (`4`/`2`/`1`-Node) TAS `JobSet`s vs. 128-Node `2`/`1`-Node TAS Jobs Pair

- **Controlled Variable Changed**: `40` `4`-node rack `t0` `JobSet`s + `80` `2`-node `JobSet`s + `1,000` `1`-node `Job`s (`st52`) vs. `64` `2`-node `t0` `Job`s + `1,024` `1`-node `Job`s (`st51`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st51-scale-tas-rack-packing-128n-1088w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st51 --port 16451`
- **Cross-Examination Assertion**: Both complete `100%` of workloads with `0` evictions and `0` duplicate child `Job` submissions.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `Peak 100% (1,280/1,280 TPUs), AVG ~68%–90%` (`st52`) vs. `Peak 100% (1,024/1,024 TPUs), AVG ~70%–92%` (`st51`)
  - `Sec 5.2 Wait Time`: `Multi-slice TAS packing` (`st52`) vs. `4-wave TAS drain` (`st51`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st52`) vs. `0 evictions` (`st51`)
  - `Sec 5.4 Effective Sat`: `~62%–90% (1,120 completed)` (`st52`) vs. `~65%–92% (1,088 completed)` (`st51`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 160 nodes (`10` cubes / `40` racks, `8` TPUs/node = `1,280` TPUs) with TAS. At `t0`, `40` `4`-pod (`16`-TPU) `JobSet`s (`2x2x4-id`, `12s` remaining) span all `40` racks (`640` TPUs). Arrivals: `80` `2`-pod (`8`-TPU) TAS `JobSet`s (`2x2x2-id`, `9s`) + `1,000` `1`-pod (`4`-TPU) TAS `Job`s (`hostname`, `8s`) (**`1,120` workloads**, `1,320` pods).
- **Expected Runtime Behavior**: All three TAS slice granularities (`4`-node rack, `2`-node pair, `1`-node host) pack cleanly across `160` nodes without topology fragmentation deadlocks, completing all `1,120` workloads.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `Peak 100% (1,280/1,280 TPUs), AVG ~68%–90%` |
| **Section `5.2` Queue Wait Time** | `Multi-slice TAS packing` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `~62%–90% (1,120 completed)` |

### Detailed Metric Checkpoints

- `Execution Summary`: `Initial Kueue Workloads: 40 PodGroup(s) (160 pod(s))`, `Workflows Submitted: 1080/1080`.
- `Section 5.4`: `Qualifying Workloads: 1120 (completed=1120 [1320 pods])`.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ Child `Job`s owned by the `120` `JobSet`s duplicated as standalone workflows in `workflows.json` or replay.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
python3 testscenarios/self-tests/generate_self_test_suite.py
python3 deploy/scripts/generate_scenario_readme.py --all
python3 testscenarios/self-tests/generate_self_test_suite.py --check
```
