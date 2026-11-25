# Agent Execution & Cross-Examination Guide — `st51-scale-tas-rack-packing-128n-1088w`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st51-scale-tas-rack-packing-128n-1088w`
- **Category**: 11. Large-Scale TAS Packing, Slices & Multi-Pool (>100 Nodes, >1,000 Workloads)
- **Title**: 128-Node, 1,088-Workload 7-Level TAS Pair & Host Packing with `64` `t0` TAS Jobs
- **Primary Verification Target**: Validates 7-level TAS `topologyAssignment` seeding across `64` `2`-node `t0` TAS `PodGroup`s (`512` TPUs) and `1,024` arriving TAS jobs on `128` nodes
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st51` (`--namespace kueue-optimizer-chamber-st51`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16451` | Prometheus `19098` (collision-free across all 95 scenarios `st01`–`st95`)
- **Mandatory Paired Counterpart(s)**: [`st46-scale-quota-saturation-128n-1056w`](../st46-scale-quota-saturation-128n-1056w/AGENTS.md), [`st55-scale-tas-dual-flavor-256n-1200w`](../st55-scale-tas-dual-flavor-256n-1200w/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st95`) and non-overlapping port pair (`16401/19048` .. `16495/19142`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST51 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st51' (API port 16451, Prom port 19098):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st51-scale-tas-rack-packing-128n-1088w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st51 --port 16451 > /tmp/st51.log 2>&1 &
PID_ST51=$!

# Simultaneously launch paired ST46 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st46' (API port 16446, Prom port 19093):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st46-scale-quota-saturation-128n-1056w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st46 --port 16446 > /tmp/st46.log 2>&1 &
PID_ST46=$!

# Simultaneously launch paired ST55 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st55' (API port 16455, Prom port 19102):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st55-scale-tas-dual-flavor-256n-1200w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st55 --port 16455 > /tmp/st55.log 2>&1 &
PID_ST55=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST51" "$PID_ST46" "$PID_ST55"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST51 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st51' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st51-scale-tas-rack-packing-128n-1088w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st51 --port 16451 | tee /tmp/st51.log

# 2. Run paired ST46 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st46':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st46-scale-quota-saturation-128n-1056w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st46 --port 16446 | tee /tmp/st46.log

# 3. Run paired ST55 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st55':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st55-scale-tas-dual-flavor-256n-1200w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st55 --port 16455 | tee /tmp/st55.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st51-scale-tas-rack-packing-128n-1088w --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st51`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st51-scale-tas-rack-packing-128n-1088w` (`kueue-optimizer-chamber-st51`) ↔ [`st46-scale-quota-saturation-128n-1056w`](../st46-scale-quota-saturation-128n-1056w/AGENTS.md) (`kueue-optimizer-chamber-st46`) — 128-Node TAS Packing vs. Quota-Only Baseline Pair

- **Controlled Variable Changed**: 7-level TAS `st-slice-topology` (`st51`) vs. quota-only (`st46`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st46-scale-quota-saturation-128n-1056w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st46 --port 16446`
- **Cross-Examination Assertion**: Both achieve `100.0%` peak CQ utilization (`1,024/1,024` TPUs) and `0` evictions across >1,050 workloads.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `Peak 100% (1,024/1,024 TPUs), AVG ~70%–92%` (`st51`) vs. `Peak 100% (1,024/1,024 TPUs), AVG ~70%–92%` (`st46`)
  - `Sec 5.2 Wait Time`: `4-wave TAS drain` (`st51`) vs. `Staircase waves (AVG ~12s–26s)` (`st46`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st51`) vs. `0 evictions` (`st46`)
  - `Sec 5.4 Effective Sat`: `~65%–92% (1,088 completed)` (`st51`) vs. `~65%–92% (1,056 completed)` (`st46`)

### Pair: `st51-scale-tas-rack-packing-128n-1088w` (`kueue-optimizer-chamber-st51`) ↔ [`st55-scale-tas-dual-flavor-256n-1200w`](../st55-scale-tas-dual-flavor-256n-1200w/AGENTS.md) (`kueue-optimizer-chamber-st55`) — 128-Node Single-Pool TAS vs. 256-Node Dual-`ResourceFlavor` TAS Scale Pair

- **Controlled Variable Changed**: `128` nodes / `1,024` TPUs / 1 flavor (`st51`) vs. `256` nodes / `2,048` TPUs / 2 flavors (`st55`, `1,200` workloads).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st55-scale-tas-dual-flavor-256n-1200w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st55 --port 16455`
- **Cross-Examination Assertion**: Both complete `100%` of >1,080 TAS workloads with `0` evictions while `st55` doubles concurrent pod capacity to `512` pods (`2,048` TPUs).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `Peak 100% (1,024/1,024 TPUs), AVG ~70%–92%` (`st51`) vs. `Peak ~100% (2,048/2,048 TPUs), AVG ~60%–88%` (`st55`)
  - `Sec 5.2 Wait Time`: `4-wave TAS drain` (`st51`) vs. `Dual-flavor 512-pod concurrency` (`st55`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st51`) vs. `0 evictions` (`st55`)
  - `Sec 5.4 Effective Sat`: `~65%–92% (1,088 completed)` (`st51`) vs. `~58%–88% (1,200 completed)` (`st55`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 128 nodes (`8` cubes / `32` racks, `8` TPUs/node = `1,024` TPUs) with 7-level `st-slice-topology`. At `t0`, `64` `Admitted` `2`-pod (`8`-TPU) TAS `Job`s (`podset-required-topology: 2x2x2-id`, `12s` remaining) occupy `4` TPUs on every node (`512/1,024` TPUs). `1,024` `1`-pod (`4`-TPU) TAS `Job`s (`hostname`, `9s`) arrive in 4 waves of `256` (`t=1s, 4s, 8s, 12s`) for **`1,088` total TAS workloads**.
- **Expected Runtime Behavior**: Kueue's TAS cache ingests all `64` `t0` `topologyAssignment`s (`4/8` TPUs used per node), packs the first `128` arrivals onto the remaining `4` TPUs of each node (`1,024/1,024` TPUs), and completes all `1,088` TAS workloads with `0` evictions.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `Peak 100% (1,024/1,024 TPUs), AVG ~70%–92%` |
| **Section `5.2` Queue Wait Time** | `4-wave TAS drain` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `~65%–92% (1,088 completed)` |

### Detailed Metric Checkpoints

- `Execution Summary`: `Initial Kueue Workloads: 64 PodGroup(s) (128 pod(s))`, `Workflows Submitted: 1024/1024`.
- `Section 5.1`: `cq-tas-pack` peaks at **`100.0%`** (`1,024 / 1,024` TPUs).
- `Section 5.4`: `Qualifying Workloads: 1088 (completed=1088 [1152 pods])`.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ TAS double-booking any node above `8` TPUs (`pods[pend]` stuck due to missing `t0` `topologyAssignment`).
- ❌ `cluster-scraper` omitting `podSetAssignments[].topologyAssignment` in `execution-time.json`.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
