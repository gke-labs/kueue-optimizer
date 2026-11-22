# Agent Execution & Cross-Examination Guide — `st55-scale-tas-dual-flavor-256n-1200w`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and re-run:
> ```bash
> python3 testscenarios/self-tests/generate_self_test_suite.py
> python3 deploy/scripts/generate_scenario_readme.py --all
> python3 testscenarios/self-tests/generate_self_test_suite.py --check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st55-scale-tas-dual-flavor-256n-1200w`
- **Category**: 11. Large-Scale TAS Packing, Slices & Multi-Pool (>100 Nodes, >1,000 Workloads)
- **Title**: 256-Node (`2,048`-TPU), 1,200-Workload Dual-`ResourceFlavor` TAS Fleet Stress Test
- **Primary Verification Target**: Largest node & workload scale test: `256` KWOK nodes (`64` racks, `2,048` TPUs) across 2 TAS `ResourceFlavor`s (`st-tpu-pool-a`, `st-tpu-pool-b`) with `128` `t0` TAS jobs (`256` pods) and `1,072` arrivals
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st55` (`--namespace kueue-optimizer-chamber-st55`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16455` | Prometheus `19102` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st51-scale-tas-rack-packing-128n-1088w`](../st51-scale-tas-rack-packing-128n-1088w/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST55 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st55' (API port 16455, Prom port 19102):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st55-scale-tas-dual-flavor-256n-1200w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st55 --port 16455 > /tmp/st55.log 2>&1 &
PID_ST55=$!

# Simultaneously launch paired ST51 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st51' (API port 16451, Prom port 19098):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st51-scale-tas-rack-packing-128n-1088w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st51 --port 16451 > /tmp/st51.log 2>&1 &
PID_ST51=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST55" "$PID_ST51"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST55 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st55' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st55-scale-tas-dual-flavor-256n-1200w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st55 --port 16455 | tee /tmp/st55.log

# 2. Run paired ST51 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st51':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st51-scale-tas-rack-packing-128n-1088w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st51 --port 16451 | tee /tmp/st51.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st55-scale-tas-dual-flavor-256n-1200w --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st55`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st55-scale-tas-dual-flavor-256n-1200w` (`kueue-optimizer-chamber-st55`) ↔ [`st51-scale-tas-rack-packing-128n-1088w`](../st51-scale-tas-rack-packing-128n-1088w/AGENTS.md) (`kueue-optimizer-chamber-st51`) — 256-Node Dual-Flavor TAS Fleet (`st55`) vs. 128-Node Single-Flavor TAS (`st51`)

- **Controlled Variable Changed**: `256` nodes (`2,048` TPUs, `st-tpu-pool-a` + `st-tpu-pool-b`, `1,200` workloads) vs. `128` nodes (`1,024` TPUs, `1,088` workloads).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st51-scale-tas-rack-packing-128n-1088w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st51 --port 16451`
- **Cross-Examination Assertion**: `st55` syncs `256` nodes without ConfigMap overflow and saturates both `1,024`-TPU flavors concurrently (`2,048` TPUs total).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `Peak ~100% (2,048/2,048 TPUs), AVG ~60%–88%` (`st55`) vs. `Peak 100% (1,024/1,024 TPUs), AVG ~70%–92%` (`st51`)
  - `Sec 5.2 Wait Time`: `Dual-flavor 512-pod concurrency` (`st55`) vs. `4-wave TAS drain` (`st51`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st55`) vs. `0 evictions` (`st51`)
  - `Sec 5.4 Effective Sat`: `~58%–88% (1,200 completed)` (`st55`) vs. `~65%–92% (1,088 completed)` (`st51`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: **256 nodes** (`16` cubes / `64` racks, `8` TPUs/node = **`2,048` TPUs**) split across `st-tpu-pool-a` (`128` nodes, `1,024` TPUs) and `st-tpu-pool-b` (`128` nodes, `1,024` TPUs) inside `cq-tas-dual-256n` (`2,048` TPUs). At `t0`, `128` `2`-pod (`8`-TPU) TAS `Job`s (`1,024` TPUs across both pools, `12s` remaining) run alongside `1,072` arriving `4`-TPU TAS `Job`s (`9s`) for **`1,200` total workloads** (`1,328` pods).
- **Expected Runtime Behavior**: Both `st-tpu-pool-a` and `st-tpu-pool-b` saturate up to `1,024` TPUs each (`512` concurrent pods across `256` nodes!), `cluster-scraper` captures all `256` nodes, both `ResourceFlavor`s, and all `1,200` workflows cleanly, and all `1,200` workloads finish.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `Peak ~100% (2,048/2,048 TPUs), AVG ~60%–88%` |
| **Section `5.2` Queue Wait Time** | `Dual-flavor 512-pod concurrency` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `~58%–88% (1,200 completed)` |

### Detailed Metric Checkpoints

- `Execution Summary`: `256` synced KWOK Nodes, `Initial Kueue Workloads: 128 PodGroup(s) (256 pod(s))`, `Workflows Submitted: 1072/1072`.
- `Section 5.1`: `cq-tas-dual-256n` peaks near **`100.0%`** (`2,048 / 2,048` TPUs).
- `Section 5.4`: `Qualifying Workloads: 1200 (completed=1200 [1328 pods])`.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ ConfigMap `1MiB` overflow when syncing `256` nodes (`lifecycle.py` dynamically omits `kwok-nodes.yaml` >50 nodes).
- ❌ Flavor mismatch assigning `st-tpu-pool-a` `t0` pods to `st-tpu-pool-b`.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
python3 testscenarios/self-tests/generate_self_test_suite.py
python3 deploy/scripts/generate_scenario_readme.py --all
python3 testscenarios/self-tests/generate_self_test_suite.py --check
```
