# Agent Execution & Cross-Examination Guide — `st46-scale-quota-saturation-128n-1056w`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and re-run:
> ```bash
> python3 testscenarios/self-tests/generate_self_test_suite.py
> python3 deploy/scripts/generate_scenario_readme.py --all
> python3 testscenarios/self-tests/generate_self_test_suite.py --check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st46-scale-quota-saturation-128n-1056w`
- **Category**: 10. Large-Scale Quota-Only Admission (>100 Nodes, >1,000 Workloads — No TAS)
- **Title**: 128-Node, 1,056-Workload Quota-Only Full Saturation with 128 In-Flight `t0` Jobs
- **Primary Verification Target**: Validates chamber throughput, `t0` quota accounting (`512/1,024` TPUs), and live `cluster-scraper` parity at >100 nodes and >1,000 workloads without TAS
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st46` (`--namespace kueue-optimizer-chamber-st46`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16446` | Prometheus `19093` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st51-scale-tas-rack-packing-128n-1088w`](../st51-scale-tas-rack-packing-128n-1088w/AGENTS.md), [`st47-scale-quota-preemption-128n-1080w`](../st47-scale-quota-preemption-128n-1080w/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST46 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st46' (API port 16446, Prom port 19093):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st46-scale-quota-saturation-128n-1056w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st46 --port 16446 > /tmp/st46.log 2>&1 &
PID_ST46=$!

# Simultaneously launch paired ST51 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st51' (API port 16451, Prom port 19098):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st51-scale-tas-rack-packing-128n-1088w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st51 --port 16451 > /tmp/st51.log 2>&1 &
PID_ST51=$!

# Simultaneously launch paired ST47 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st47' (API port 16447, Prom port 19094):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st47-scale-quota-preemption-128n-1080w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st47 --port 16447 > /tmp/st47.log 2>&1 &
PID_ST47=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST46" "$PID_ST51" "$PID_ST47"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST46 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st46' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st46-scale-quota-saturation-128n-1056w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st46 --port 16446 | tee /tmp/st46.log

# 2. Run paired ST51 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st51':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st51-scale-tas-rack-packing-128n-1088w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st51 --port 16451 | tee /tmp/st51.log

# 3. Run paired ST47 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st47':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st47-scale-quota-preemption-128n-1080w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st47 --port 16447 | tee /tmp/st47.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st46-scale-quota-saturation-128n-1056w --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st46`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st46-scale-quota-saturation-128n-1056w` (`kueue-optimizer-chamber-st46`) ↔ [`st51-scale-tas-rack-packing-128n-1088w`](../st51-scale-tas-rack-packing-128n-1088w/AGENTS.md) (`kueue-optimizer-chamber-st51`) — 128-Node Quota-Only vs. 7-Level TAS Packing Scale Pair

- **Controlled Variable Changed**: Quota-only `ResourceFlavor` without `Topology` (`st46`, `1,056` workloads) vs. 7-level `st-slice-topology` TAS packing (`st51`, `1,088` workloads) on `128` nodes (`1,024` TPUs).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st51-scale-tas-rack-packing-128n-1088w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st51 --port 16451`
- **Cross-Examination Assertion**: Both hit `100.0%` peak quota utilization (`1,024/1,024` TPUs) with `0` evictions and `100%` completion; `cluster-scraper` records `Topology` + `podSetAssignments[].topologyAssignment` in `st51` and omits them in `st46`.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `Peak 100% (1,024/1,024 TPUs), AVG ~70%–92%` (`st46`) vs. `Peak 100% (1,024/1,024 TPUs), AVG ~70%–92%` (`st51`)
  - `Sec 5.2 Wait Time`: `Staircase waves (AVG ~12s–26s)` (`st46`) vs. `4-wave TAS drain` (`st51`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st46`) vs. `0 evictions` (`st51`)
  - `Sec 5.4 Effective Sat`: `~65%–92% (1,056 completed)` (`st46`) vs. `~65%–92% (1,088 completed)` (`st51`)

### Pair: `st46-scale-quota-saturation-128n-1056w` (`kueue-optimizer-chamber-st46`) ↔ [`st47-scale-quota-preemption-128n-1080w`](../st47-scale-quota-preemption-128n-1080w/AGENTS.md) (`kueue-optimizer-chamber-st47`) — 128-Node Non-Preemptive Saturation vs. Intra-CQ `LowerPriority` Preemption Pair

- **Controlled Variable Changed**: Single priority `withinClusterQueue: Never` (`st46`) vs. `p-high` burst preempting `64` `t0` `p-low` `PodGroup`s (`withinClusterQueue: LowerPriority`, `st47`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st47-scale-quota-preemption-128n-1080w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st47 --port 16447`
- **Cross-Examination Assertion**: `Sec 5.3` shifts from `0` (`st46`) to `>= 64` `InClusterQueue` evictions + `[+64 preempted-t0 resubmit]` (`st47`).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `Peak 100% (1,024/1,024 TPUs), AVG ~70%–92%` (`st46`) vs. `Peak 100% (1,024/1,024 TPUs), AVG ~70%–92%` (`st47`)
  - `Sec 5.2 Wait Time`: `Staircase waves (AVG ~12s–26s)` (`st46`) vs. `p-high < p-low wait` (`st47`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st46`) vs. `>= 64 InClusterQueue evictions` (`st47`)
  - `Sec 5.4 Effective Sat`: `~65%–92% (1,056 completed)` (`st46`) vs. `~60%–90% (1,080 completed, 64 excluded)` (`st47`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 128 simulated KWOK nodes (`8` TPUs/node = `1,024` TPUs, no TAS), single `cq-scale-full` (`1,024` TPUs). At `t0`, `128` `Admitted` `batch/v1` `Job`s (`4` TPUs each = `512` TPUs, `12s` remaining) run across `st-node-000..127`. `928` `NotAdmitted` `Job`s (`4` TPUs, `9s`) arrive across 4 waves (`128` at `t=1s`, `256` at `t=3s`, `256` at `t=6s`, `288` at `t=9s`) for **`1,056` total workloads**.
- **Expected Runtime Behavior**: `128` `t0` `PodGroup`s hold `512/1,024` TPUs immediately at `t0`; the first `128` arrivals at `t=1s` fill the remaining `512` TPUs (`1,024/1,024 = 100%`), while the remaining `800` arrivals drain across waves with `0` evictions and `1,056/1,056` completed workloads.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `Peak 100% (1,024/1,024 TPUs), AVG ~70%–92%` |
| **Section `5.2` Queue Wait Time** | `Staircase waves (AVG ~12s–26s)` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `~65%–92% (1,056 completed)` |

### Detailed Metric Checkpoints

- `Execution Summary`: `Initial Kueue Workloads: 128 PodGroup(s) (128 pod(s))`, `Workflows Submitted: 928/928`.
- `Section 5.1`: `cq-scale-full` peaks at **`100.0%`** (`1,024 / 1,024` TPUs).
- `Section 5.3`: **`0`** evictions.
- `Section 5.4`: `Qualifying Workloads: 1056 (completed=1056 [1056 pods])` with high effective saturation (`~65%–92%`).

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ `cq-scale-full` admitting >128 new `4`-TPU jobs at `t=1s` while `128` `t0` `PodGroup`s occupy `512` TPUs.
- ❌ Any dropped/failed pods or missing terminal `Pod` `Deleted` events when captured by `cluster-scraper`.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
python3 testscenarios/self-tests/generate_self_test_suite.py
python3 deploy/scripts/generate_scenario_readme.py --all
python3 testscenarios/self-tests/generate_self_test_suite.py --check
```
