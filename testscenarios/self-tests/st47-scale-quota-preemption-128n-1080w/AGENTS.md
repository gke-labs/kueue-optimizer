# Agent Execution & Cross-Examination Guide — `st47-scale-quota-preemption-128n-1080w`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st47-scale-quota-preemption-128n-1080w`
- **Category**: 10. Large-Scale Quota-Only Admission (>100 Nodes, >1,000 Workloads — No TAS)
- **Title**: 128-Node, 1,080-Workload Quota-Only Intra-CQ Preemption & `t0` Resubmission
- **Primary Verification Target**: Stress-tests `withinClusterQueue: LowerPriority` evicting `64` `t0` `p-low` `PodGroup`s (`512` TPUs) at `t=3s` and dynamically resubmitting all `64` parent `Job`s alongside `888` arrivals
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st47` (`--namespace kueue-optimizer-chamber-st47`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16447` | Prometheus `19094` (collision-free across all 95 scenarios `st01`–`st95`)
- **Mandatory Paired Counterpart(s)**: [`st46-scale-quota-saturation-128n-1056w`](../st46-scale-quota-saturation-128n-1056w/AGENTS.md), [`st56-scale-tas-preempt-within-cq-128n-1060w`](../st56-scale-tas-preempt-within-cq-128n-1060w/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st95`) and non-overlapping port pair (`16401/19048` .. `16495/19142`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST47 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st47' (API port 16447, Prom port 19094):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st47-scale-quota-preemption-128n-1080w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st47 --port 16447 > /tmp/st47.log 2>&1 &
PID_ST47=$!

# Simultaneously launch paired ST46 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st46' (API port 16446, Prom port 19093):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st46-scale-quota-saturation-128n-1056w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st46 --port 16446 > /tmp/st46.log 2>&1 &
PID_ST46=$!

# Simultaneously launch paired ST56 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st56' (API port 16456, Prom port 19103):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st56-scale-tas-preempt-within-cq-128n-1060w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st56 --port 16456 > /tmp/st56.log 2>&1 &
PID_ST56=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST47" "$PID_ST46" "$PID_ST56"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST47 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st47' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st47-scale-quota-preemption-128n-1080w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st47 --port 16447 | tee /tmp/st47.log

# 2. Run paired ST46 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st46':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st46-scale-quota-saturation-128n-1056w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st46 --port 16446 | tee /tmp/st46.log

# 3. Run paired ST56 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st56':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st56-scale-tas-preempt-within-cq-128n-1060w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st56 --port 16456 | tee /tmp/st56.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st47-scale-quota-preemption-128n-1080w --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st47`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st47-scale-quota-preemption-128n-1080w` (`kueue-optimizer-chamber-st47`) ↔ [`st46-scale-quota-saturation-128n-1056w`](../st46-scale-quota-saturation-128n-1056w/AGENTS.md) (`kueue-optimizer-chamber-st46`) — 128-Node Intra-CQ Preemption vs. Non-Preemptive Baseline Pair

- **Controlled Variable Changed**: `withinClusterQueue: LowerPriority` with `64` `t0` `p-low` victims (`st47`) vs. `withinClusterQueue: Never` (`st46`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st46-scale-quota-saturation-128n-1056w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st46 --port 16446`
- **Cross-Examination Assertion**: `st47` records `>= 64` `InClusterQueue` evictions and excludes the `64` aborted `t0` `PodGroup` runs from `Sec 5.4`, whereas `st46` has `0` evictions and `0` excluded.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `Peak 100% (1,024/1,024 TPUs), AVG ~70%–92%` (`st47`) vs. `Peak 100% (1,024/1,024 TPUs), AVG ~70%–92%` (`st46`)
  - `Sec 5.2 Wait Time`: `p-high < p-low wait` (`st47`) vs. `Staircase waves (AVG ~12s–26s)` (`st46`)
  - `Sec 5.3 Evictions`: `>= 64 InClusterQueue evictions` (`st47`) vs. `0 evictions` (`st46`)
  - `Sec 5.4 Effective Sat`: `~60%–90% (1,080 completed, 64 excluded)` (`st47`) vs. `~65%–92% (1,056 completed)` (`st46`)

### Pair: `st47-scale-quota-preemption-128n-1080w` (`kueue-optimizer-chamber-st47`) ↔ [`st56-scale-tas-preempt-within-cq-128n-1060w`](../st56-scale-tas-preempt-within-cq-128n-1060w/AGENTS.md) (`kueue-optimizer-chamber-st56`) — Quota-Only vs. TAS 128-Node Intra-CQ Preemption Pair

- **Controlled Variable Changed**: Quota-only preemption (`st47`) vs. 7-level TAS `2x2x2-id` node-pair preemption (`st56`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st56-scale-tas-preempt-within-cq-128n-1060w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st56 --port 16456`
- **Cross-Examination Assertion**: Both evict `>= 64` `t0` `p-low` `PodGroup`s (`512` TPUs) and dynamically resubmit all `64` parent `Job`s to full completion.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `Peak 100% (1,024/1,024 TPUs), AVG ~70%–92%` (`st47`) vs. `Peak 100% (1,024/1,024 TPUs), AVG ~68%–90%` (`st56`)
  - `Sec 5.2 Wait Time`: `p-high < p-low wait` (`st47`) vs. `p-high 2-node TAS burst waits < 5s` (`st56`)
  - `Sec 5.3 Evictions`: `>= 64 InClusterQueue evictions` (`st47`) vs. `>= 64 InClusterQueue TAS evictions` (`st56`)
  - `Sec 5.4 Effective Sat`: `~60%–90% (1,080 completed, 64 excluded)` (`st47`) vs. `~60%–88% (1,060 completed)` (`st56`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 128 nodes (`1,024` TPUs, no TAS), `cq-scale-preempt` (`withinClusterQueue: LowerPriority`). At `t0`, `64` `p-low (100)` `8`-TPU `Job`s (`512` TPUs, `34s` remaining) + `128` `p-high (1000)` `4`-TPU `Job`s (`512` TPUs, `20s` remaining) fill `100%` of quota (`192` `t0` workloads). At `t=3s`, `64` `p-high` `8`-TPU `Job`s arrive, followed by `824` `4`-TPU `p-high`/`p-low` `Job`s across `t=8s..18s` (**`1,080` configured workloads**).
- **Expected Runtime Behavior**: At `t=3s`, Kueue evicts `64` `t0` `p-low` `PodGroup`s (`InClusterQueue`), admits the `64` `p-high` `8`-TPU burst jobs immediately, dynamically resubmits the `64` preempted `t0` `Job`s (`[+64 preempted-t0 resubmit]`), and drains all `1,080` workloads to completion.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `Peak 100% (1,024/1,024 TPUs), AVG ~70%–92%` |
| **Section `5.2` Queue Wait Time** | `p-high < p-low wait` |
| **Section `5.3` Evictions & Reclaims** | `>= 64 InClusterQueue evictions` |
| **Section `5.4` Effective Saturation** | `~60%–90% (1,080 completed, 64 excluded)` |

### Detailed Metric Checkpoints

- `Execution Summary`: `Initial Kueue Workloads: 192 PodGroup(s)`, `[+64 preempted-t0 resubmit]`.
- `Section 5.3`: **`>= 64`** evictions (`InClusterQueue` on `p-low`).
- `Section 5.4`: `Qualifying Workloads: 1080 (completed=1080)`, `Excluded (Preempted/Unadmitted): 64` (aborted `t0` `PodGroup` runs excluded as badput).

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ Any `t0-qhigh-*` (`p-high`) workload evicted.
- ❌ `Workload` name collision when dynamically resubmitting `64` `t0-qlow-*` `Job`s.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
