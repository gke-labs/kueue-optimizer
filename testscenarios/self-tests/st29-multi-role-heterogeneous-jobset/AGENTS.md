# Agent Execution & Cross-Examination Guide — `st29-multi-role-heterogeneous-jobset`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and re-run:
> ```bash
> python3 testscenarios/self-tests/generate_self_test_suite.py
> python3 deploy/scripts/generate_scenario_readme.py --all
> python3 testscenarios/self-tests/generate_self_test_suite.py --check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st29-multi-role-heterogeneous-jobset`
- **Category**: 6. Tricky Accounting & Edge Cases
- **Title**: Multi-Role Heterogeneous `JobSet` (`1` CPU Coordinator + `4` TPU Workers per JobSet)
- **Primary Verification Target**: Accumulation across multiple `spec.podSets` (`coord` with `0` TPUs + `w` with `4×4=16` TPUs) inside a single Kueue `Workload`
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st29` (`--namespace kueue-optimizer-chamber-st29`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16429` | Prometheus `19076` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st34-t0-multi-role-heterogeneous-podgroup`](../st34-t0-multi-role-heterogeneous-podgroup/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST29 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st29' (API port 16429, Prom port 19076):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st29-multi-role-heterogeneous-jobset --namespace kueue-optimizer-chamber-st29 --port 16429 > /tmp/st29.log 2>&1 &
PID_ST29=$!

# Simultaneously launch paired ST34 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st34' (API port 16434, Prom port 19081):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st34-t0-multi-role-heterogeneous-podgroup --namespace kueue-optimizer-chamber-st34 --port 16434 > /tmp/st34.log 2>&1 &
PID_ST34=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST29" "$PID_ST34"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST29 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st29' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st29-multi-role-heterogeneous-jobset --namespace kueue-optimizer-chamber-st29 --port 16429 | tee /tmp/st29.log

# 2. Run paired ST34 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st34':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st34-t0-multi-role-heterogeneous-podgroup --namespace kueue-optimizer-chamber-st34 --port 16434 | tee /tmp/st34.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st29-multi-role-heterogeneous-jobset --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st29`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st29-multi-role-heterogeneous-jobset` (`kueue-optimizer-chamber-st29`) ↔ [`st34-t0-multi-role-heterogeneous-podgroup`](../st34-t0-multi-role-heterogeneous-podgroup/AGENTS.md) (`kueue-optimizer-chamber-st34`) — Scheduled Multi-Role `JobSet` (`st29`) vs. Pre-Admitted `t0` Multi-Role `PodGroup` (`st34`) Pair

- **Controlled Variable Changed**: `st29` submits 2 multi-role JobSets (`coord` + `w`) as `NotAdmitted` at `t=0s`, whereas `st34` bootstraps them as pre-admitted `t0` `PodGroup`s and queues a 3rd multi-role arrival behind them.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st34-t0-multi-role-heterogeneous-podgroup --namespace kueue-optimizer-chamber-st34 --port 16434`
- **Cross-Examination Assertion**: Both scenarios preserve exact multi-PodSet resource totals (`16` TPUs + `20` CPUs across `5` pods per JobSet) without `WorkloadWebhook` errors.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `tpu ~100%, cpu ~31.25%` (`st29`) vs. `tpu ~75%–82% (100% w1, 50% w2)` (`st34`)
  - `Sec 5.2 Wait Time`: `< 1.0s` (`st29`) vs. `arr-mrole-after waits ~24s` (`st34`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st29`) vs. `0 evictions` (`st34`)
  - `Sec 5.4 Effective Sat`: `tpu ~96% (2 workloads, 10 pods)` (`st29`) vs. `tpu ~75%–82% (3 workloads, 15 pods)` (`st34`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 8-node (`32`-TPU, `128`-CPU) cluster. 2 multi-role `JobSet`s (`js-dist-train-0`, `js-dist-train-1`, `35s` duration), each containing two `replicatedJobs`: `coord` (`1` pod × `0` TPUs, `4` CPUs) and `w` (`4` pods × `4` TPUs, `4` CPUs). Total across both JobSets: `2` workloads, `10` pods (`2` coordinators + `8` workers), `32/32` TPUs (`100%`), `40/128` CPUs (`31.25%`).
- **Expected Runtime Behavior**: Both JobSets admit immediately at `t=0s`, launching all `10` pods across the 8 nodes and running to completion at `t=35s`.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `tpu ~100%, cpu ~31.25%` |
| **Section `5.2` Queue Wait Time** | `< 1.0s` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `tpu ~96% (2 workloads, 10 pods)` |

### Detailed Metric Checkpoints

- `Execution Summary`: `Workflows Submitted: 2/2`, `Workloads Status: ... finished=2`, `KWOK Pods Status: ... succeeded=10`.
- `Section 5.4`: Subtitle shows `Qualifying Workloads: 2 (completed=2 [10 pods] ...)`. `google.com/tpu` = **`93%–100%`** (`32/32` TPUs), `cpu` = **`~29%–32%`** (`40/128` CPUs).

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ `Section 5.4` reading only `podSets[0]` (`coord` = `0` TPUs) and reporting **`0.00%` TPU saturation**.
- ❌ `Section 5.4` multiplying `coord` resources by `10` pods (`0` TPUs) or `w` resources by `10` pods (`40` TPUs > `32` cluster cap = `125%`).

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
python3 testscenarios/self-tests/generate_self_test_suite.py
python3 deploy/scripts/generate_scenario_readme.py --all
python3 testscenarios/self-tests/generate_self_test_suite.py --check
```
