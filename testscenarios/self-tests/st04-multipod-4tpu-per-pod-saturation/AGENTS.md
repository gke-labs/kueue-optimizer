# Agent Execution & Cross-Examination Guide — `st04-multipod-4tpu-per-pod-saturation`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st04-multipod-4tpu-per-pod-saturation`
- **Category**: 1. Baseline Saturation & Accounting
- **Title**: Multi-Pod Per-Pod Resource Scaling Verification (2-Pod & 4-Pod JobSets)
- **Primary Verification Target**: Exact `num_pods × per_pod_tpu` accounting in `Section 5.4` + child Job/JobSet deduplication
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st04` (`--namespace kueue-optimizer-chamber-st04`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16404` | Prometheus `19051` (collision-free across all 95 scenarios `st01`–`st95`)
- **Mandatory Paired Counterpart(s)**: [`st02-100pct-full-cluster-saturation`](../st02-100pct-full-cluster-saturation/AGENTS.md), [`st29-multi-role-heterogeneous-jobset`](../st29-multi-role-heterogeneous-jobset/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st95`) and non-overlapping port pair (`16401/19048` .. `16495/19142`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST04 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st04' (API port 16404, Prom port 19051):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st04-multipod-4tpu-per-pod-saturation --namespace kueue-optimizer-chamber-st04 --port 16404 > /tmp/st04.log 2>&1 &
PID_ST04=$!

# Simultaneously launch paired ST02 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st02' (API port 16402, Prom port 19049):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st02-100pct-full-cluster-saturation --namespace kueue-optimizer-chamber-st02 --port 16402 > /tmp/st02.log 2>&1 &
PID_ST02=$!

# Simultaneously launch paired ST29 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st29' (API port 16429, Prom port 19076):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st29-multi-role-heterogeneous-jobset --namespace kueue-optimizer-chamber-st29 --port 16429 > /tmp/st29.log 2>&1 &
PID_ST29=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST04" "$PID_ST02" "$PID_ST29"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST04 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st04' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st04-multipod-4tpu-per-pod-saturation --namespace kueue-optimizer-chamber-st04 --port 16404 | tee /tmp/st04.log

# 2. Run paired ST02 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st02':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st02-100pct-full-cluster-saturation --namespace kueue-optimizer-chamber-st02 --port 16402 | tee /tmp/st02.log

# 3. Run paired ST29 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st29':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st29-multi-role-heterogeneous-jobset --namespace kueue-optimizer-chamber-st29 --port 16429 | tee /tmp/st29.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st04-multipod-4tpu-per-pod-saturation --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st04`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st04-multipod-4tpu-per-pod-saturation` (`kueue-optimizer-chamber-st04`) ↔ [`st02-100pct-full-cluster-saturation`](../st02-100pct-full-cluster-saturation/AGENTS.md) (`kueue-optimizer-chamber-st02`) — Multi-Pod (`2x4` + `4x4` TPUs) vs. Single-Pod (`8x4` TPUs) Iso-Capacity Pair

- **Controlled Variable Changed**: `3` multi-pod JobSets totaling `8` pods (`32` TPUs, `st04`) vs. `8` single-pod JobSets totaling `8` pods (`32` TPUs, `st02`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st02-100pct-full-cluster-saturation --namespace kueue-optimizer-chamber-st02 --port 16402`
- **Cross-Examination Assertion**: `Sec 5.4 EFFECTIVE SAT` for `google.com/tpu` MUST be `93%–100%` in BOTH `st04` and `st02`, proving per-pod TPU requests are multiplied by `num_pods` and parent/child JobSet workloads are never double-counted.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `95%–100% (32/32 TPUs)` (`st04`) vs. `95%–100% (32/32 TPUs)` (`st02`)
  - `Sec 5.2 Wait Time`: `< 1.0s` (`st04`) vs. `< 1.0s (immediate)` (`st02`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st04`) vs. `0 evictions` (`st02`)
  - `Sec 5.4 Effective Sat`: `93%–100% (3 workloads, 8 pods)` (`st04`) vs. `93%–100%` (`st02`)

### Pair: `st04-multipod-4tpu-per-pod-saturation` (`kueue-optimizer-chamber-st04`) ↔ [`st29-multi-role-heterogeneous-jobset`](../st29-multi-role-heterogeneous-jobset/AGENTS.md) (`kueue-optimizer-chamber-st29`) — Homogeneous Multi-Pod (`st04`) vs. Heterogeneous Multi-Role (`st29`) JobSet Pair

- **Controlled Variable Changed**: Single `replicatedJob` (`w`) per JobSet in `st04` vs. dual `replicatedJobs` (`coord` with `0` TPUs + `w` with `4` TPUs/pod) in `st29`.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st29-multi-role-heterogeneous-jobset --namespace kueue-optimizer-chamber-st29 --port 16429`
- **Cross-Examination Assertion**: Both scenarios pack `32/32` cluster TPUs (`Sec 5.4 ≈ 93%–100%`), while `st29` reports `10` total succeeded pods (`2` CPU coordinators + `8` TPU workers) vs. `8` pods in `st04`.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `95%–100% (32/32 TPUs)` (`st04`) vs. `tpu ~100%, cpu ~31.25%` (`st29`)
  - `Sec 5.2 Wait Time`: `< 1.0s` (`st04`) vs. `< 1.0s` (`st29`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st04`) vs. `0 evictions` (`st29`)
  - `Sec 5.4 Effective Sat`: `93%–100% (3 workloads, 8 pods)` (`st04`) vs. `tpu ~96% (2 workloads, 10 pods)` (`st29`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 8-node cluster (`32` TPUs = `8 × 4` TPUs). Only 3 `JobSet` objects submitted at `t=0s`: two 2-pod JobSets (`2 pods × 4 TPUs = 8 TPUs` each) and one 4-pod JobSet (`4 pods × 4 TPUs = 16 TPUs`), totaling `8 + 8 + 16 = 32 TPUs` (`8` pods across `3` workloads) for `35s`.
- **Expected Runtime Behavior**: All 3 multi-pod JobSets admit together at `t=0s`, launching `2 + 2 + 4 = 8` pods that fill all 8 nodes (`32/32` TPUs) at 100% saturation.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `95%–100% (32/32 TPUs)` |
| **Section `5.2` Queue Wait Time** | `< 1.0s` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `93%–100% (3 workloads, 8 pods)` |

### Detailed Metric Checkpoints

- `Execution Summary`: `Workflows Submitted: 3/3`, `Workloads Status: ... finished=3`, `KWOK Pods Status: ... succeeded=8`.
- `Section 5.1 (cq-mpod)`: `AVG ≈ 95%–100%` (`32 / 32` TPUs).
- `Section 5.4 (Effective Saturation)`: Subtitle reports `Qualifying Workloads: 3 (completed=3 [8 pods] ...)`. `EFFECTIVE SAT` for `google.com/tpu` MUST be **`93%–100%`** (`1,120 / 1,120 TPU-s`).

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ `Section 5.4` reporting `37.5%` (`12/32` TPUs — classic bug of counting 1 pod per JobSet instead of `num_pods × per_pod_tpu`).
- ❌ `Section 5.4` reporting `>100%` / `200%` (classic bug of double-counting both parent `JobSet` Workload and child `batch/v1 Job` Workload).

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
