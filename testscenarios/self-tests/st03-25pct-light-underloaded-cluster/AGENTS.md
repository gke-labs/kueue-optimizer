# Agent Execution & Cross-Examination Guide — `st03-25pct-light-underloaded-cluster`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and re-run:
> ```bash
> python3 testscenarios/self-tests/generate_self_test_suite.py
> python3 deploy/scripts/generate_scenario_readme.py --all
> python3 testscenarios/self-tests/generate_self_test_suite.py --check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st03-25pct-light-underloaded-cluster`
- **Category**: 1. Baseline Saturation & Accounting
- **Title**: 25% Under-Saturated Cluster with Unrestricted Queue Quota
- **Primary Verification Target**: Demand-limited (not quota-limited) 25% cluster utilization (`8/32` TPUs)
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st03` (`--namespace kueue-optimizer-chamber-st03`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16403` | Prometheus `19050` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st02-100pct-full-cluster-saturation`](../st02-100pct-full-cluster-saturation/AGENTS.md), [`st01-50pct-single-cq-quota-cap`](../st01-50pct-single-cq-quota-cap/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST03 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st03' (API port 16403, Prom port 19050):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st03-25pct-light-underloaded-cluster --namespace kueue-optimizer-chamber-st03 --port 16403 > /tmp/st03.log 2>&1 &
PID_ST03=$!

# Simultaneously launch paired ST02 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st02' (API port 16402, Prom port 19049):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st02-100pct-full-cluster-saturation --namespace kueue-optimizer-chamber-st02 --port 16402 > /tmp/st02.log 2>&1 &
PID_ST02=$!

# Simultaneously launch paired ST01 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st01' (API port 16401, Prom port 19048):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st01-50pct-single-cq-quota-cap --namespace kueue-optimizer-chamber-st01 --port 16401 > /tmp/st01.log 2>&1 &
PID_ST01=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST03" "$PID_ST02" "$PID_ST01"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST03 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st03' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st03-25pct-light-underloaded-cluster --namespace kueue-optimizer-chamber-st03 --port 16403 | tee /tmp/st03.log

# 2. Run paired ST02 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st02':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st02-100pct-full-cluster-saturation --namespace kueue-optimizer-chamber-st02 --port 16402 | tee /tmp/st02.log

# 3. Run paired ST01 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st01':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st01-50pct-single-cq-quota-cap --namespace kueue-optimizer-chamber-st01 --port 16401 | tee /tmp/st01.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st03-25pct-light-underloaded-cluster --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st03`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st03-25pct-light-underloaded-cluster` (`kueue-optimizer-chamber-st03`) ↔ [`st02-100pct-full-cluster-saturation`](../st02-100pct-full-cluster-saturation/AGENTS.md) (`kueue-optimizer-chamber-st02`) — Demand-Limited `25%` vs. Full `100%` Baseline Pair

- **Controlled Variable Changed**: Submitted job count (`2` vs. `8` single-node `35s` jobs) with identical `32`-TPU cluster and CQ quota.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st02-100pct-full-cluster-saturation --namespace kueue-optimizer-chamber-st02 --port 16402`
- **Cross-Examination Assertion**: In both `st03` and `st02`, `|Sec 5.1 AVG - Sec 5.4 EFFECTIVE SAT| <= 4%`, confirming agreement between Prometheus quota gauges and snapshot-based saturation when `quota == cluster_capacity` and `evictions == 0`.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `23%–26% (8/32 TPUs)` (`st03`) vs. `95%–100% (32/32 TPUs)` (`st02`)
  - `Sec 5.2 Wait Time`: `< 1.0s` (`st03`) vs. `< 1.0s (immediate)` (`st02`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st03`) vs. `0 evictions` (`st02`)
  - `Sec 5.4 Effective Sat`: `23%–25%` (`st03`) vs. `93%–100%` (`st02`)

### Pair: `st03-25pct-light-underloaded-cluster` (`kueue-optimizer-chamber-st03`) ↔ [`st01-50pct-single-cq-quota-cap`](../st01-50pct-single-cq-quota-cap/AGENTS.md) (`kueue-optimizer-chamber-st01`) — Demand-Limited (`st03`) vs. Quota-Limited (`st01`) Contrast Pair

- **Controlled Variable Changed**: `st03` has `32`-TPU quota with `8`-TPU demand (`Sec 5.1 ≈ Sec 5.4 ≈ 25%`); `st01` has `16`-TPU quota with `32`-TPU demand (`Sec 5.1 ≈ 100%`, `Sec 5.4 ≈ 50%`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st01-50pct-single-cq-quota-cap --namespace kueue-optimizer-chamber-st01 --port 16401`
- **Cross-Examination Assertion**: Cross-examining `st03` vs. `st01` verifies whether an idle cluster is starved of incoming demand (`Sec 5.1` low) or bottlenecked by CQ quota configuration (`Sec 5.1 ≈ 100%` while `Sec 5.4` low).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `23%–26% (8/32 TPUs)` (`st03`) vs. `95%–100% (16/16 TPUs)` (`st01`)
  - `Sec 5.2 Wait Time`: `< 1.0s` (`st03`) vs. `P50 <=1s, AVG ~0.5*T_wave (~10s @1x)` (`st01`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st03`) vs. `0 evictions` (`st01`)
  - `Sec 5.4 Effective Sat`: `23%–25%` (`st03`) vs. `~50.0% (640 / 1,280 TPU-s)` (`st01`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 8-node cluster (`32` TPUs) and unrestricted `cq-light` (`32`-TPU quota), but only 2 single-node (`4`-TPU, `35s`) workloads submitted at `t=0s`.
- **Expected Runtime Behavior**: Both jobs admit immediately (`8/32` TPUs used) while 6 nodes (`24` TPUs) remain idle throughout the `35s` run.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `23%–26% (8/32 TPUs)` |
| **Section `5.2` Queue Wait Time** | `< 1.0s` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `23%–25%` |

### Detailed Metric Checkpoints

- `Section 5.1 (cq-light)`: `AVG ≈ 23%–26%` (`8 / 32` TPUs).
- `Section 5.2 (Wait Time)`: `< 1.0s`.
- `Section 5.3 (Evictions)`: `0`.
- `Section 5.4 (Effective Saturation)`: **`EFFECTIVE SAT ≈ 23%–25%`** (`280 / 1,120 TPU-s`), matching `Section 5.1` within `~2%`.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ Discrepancy `|Sec 5.1 AVG - Sec 5.4 EFFECTIVE SAT| > 3%` when CQ quota equals full cluster capacity and zero evictions occurred.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
python3 testscenarios/self-tests/generate_self_test_suite.py
python3 deploy/scripts/generate_scenario_readme.py --all
python3 testscenarios/self-tests/generate_self_test_suite.py --check
```
