# Agent Execution & Cross-Examination Guide — `st01-50pct-single-cq-quota-cap`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st01-50pct-single-cq-quota-cap`
- **Category**: 1. Baseline Saturation & Accounting
- **Title**: 50% Cluster Saturation Limited by Single ClusterQueue Quota
- **Primary Verification Target**: Single CQ quota ceiling (`16/32` TPUs = 50% of physical cluster) across 2 back-to-back waves
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st01` (`--namespace kueue-optimizer-chamber-st01`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16401` | Prometheus `19048` (collision-free across all 95 scenarios `st01`–`st95`)
- **Mandatory Paired Counterpart(s)**: [`st02-100pct-full-cluster-saturation`](../st02-100pct-full-cluster-saturation/AGENTS.md), [`st16-multi-cq-isolated-no-cohort`](../st16-multi-cq-isolated-no-cohort/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st95`) and non-overlapping port pair (`16401/19048` .. `16495/19142`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST01 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st01' (API port 16401, Prom port 19048):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st01-50pct-single-cq-quota-cap --namespace kueue-optimizer-chamber-st01 --port 16401 > /tmp/st01.log 2>&1 &
PID_ST01=$!

# Simultaneously launch paired ST02 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st02' (API port 16402, Prom port 19049):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st02-100pct-full-cluster-saturation --namespace kueue-optimizer-chamber-st02 --port 16402 > /tmp/st02.log 2>&1 &
PID_ST02=$!

# Simultaneously launch paired ST16 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st16' (API port 16416, Prom port 19063):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st16-multi-cq-isolated-no-cohort --namespace kueue-optimizer-chamber-st16 --port 16416 > /tmp/st16.log 2>&1 &
PID_ST16=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST01" "$PID_ST02" "$PID_ST16"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST01 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st01' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st01-50pct-single-cq-quota-cap --namespace kueue-optimizer-chamber-st01 --port 16401 | tee /tmp/st01.log

# 2. Run paired ST02 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st02':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st02-100pct-full-cluster-saturation --namespace kueue-optimizer-chamber-st02 --port 16402 | tee /tmp/st02.log

# 3. Run paired ST16 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st16':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st16-multi-cq-isolated-no-cohort --namespace kueue-optimizer-chamber-st16 --port 16416 | tee /tmp/st16.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st01-50pct-single-cq-quota-cap --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st01`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st01-50pct-single-cq-quota-cap` (`kueue-optimizer-chamber-st01`) ↔ [`st02-100pct-full-cluster-saturation`](../st02-100pct-full-cluster-saturation/AGENTS.md) (`kueue-optimizer-chamber-st02`) — Quota Ceiling vs. Full Cluster Capacity Pair

- **Controlled Variable Changed**: `cq-half` nominal quota (`16` TPUs = `50%` of cluster) in `st01` vs. `cq-full` (`32` TPUs = `100%` of cluster) in `st02`.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st02-100pct-full-cluster-saturation --namespace kueue-optimizer-chamber-st02 --port 16402`
- **Cross-Examination Assertion**: `Sec 5.1 AVG` MUST be `~100%` in BOTH `st01` (`16/16`) and `st02` (`32/32`), whereas `Sec 5.4 EFFECTIVE SAT` MUST drop by half from `~96%–100%` (`st02`) to `~50.0%` (`st01`), proving `Sec 5.1` normalizes by CQ nominal quota while `Sec 5.4` normalizes by physical cluster capacity.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `95%–100% (16/16 TPUs)` (`st01`) vs. `95%–100% (32/32 TPUs)` (`st02`)
  - `Sec 5.2 Wait Time`: `P50 <=1s, AVG ~0.5*T_wave (~10s @1x)` (`st01`) vs. `< 1.0s (immediate)` (`st02`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st01`) vs. `0 evictions` (`st02`)
  - `Sec 5.4 Effective Sat`: `~50.0% (640 / 1,280 TPU-s)` (`st01`) vs. `93%–100%` (`st02`)

### Pair: `st01-50pct-single-cq-quota-cap` (`kueue-optimizer-chamber-st01`) ↔ [`st16-multi-cq-isolated-no-cohort`](../st16-multi-cq-isolated-no-cohort/AGENTS.md) (`kueue-optimizer-chamber-st16`) — Single Quota-Capped CQ vs. Two Isolated CQs Equivalence Pair

- **Controlled Variable Changed**: Single `16`-TPU CQ (`st01`) vs. two isolated `16`-TPU CQs (`cq-alpha`, `cq-beta`, no cohort) where only `cq-alpha` receives load (`st16`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st16-multi-cq-isolated-no-cohort --namespace kueue-optimizer-chamber-st16 --port 16416`
- **Cross-Examination Assertion**: Both runs execute the exact same 2 waves of 4 jobs (`40s` total) and MUST yield virtually identical `Sec 5.4 EFFECTIVE SAT ≈ 50.0%` and `Sec 5.2 AVG ≈ 10s`.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `95%–100% (16/16 TPUs)` (`st01`) vs. `ClusterQueue: cq-alpha ~100%, cq-beta 0%, All ~50%` (`st16`)
  - `Sec 5.2 Wait Time`: `P50 <=1s, AVG ~0.5*T_wave (~10s @1x)` (`st01`) vs. `P50 <=1s, AVG ~0.5*T_wave (~10s @1x)` (`st16`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st01`) vs. `0 evictions` (`st16`)
  - `Sec 5.4 Effective Sat`: `~50.0% (640 / 1,280 TPU-s)` (`st01`) vs. `~50.0%` (`st16`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 8-node cluster (`32` TPUs total). Single `ClusterQueue` `cq-half` configured with `nominalQuota = 16` TPUs (`50%` of cluster) and no cohort. 8 single-node (`4`-TPU, `20s`) jobs arrive simultaneously at `t=0s`.
- **Expected Runtime Behavior**: Wave 1 (4 jobs = `16` TPUs) admits at `t=0s` and runs from `0s` to `20s` while 4 physical nodes sit completely idle due to the CQ quota ceiling. Wave 2 (remaining 4 jobs = `16` TPUs) waits `20s` in queue, admits at `t=20s`, and finishes at `t=40s`.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `95%–100% (16/16 TPUs)` |
| **Section `5.2` Queue Wait Time** | `P50 <=1s, AVG ~0.5*T_wave (~10s @1x)` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `~50.0% (640 / 1,280 TPU-s)` |

### Detailed Metric Checkpoints

- `Execution Summary`: `Workflows Submitted: 8/8`, `Workloads Status: ... finished=8`, `KWOK Pods Status: ... succeeded=8, failed=0`.
- `Section 5.1 (ClusterQueue: cq-half / All)`: `P50 = P90 = P99 = 100.00%`, `AVG ≈ 95%–100%` (`16 / 16` TPUs) because the CQ's own `16`-TPU quota is 100% full across both waves.
- `Section 5.2 (Wait Time)`: Exact 4+4 bimodal wave split (`<=1s: 4` from Wave 1, `~T_wave` for Wave 2 where `T_wave = 20s / speedup`): Prometheus `P50 <= 1.00s` (exact `4/8 = 50%` cumulative count at `le="1.0"`), while `AVG ≈ 0.5 * T_wave` (`~10s` at `1.0x`) and `P90 ≈ 0.9 * T_wave` (`~19s` at `1.0x`).
- `Section 5.3 (Evictions)`: `0` total evictions, `0` unique workloads evicted.
- `Section 5.4 (Effective Saturation)`: `8 jobs × 4 TPUs × T_wave` divided by `32 cluster TPUs × 2 * T_wave` → **`EFFECTIVE SAT ≈ 50.0%` (`46%–52%` across any `--speedup`)**, proving `Sec 5.1` (quota-relative = `100%`) and `Sec 5.4` (cluster-relative = `50%`) use their distinct denominators accurately.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ `Section 5.4 EFFECTIVE SAT` MUST NOT report `100%` (which would mean dividing by CQ quota `16` instead of physical cluster capacity `32`).
- ❌ `Section 5.1 AVG` MUST NOT report `50%` (which would mean dividing CQ usage `16` by cluster capacity `32` instead of `cq-half` nominal quota `16`).
- ❌ Zero workloads may be preempted or fail (`Evictions == 0`, `failed == 0`).

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
