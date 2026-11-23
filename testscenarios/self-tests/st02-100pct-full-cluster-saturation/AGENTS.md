# Agent Execution & Cross-Examination Guide — `st02-100pct-full-cluster-saturation`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st02-100pct-full-cluster-saturation`
- **Category**: 1. Baseline Saturation & Accounting
- **Title**: 100% Full Cluster Saturation in a Single Immediate Wave
- **Primary Verification Target**: Full `32/32` TPU saturation with zero queueing delay and zero evictions
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st02` (`--namespace kueue-optimizer-chamber-st02`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16402` | Prometheus `19049` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st01-50pct-single-cq-quota-cap`](../st01-50pct-single-cq-quota-cap/AGENTS.md), [`st03-25pct-light-underloaded-cluster`](../st03-25pct-light-underloaded-cluster/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST02 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st02' (API port 16402, Prom port 19049):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st02-100pct-full-cluster-saturation --namespace kueue-optimizer-chamber-st02 --port 16402 > /tmp/st02.log 2>&1 &
PID_ST02=$!

# Simultaneously launch paired ST01 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st01' (API port 16401, Prom port 19048):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st01-50pct-single-cq-quota-cap --namespace kueue-optimizer-chamber-st01 --port 16401 > /tmp/st01.log 2>&1 &
PID_ST01=$!

# Simultaneously launch paired ST03 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st03' (API port 16403, Prom port 19050):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st03-25pct-light-underloaded-cluster --namespace kueue-optimizer-chamber-st03 --port 16403 > /tmp/st03.log 2>&1 &
PID_ST03=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST02" "$PID_ST01" "$PID_ST03"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST02 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st02' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st02-100pct-full-cluster-saturation --namespace kueue-optimizer-chamber-st02 --port 16402 | tee /tmp/st02.log

# 2. Run paired ST01 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st01':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st01-50pct-single-cq-quota-cap --namespace kueue-optimizer-chamber-st01 --port 16401 | tee /tmp/st01.log

# 3. Run paired ST03 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st03':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st03-25pct-light-underloaded-cluster --namespace kueue-optimizer-chamber-st03 --port 16403 | tee /tmp/st03.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st02-100pct-full-cluster-saturation --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st02`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st02-100pct-full-cluster-saturation` (`kueue-optimizer-chamber-st02`) ↔ [`st01-50pct-single-cq-quota-cap`](../st01-50pct-single-cq-quota-cap/AGENTS.md) (`kueue-optimizer-chamber-st01`) — Linear Saturation Scaling Ladder (`100%` vs. `50%`)

- **Controlled Variable Changed**: `32`-TPU full-cluster quota (`st02`) vs. `16`-TPU half-cluster quota (`st01`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st01-50pct-single-cq-quota-cap --namespace kueue-optimizer-chamber-st01 --port 16401`
- **Cross-Examination Assertion**: `Sec 5.4(st02) / Sec 5.4(st01) ≈ 2.0x` (`~96%` vs. `~50%`) with `0` evictions in both.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `95%–100% (32/32 TPUs)` (`st02`) vs. `95%–100% (16/16 TPUs)` (`st01`)
  - `Sec 5.2 Wait Time`: `< 1.0s (immediate)` (`st02`) vs. `P50 <=1s, AVG ~0.5*T_wave (~10s @1x)` (`st01`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st02`) vs. `0 evictions` (`st01`)
  - `Sec 5.4 Effective Sat`: `93%–100%` (`st02`) vs. `~50.0% (640 / 1,280 TPU-s)` (`st01`)

### Pair: `st02-100pct-full-cluster-saturation` (`kueue-optimizer-chamber-st02`) ↔ [`st03-25pct-light-underloaded-cluster`](../st03-25pct-light-underloaded-cluster/AGENTS.md) (`kueue-optimizer-chamber-st03`) — Linear Saturation Scaling Ladder (`100%` vs. `25%`)

- **Controlled Variable Changed**: `8` single-node jobs (`32` TPUs, `st02`) vs. `2` single-node jobs (`8` TPUs, `st03`) under the same unrestricted `32`-TPU quota.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st03-25pct-light-underloaded-cluster --namespace kueue-optimizer-chamber-st03 --port 16403`
- **Cross-Examination Assertion**: `Sec 5.4(st02) / Sec 5.4(st03) ≈ 4.0x` (`~96%` vs. `~24%`) and `Sec 5.1(st02) / Sec 5.1(st03) ≈ 4.0x`.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `95%–100% (32/32 TPUs)` (`st02`) vs. `23%–26% (8/32 TPUs)` (`st03`)
  - `Sec 5.2 Wait Time`: `< 1.0s (immediate)` (`st02`) vs. `< 1.0s` (`st03`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st02`) vs. `0 evictions` (`st03`)
  - `Sec 5.4 Effective Sat`: `93%–100%` (`st02`) vs. `23%–25%` (`st03`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 8-node cluster (`32` TPUs). Single `ClusterQueue` `cq-full` with full `32`-TPU quota. 8 single-node (`4`-TPU, `35s`) workloads submitted at `t=0s`.
- **Expected Runtime Behavior**: All 8 workloads admit immediately at `t=0s`, occupy all 8 nodes (`32/32` TPUs) continuously for `35s`, and finish together at `t=35s`.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `95%–100% (32/32 TPUs)` |
| **Section `5.2` Queue Wait Time** | `< 1.0s (immediate)` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `93%–100%` |

### Detailed Metric Checkpoints

- `Section 5.1 (cq-full)`: `AVG ≈ 95%–100%` (`32 / 32` TPUs).
- `Section 5.2 (Wait Time)`: `P50 < 0.5s`, `P99 < 1.5s` (pure controller admission latency).
- `Section 5.3 (Evictions)`: `0`.
- `Section 5.4 (Effective Saturation)`: `8 × 4 × 35 = 1,120 TPU-s` over `~35s` → **`EFFECTIVE SAT ≈ 93%–100%`** (`COMPLETED ONLY ≈ 93%–100%`, `ACTIVE AT END = 0.00%`).

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ `EFFECTIVE SAT` exceeding `100.00%` (would indicate wall-clock evaluation window under-measurement or duplicate pod counting).
- ❌ Any workload waiting in `kueue_pending_workloads` after initial admission cycle.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
