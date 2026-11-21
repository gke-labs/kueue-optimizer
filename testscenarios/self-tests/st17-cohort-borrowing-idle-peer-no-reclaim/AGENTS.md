# Agent Execution & Cross-Examination Guide — `st17-cohort-borrowing-idle-peer-no-reclaim`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and re-run:
> ```bash
> python3 testscenarios/self-tests/generate_self_test_suite.py
> python3 deploy/scripts/generate_scenario_readme.py --all
> python3 testscenarios/self-tests/generate_self_test_suite.py --check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st17-cohort-borrowing-idle-peer-no-reclaim`
- **Category**: 4. Multi-CQ & Cohort Borrowing/Reclaim
- **Title**: 100% Cohort Borrowing From Idle Sister ClusterQueue (`200%` of Nominal Quota — Twin of st16)
- **Primary Verification Target**: Direct A/B twin of `st16` adding `cohortName: cohort-shared`: verifies `cq-alpha` reports `200%` of nominal quota while Cohort and Cluster report `100%`
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st17` (`--namespace kueue-optimizer-chamber-st17`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16417` | Prometheus `19064` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st16-multi-cq-isolated-no-cohort`](../st16-multi-cq-isolated-no-cohort/AGENTS.md), [`st20-borrowing-and-lending-limits-enforced`](../st20-borrowing-and-lending-limits-enforced/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST17 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st17' (API port 16417, Prom port 19064):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st17-cohort-borrowing-idle-peer-no-reclaim --namespace kueue-optimizer-chamber-st17 --port 16417 > /tmp/st17.log 2>&1 &
PID_ST17=$!

# Simultaneously launch paired ST16 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st16' (API port 16416, Prom port 19063):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st16-multi-cq-isolated-no-cohort --namespace kueue-optimizer-chamber-st16 --port 16416 > /tmp/st16.log 2>&1 &
PID_ST16=$!

# Simultaneously launch paired ST20 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st20' (API port 16420, Prom port 19067):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st20-borrowing-and-lending-limits-enforced --namespace kueue-optimizer-chamber-st20 --port 16420 > /tmp/st20.log 2>&1 &
PID_ST20=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST17" "$PID_ST16" "$PID_ST20"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST17 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st17' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st17-cohort-borrowing-idle-peer-no-reclaim --namespace kueue-optimizer-chamber-st17 --port 16417 | tee /tmp/st17.log

# 2. Run paired ST16 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st16':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st16-multi-cq-isolated-no-cohort --namespace kueue-optimizer-chamber-st16 --port 16416 | tee /tmp/st16.log

# 3. Run paired ST20 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st20':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st20-borrowing-and-lending-limits-enforced --namespace kueue-optimizer-chamber-st20 --port 16420 | tee /tmp/st20.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st17-cohort-borrowing-idle-peer-no-reclaim --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st17`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st17-cohort-borrowing-idle-peer-no-reclaim` (`kueue-optimizer-chamber-st17`) ↔ [`st16-multi-cq-isolated-no-cohort`](../st16-multi-cq-isolated-no-cohort/AGENTS.md) (`kueue-optimizer-chamber-st16`) — Exact A/B Twin (Full Cohort Borrowing `200%` vs. Strict No-Cohort Isolation `100%`)

- **Controlled Variable Changed**: Shared `cohort-shared` (`st17`) vs. standalone CQs with no cohort (`st16`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st16-multi-cq-isolated-no-cohort --namespace kueue-optimizer-chamber-st16 --port 16416`
- **Cross-Examination Assertion**: `cq-alpha` in `Sec 5.1` MUST report `~200%` (`32/16` nominal TPUs — never clamped to `100%`), while `Sec 5.4` reports `~96%` (`32/32` physical cluster TPUs — never `200%`).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `cq-alpha ~200%, Cohort ~100%` (`st17`) vs. `ClusterQueue: cq-alpha ~100%, cq-beta 0%, All ~50%` (`st16`)
  - `Sec 5.2 Wait Time`: `< 1.0s` (`st17`) vs. `P50 <=1s, AVG ~0.5*T_wave (~10s @1x)` (`st16`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st17`) vs. `0 evictions` (`st16`)
  - `Sec 5.4 Effective Sat`: `93%–100%` (`st17`) vs. `~50.0%` (`st16`)

### Pair: `st17-cohort-borrowing-idle-peer-no-reclaim` (`kueue-optimizer-chamber-st17`) ↔ [`st20-borrowing-and-lending-limits-enforced`](../st20-borrowing-and-lending-limits-enforced/AGENTS.md) (`kueue-optimizer-chamber-st20`) — Unrestricted Borrowing (`200%` in `st17`) vs. Capped `borrowingLimit` (`150%` in `st20`) Pair

- **Controlled Variable Changed**: No `borrowingLimit` (`st17` borrows all `+16` TPUs -> `32/16 = 200%`) vs. `borrowingLimit: 8` (`st20` borrows max `+8` TPUs -> `24/16 = 150%`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st20-borrowing-and-lending-limits-enforced --namespace kueue-optimizer-chamber-st20 --port 16420`
- **Cross-Examination Assertion**: `cq` peak utilization in `Sec 5.1` steps down from `200.0%` (`st17`) to exact `150.0%` (`st20`) to `100.0%` (`st16`).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `cq-alpha ~200%, Cohort ~100%` (`st17`) vs. `cq-capped P90/P99 = 150.0% (24/16 TPUs)` (`st20`)
  - `Sec 5.2 Wait Time`: `< 1.0s` (`st17`) vs. `6 jobs ~0s, 2 jobs ~22s` (`st20`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st17`) vs. `0 evictions` (`st20`)
  - `Sec 5.4 Effective Sat`: `93%–100%` (`st17`) vs. `~50.0% (Wave 1: 75%, Wave 2: 25%)` (`st20`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: Identical two `16`-TPU CQs (`cq-alpha`, `cq-beta`) as `st16`, now joined in `cohort-shared`. 8 jobs (`32` TPUs total, `30s`) arrive in `cq-alpha` at `t=0s`; `0` jobs in `cq-beta`.
- **Expected Runtime Behavior**: `cq-alpha` immediately uses its own `16` nominal TPUs PLUS borrows all `16` idle TPUs from `cq-beta` (`32` TPUs total), admitting all 8 jobs at `t=0s` in a single `30s` wave with zero waiting and zero evictions.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `cq-alpha ~200%, Cohort ~100%` |
| **Section `5.2` Queue Wait Time** | `< 1.0s` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `93%–100%` |

### Detailed Metric Checkpoints

- `Section 5.1`: `cq-alpha` = **`~190%–200%`** (`32 / 16` nominal TPUs — proves `Section 5.1` does NOT falsely clamp individual borrowing CQs at `100%`), `cq-beta` = **`0.00%`**, and `Cohort: cohort-shared` / `ClusterQueue Quotas (All)` = **`~95%–100%`** (`32 / 32` TPUs).
- `Section 5.2`: `< 1.0s` (all 8 jobs admit immediately).
- `Section 5.4`: **`EFFECTIVE SAT ≈ 93%–100%`** (double `st16`'s `50%`).

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ `Section 5.1` clamping `cq-alpha` at `100%` when it is actively borrowing `32/16` TPUs (`200%`).
- ❌ `Section 5.4` exceeding `100%` (`200%` is per-CQ nominal ratio only; physical cluster saturation is `32/32 = 100%`).

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
python3 testscenarios/self-tests/generate_self_test_suite.py
python3 deploy/scripts/generate_scenario_readme.py --all
python3 testscenarios/self-tests/generate_self_test_suite.py --check
```
