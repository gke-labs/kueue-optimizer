# Agent Execution & Cross-Examination Guide — `st20-borrowing-and-lending-limits-enforced`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st20-borrowing-and-lending-limits-enforced`
- **Category**: 4. Multi-CQ & Cohort Borrowing/Reclaim
- **Title**: Explicit Asymmetric `borrowingLimit: 8` vs. `lendingLimit: 12` Cap Verification
- **Primary Verification Target**: Verifies `cq-capped` stops at exact `borrowingLimit: 8` (`16 + 8 = 24` TPUs = `150%` of nominal) even though `cq-donor` permits `lendingLimit: 12`
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st20` (`--namespace kueue-optimizer-chamber-st20`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16420` | Prometheus `19067` (collision-free across all 95 scenarios `st01`–`st95`)
- **Mandatory Paired Counterpart(s)**: [`st43-t0-borrowing-limit-enforced-with-initial-load`](../st43-t0-borrowing-limit-enforced-with-initial-load/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st95`) and non-overlapping port pair (`16401/19048` .. `16495/19142`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST20 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st20' (API port 16420, Prom port 19067):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st20-borrowing-and-lending-limits-enforced --namespace kueue-optimizer-chamber-st20 --port 16420 > /tmp/st20.log 2>&1 &
PID_ST20=$!

# Simultaneously launch paired ST43 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st43' (API port 16443, Prom port 19090):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st43-t0-borrowing-limit-enforced-with-initial-load --namespace kueue-optimizer-chamber-st43 --port 16443 > /tmp/st43.log 2>&1 &
PID_ST43=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST20" "$PID_ST43"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST20 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st20' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st20-borrowing-and-lending-limits-enforced --namespace kueue-optimizer-chamber-st20 --port 16420 | tee /tmp/st20.log

# 2. Run paired ST43 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st43':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st43-t0-borrowing-limit-enforced-with-initial-load --namespace kueue-optimizer-chamber-st43 --port 16443 | tee /tmp/st43.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st20-borrowing-and-lending-limits-enforced --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st20`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st20-borrowing-and-lending-limits-enforced` (`kueue-optimizer-chamber-st20`) ↔ [`st43-t0-borrowing-limit-enforced-with-initial-load`](../st43-t0-borrowing-limit-enforced-with-initial-load/AGENTS.md) (`kueue-optimizer-chamber-st43`) — Greenfield `borrowingLimit: 8` (`st20`, `lendingLimit: 12`) vs. `t0`-Occupied `lendingLimit: 8` (`st43`, `borrowingLimit: 12`) Pair

- **Controlled Variable Changed**: `st20` binds on `borrowingLimit: 8` (`< lendingLimit: 12`) with 8 `NotAdmitted` jobs at `t=0s`, whereas `st43` binds on `lendingLimit: 8` (`< borrowingLimit: 12`) with 4 `t0` `Admitted` jobs (`16` nominal TPUs) + 4 `NotAdmitted` jobs at `t=0s`.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st43-t0-borrowing-limit-enforced-with-initial-load --namespace kueue-optimizer-chamber-st43 --port 16443`
- **Cross-Examination Assertion**: Both runs cap `cq-capped` at **exact `150.0%` (`24/16` TPUs)** in Wave 1 (`6` concurrent jobs = `4` `t0` + `2` arriving in `st43`), leaving 2 nodes idle and 2 jobs queued, proving independent enforcement of both `borrowingLimit` and `lendingLimit`.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `cq-capped P90/P99 = 150.0% (24/16 TPUs)` (`st20`) vs. `cq-capped P90/P99 = 150.0% (24/16 TPUs)` (`st43`)
  - `Sec 5.2 Wait Time`: `6 jobs ~0s, 2 jobs ~22s` (`st20`) vs. `2 jobs ~0s, 2 jobs ~22s (AVG ~11s)` (`st43`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st20`) vs. `0 evictions` (`st43`)
  - `Sec 5.4 Effective Sat`: `~50.0% (Wave 1: 75%, Wave 2: 25%)` (`st20`) vs. `~46%–52% (Wave 1: 75%, Wave 2: 25%)` (`st43`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 8-node (`32`-TPU) cluster, `cohort-limits`. `cq-capped` has `nominalQuota=16, borrowingLimit=8`; `cq-donor` has `nominalQuota=16, lendingLimit=12` (asymmetric so `borrowingLimit=8` is strictly the binding cap). 8 single-node (`4`-TPU, `22s`) jobs submitted to `cq-capped` at `t=0s`; `0` jobs in `cq-donor`.
- **Expected Runtime Behavior**: Wave 1 admits **6 jobs** (`24` TPUs = `16` nominal + `8` borrowed = `150%` of nominal quota) because `cq-capped`'s `borrowingLimit: 8` binds strictly below `cq-donor`'s `lendingLimit: 12`, leaving 2 physical nodes (`8` TPUs) idle and keeping 2 jobs queued for `22s`. Wave 2 runs those last 2 jobs (`8` TPUs) from `22s` to `44s`.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `cq-capped P90/P99 = 150.0% (24/16 TPUs)` |
| **Section `5.2` Queue Wait Time** | `6 jobs ~0s, 2 jobs ~22s` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `~50.0% (Wave 1: 75%, Wave 2: 25%)` |

### Detailed Metric Checkpoints

- `Section 5.1 (cq-capped)`: Peak utilization hits **exact `150.0%`** (`24 / 16` TPUs) during Wave 1 and never exceeds `150%`.
- `Section 5.4 (Effective Saturation)`: `8 × 4 × 22 = 704 TPU-s` over `44s × 32 = 1,408 TPU-s` → **`EFFECTIVE SAT ≈ 47%–52%`** (Wave 1 = `75%`, Wave 2 = `25%`, average = `50%`).

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ `cq-capped` utilization exceeding `150%` (`> 24` TPUs) in `Section 5.1` (would mean `borrowingLimit: 8` was ignored in favor of `lendingLimit: 12`).

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
