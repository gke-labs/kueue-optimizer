# Agent Execution & Cross-Examination Guide — `st43-t0-borrowing-limit-enforced-with-initial-load`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st43-t0-borrowing-limit-enforced-with-initial-load`
- **Category**: 9. Initial t0 Workloads — Reclaims, Limits & Fair Sharing
- **Title**: `borrowingLimit: 8` & `lendingLimit: 8` Enforced on Top of `t0` Nominal Quota Load (`150%` Ceiling)
- **Primary Verification Target**: Verifies `16` TPUs of `t0` nominal usage + `8` TPUs of `t=0s` arrivals hit the exact `24`-TPU (`150.0%`) borrowing ceiling while keeping 2 nodes idle and 2 arrivals queued
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st43` (`--namespace kueue-optimizer-chamber-st43`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16443` | Prometheus `19090` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st20-borrowing-and-lending-limits-enforced`](../st20-borrowing-and-lending-limits-enforced/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST43 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st43' (API port 16443, Prom port 19090):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st43-t0-borrowing-limit-enforced-with-initial-load --namespace kueue-optimizer-chamber-st43 --port 16443 > /tmp/st43.log 2>&1 &
PID_ST43=$!

# Simultaneously launch paired ST20 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st20' (API port 16420, Prom port 19067):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st20-borrowing-and-lending-limits-enforced --namespace kueue-optimizer-chamber-st20 --port 16420 > /tmp/st20.log 2>&1 &
PID_ST20=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST43" "$PID_ST20"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST43 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st43' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st43-t0-borrowing-limit-enforced-with-initial-load --namespace kueue-optimizer-chamber-st43 --port 16443 | tee /tmp/st43.log

# 2. Run paired ST20 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st20':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st20-borrowing-and-lending-limits-enforced --namespace kueue-optimizer-chamber-st20 --port 16420 | tee /tmp/st20.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st43-t0-borrowing-limit-enforced-with-initial-load --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st43`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st43-t0-borrowing-limit-enforced-with-initial-load` (`kueue-optimizer-chamber-st43`) ↔ [`st20-borrowing-and-lending-limits-enforced`](../st20-borrowing-and-lending-limits-enforced/AGENTS.md) (`kueue-optimizer-chamber-st20`) — `t0`-Loaded `borrowingLimit: 8` (`st43`) vs. Fresh `borrowingLimit: 8` (`st20`) Pair

- **Controlled Variable Changed**: `st43` pre-fills `cq-capped`'s `16` nominal TPUs with 4 `t0` `PodGroup`s before 4 arrivals submit at `t=0s`, whereas `st20` submits all 8 as `NotAdmitted` at `t=0s`.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st20-borrowing-and-lending-limits-enforced --namespace kueue-optimizer-chamber-st20 --port 16420`
- **Cross-Examination Assertion**: Both enforce the exact `24`-TPU (`150.0%`) ceiling in `Sec 5.1` and yield `Sec 5.4 EFFECTIVE SAT ≈ 50.0%` (`2` arrivals admitted immediately + `2` queued `~22s` in `st43`).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `cq-capped P90/P99 = 150.0% (24/16 TPUs)` (`st43`) vs. `cq-capped P90/P99 = 150.0% (24/16 TPUs)` (`st20`)
  - `Sec 5.2 Wait Time`: `2 jobs ~0s, 2 jobs ~22s (AVG ~11s)` (`st43`) vs. `6 jobs ~0s, 2 jobs ~22s` (`st20`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st43`) vs. `0 evictions` (`st20`)
  - `Sec 5.4 Effective Sat`: `~46%–52% (Wave 1: 75%, Wave 2: 25%)` (`st43`) vs. `~50.0% (Wave 1: 75%, Wave 2: 25%)` (`st20`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 8-node (`32`-TPU) cluster, `cohort-t0-lim`. `cq-capped` (`nominalQuota=16, borrowingLimit=8`); `cq-donor` (`nominalQuota=16, lendingLimit=8`). At `t0`, 4 `Admitted` `init-cap-*` jobs (`16` TPUs, `22s`) already fill `cq-capped`'s nominal quota. At `t=0s`, 4 `NotAdmitted` `arr-cap-*` jobs (`16` TPUs, `22s`) arrive.
- **Expected Runtime Behavior**: Kueue accounts for `init-cap-0..3`'s `16` TPUs, admits **only 2** of the 4 `arr-cap-*` arrivals at `t=0s` (`16 + 8 = 24` TPUs = `150.0%` of nominal, leaving 2 nodes idle), queues the other 2 `arr-cap-*` jobs for `~22s`, and drains them in `22s–44s`.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `cq-capped P90/P99 = 150.0% (24/16 TPUs)` |
| **Section `5.2` Queue Wait Time** | `2 jobs ~0s, 2 jobs ~22s (AVG ~11s)` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `~46%–52% (Wave 1: 75%, Wave 2: 25%)` |

### Detailed Metric Checkpoints

- `Section 5.1 (cq-capped)`: Peak utilization hits **exact `150.0%`** (`24 / 16` TPUs) and NEVER reaches `200%` (`32 / 16`).
- `Section 5.2`: 2 immediate arrivals (`<= 1.0s`) + 2 queued arrivals (`~22s`), `AVG ≈ 10s–12s`.
- `Section 5.4`: `Qualifying Workloads: 8 (completed=8 [8 pods])`, **`EFFECTIVE SAT ≈ 46%–52%`** (`704 / 1,408 TPU-s = 50.0%`).

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ `cq-capped` admitting all 4 `arr-cap-*` jobs at `t=0s` (`32` TPUs = `200%`, which would happen if `init-cap-*`'s `16` TPUs were missing from `cq-capped` quota accounting).

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
