# Agent Execution & Cross-Examination Guide — `st19-cohort-reclaim-disabled-lender-waits`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st19-cohort-reclaim-disabled-lender-waits`
- **Category**: 4. Multi-CQ & Cohort Borrowing/Reclaim
- **Title**: Non-Preemptive Cohort Borrowing (`reclaimWithinCohort: Never` — Twin of st18)
- **Primary Verification Target**: Direct A/B twin of `st18` with `reclaimWithinCohort: Never`: zero evictions, lender waits for borrowed leases to finish naturally
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st19` (`--namespace kueue-optimizer-chamber-st19`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16419` | Prometheus `19066` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st18-cohort-reclaim-on-lender-arrival`](../st18-cohort-reclaim-on-lender-arrival/AGENTS.md), [`st42-t0-cohort-borrowing-reclaim-never`](../st42-t0-cohort-borrowing-reclaim-never/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST19 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st19' (API port 16419, Prom port 19066):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st19-cohort-reclaim-disabled-lender-waits --namespace kueue-optimizer-chamber-st19 --port 16419 > /tmp/st19.log 2>&1 &
PID_ST19=$!

# Simultaneously launch paired ST18 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st18' (API port 16418, Prom port 19065):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st18-cohort-reclaim-on-lender-arrival --namespace kueue-optimizer-chamber-st18 --port 16418 > /tmp/st18.log 2>&1 &
PID_ST18=$!

# Simultaneously launch paired ST42 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st42' (API port 16442, Prom port 19089):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st42-t0-cohort-borrowing-reclaim-never --no-skip-initial-offset --namespace kueue-optimizer-chamber-st42 --port 16442 > /tmp/st42.log 2>&1 &
PID_ST42=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST19" "$PID_ST18" "$PID_ST42"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST19 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st19' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st19-cohort-reclaim-disabled-lender-waits --namespace kueue-optimizer-chamber-st19 --port 16419 | tee /tmp/st19.log

# 2. Run paired ST18 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st18':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st18-cohort-reclaim-on-lender-arrival --namespace kueue-optimizer-chamber-st18 --port 16418 | tee /tmp/st18.log

# 3. Run paired ST42 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st42':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st42-t0-cohort-borrowing-reclaim-never --no-skip-initial-offset --namespace kueue-optimizer-chamber-st42 --port 16442 | tee /tmp/st42.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st19-cohort-reclaim-disabled-lender-waits --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st19`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st19-cohort-reclaim-disabled-lender-waits` (`kueue-optimizer-chamber-st19`) ↔ [`st18-cohort-reclaim-on-lender-arrival`](../st18-cohort-reclaim-on-lender-arrival/AGENTS.md) (`kueue-optimizer-chamber-st18`) — Exact A/B Twin (Non-Preemptive Cohort Lending `st19` vs. Instant Quota Reclaim `st18`)

- **Controlled Variable Changed**: ONLY `reclaimWithinCohort` changes (`Never` in `st19` vs. `Any` in `st18`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st18-cohort-reclaim-on-lender-arrival --namespace kueue-optimizer-chamber-st18 --port 16418`
- **Cross-Examination Assertion**: `Sec 5.3 Evictions` MUST be strictly `0` in `st19` vs. `4` in `st18`.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `~77%–82% across 50s makespan` (`st19`) vs. `Cohort 85%–100% throughout` (`st18`)
  - `Sec 5.2 Wait Time`: `cq-lender wait ~21s` (`st19`) vs. `cq-lender wait < 2.0s` (`st18`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st19`) vs. `4 reclaims (4 unique)` (`st18`)
  - `Sec 5.4 Effective Sat`: `~77%–82% (Excluded: 0)` (`st19`) vs. `~72%–90%` (`st18`)

### Pair: `st19-cohort-reclaim-disabled-lender-waits` (`kueue-optimizer-chamber-st19`) ↔ [`st42-t0-cohort-borrowing-reclaim-never`](../st42-t0-cohort-borrowing-reclaim-never/AGENTS.md) (`kueue-optimizer-chamber-st42`) — Scheduled Non-Preemptive Borrowing (`st19`) vs. `t0` Non-Preemptive Borrowing (`st42`) Pair

- **Controlled Variable Changed**: `st19` starts 8 `NotAdmitted` borrower jobs at `t=0s`, whereas `st42` bootstraps 8 `Admitted` `t0` `PodGroup` borrower jobs.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st42-t0-cohort-borrowing-reclaim-never --no-skip-initial-offset --namespace kueue-optimizer-chamber-st42 --port 16442`
- **Cross-Examination Assertion**: Both runs record `0` evictions and hold `cq-lender` in queue (`~19s–21s`) until the borrower finishes.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `~77%–82% across 50s makespan` (`st19`) vs. `cq-borrower 200% in 0s–30s, Cohort ~78%` (`st42`)
  - `Sec 5.2 Wait Time`: `cq-lender wait ~21s` (`st19`) vs. `cq-lender wait ~18s–20s` (`st42`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st19`) vs. `0 evictions` (`st42`)
  - `Sec 5.4 Effective Sat`: `~77%–82% (Excluded: 0)` (`st19`) vs. `~74%–82% (Excluded: 0)` (`st42`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: Identical cohort and arrivals as `st18`, changing `reclaimWithinCohort` from `Any` to `Never`.
- **Expected Runtime Behavior**: `cq-borrower` borrows all `32` TPUs at `t=0s`. When `cq-lender` submits 4 jobs at `t=9s`, `reclaimWithinCohort: Never` forbids killing running borrowers, so all 8 `cq-borrower` jobs finish cleanly at `t=30s` (`0` evictions), and `cq-lender`'s 4 jobs run from `t=30s` to `t=50s` (`wait ≈ 21s`).

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `~77%–82% across 50s makespan` |
| **Section `5.2` Queue Wait Time** | `cq-lender wait ~21s` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `~77%–82% (Excluded: 0)` |

### Detailed Metric Checkpoints

- `Section 5.3 (Evictions)`: Strictly **`0`** (vs `4` in `st18`).
- `Section 5.2`: `cq-lender` wait increases from `<2s` (`st18`) to **`~21s`**.
- `Section 5.4`: Zero wasted compute (`Excluded (preempted/reclaimed/failed): 0 [0 pods]`), `EFFECTIVE SAT ≈ 77%–82%` (`1,280 / 1,600 TPU-s`).

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ Any reclaim eviction (`Section 5.3 > 0`) when `reclaimWithinCohort: Never`.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
