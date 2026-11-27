# Agent Execution & Cross-Examination Guide — `st41-t0-cohort-borrowing-reclaim-any`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st41-t0-cohort-borrowing-reclaim-any`
- **Category**: 9. Initial t0 Workloads — Reclaims, Limits & Fair Sharing
- **Title**: Cohort Reclamation (`reclaimWithinCohort: Any`) Against `t0` Borrowing `PodGroup`s (`200% -> 100%`)
- **Primary Verification Target**: Verifies `Cache.AddClusterQueue` registers `t0` borrowed Cohort usage (`32/16 = 200%`) so `cq-lender` immediately reclaims **exact 4** `t0` `PodGroup`s at `t=11s`
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st41` (`--namespace kueue-optimizer-chamber-st41`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16441` | Prometheus `19088` (collision-free across all 95 scenarios `st01`–`st95`)
- **Mandatory Paired Counterpart(s)**: [`st42-t0-cohort-borrowing-reclaim-never`](../st42-t0-cohort-borrowing-reclaim-never/AGENTS.md), [`st18-cohort-reclaim-on-lender-arrival`](../st18-cohort-reclaim-on-lender-arrival/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st95`) and non-overlapping port pair (`16401/19048` .. `16495/19142`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST41 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st41' (API port 16441, Prom port 19088):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st41-t0-cohort-borrowing-reclaim-any --no-skip-initial-offset --namespace kueue-optimizer-chamber-st41 --port 16441 > /tmp/st41.log 2>&1 &
PID_ST41=$!

# Simultaneously launch paired ST42 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st42' (API port 16442, Prom port 19089):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st42-t0-cohort-borrowing-reclaim-never --no-skip-initial-offset --namespace kueue-optimizer-chamber-st42 --port 16442 > /tmp/st42.log 2>&1 &
PID_ST42=$!

# Simultaneously launch paired ST18 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st18' (API port 16418, Prom port 19065):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st18-cohort-reclaim-on-lender-arrival --namespace kueue-optimizer-chamber-st18 --port 16418 > /tmp/st18.log 2>&1 &
PID_ST18=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST41" "$PID_ST42" "$PID_ST18"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST41 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st41' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st41-t0-cohort-borrowing-reclaim-any --no-skip-initial-offset --namespace kueue-optimizer-chamber-st41 --port 16441 | tee /tmp/st41.log

# 2. Run paired ST42 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st42':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st42-t0-cohort-borrowing-reclaim-never --no-skip-initial-offset --namespace kueue-optimizer-chamber-st42 --port 16442 | tee /tmp/st42.log

# 3. Run paired ST18 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st18':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st18-cohort-reclaim-on-lender-arrival --namespace kueue-optimizer-chamber-st18 --port 16418 | tee /tmp/st18.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st41-t0-cohort-borrowing-reclaim-any --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st41`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st41-t0-cohort-borrowing-reclaim-any` (`kueue-optimizer-chamber-st41`) ↔ [`st42-t0-cohort-borrowing-reclaim-never`](../st42-t0-cohort-borrowing-reclaim-never/AGENTS.md) (`kueue-optimizer-chamber-st42`) — Exact A/B Twin (`t0` Cohort Borrowing with `reclaimWithinCohort: Any` vs. `Never`)

- **Controlled Variable Changed**: ONLY `reclaimWithinCohort` changes (`Any` in `st41` vs. `Never` in `st42`); all 8 `t0` borrower `PodGroup`s (`32/16 = 200%`) and 4 `t=11s` lender arrivals are identical.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st42-t0-cohort-borrowing-reclaim-never --no-skip-initial-offset --namespace kueue-optimizer-chamber-st42 --port 16442`
- **Cross-Examination Assertion**: `Sec 5.3 Evictions` flips from `4` (`st41`, reclaiming `cq-lender`'s `16` TPUs while protecting `cq-borrower`'s `16` nominal TPUs) to `0` (`st42`), and `cq-lender` wait jumps from `< 2.5s` (`st41`) to `~19s` (`st42`).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `cq-borrower P90/P99=200%, Cohort ~85%–98%` (`st41`) vs. `cq-borrower 200% in 0s–30s, Cohort ~78%` (`st42`)
  - `Sec 5.2 Wait Time`: `cq-lender wait < 2.5s` (`st41`) vs. `cq-lender wait ~18s–20s` (`st42`)
  - `Sec 5.3 Evictions`: `4 reclaims (4 unique)` (`st41`) vs. `0 evictions` (`st42`)
  - `Sec 5.4 Effective Sat`: `~70%–88% (12 completed)` (`st41`) vs. `~74%–82% (Excluded: 0)` (`st42`)

### Pair: `st41-t0-cohort-borrowing-reclaim-any` (`kueue-optimizer-chamber-st41`) ↔ [`st18-cohort-reclaim-on-lender-arrival`](../st18-cohort-reclaim-on-lender-arrival/AGENTS.md) (`kueue-optimizer-chamber-st18`) — `t0` Borrower Reclaim (`st41`) vs. Scheduled Borrower Reclaim (`st18`) Pair

- **Controlled Variable Changed**: Bootstrapped `t0` `PodGroup` borrowers (`st41`) vs. `t=0s` scheduled borrowers (`st18`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st18-cohort-reclaim-on-lender-arrival --namespace kueue-optimizer-chamber-st18 --port 16418`
- **Cross-Examination Assertion**: Both hit `200%` borrower nominal utilization before reclaim, emit `4` reclaims, and complete all 12 workloads.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `cq-borrower P90/P99=200%, Cohort ~85%–98%` (`st41`) vs. `Cohort 85%–100% throughout` (`st18`)
  - `Sec 5.2 Wait Time`: `cq-lender wait < 2.5s` (`st41`) vs. `cq-lender wait < 2.0s` (`st18`)
  - `Sec 5.3 Evictions`: `4 reclaims (4 unique)` (`st41`) vs. `4 reclaims (4 unique)` (`st18`)
  - `Sec 5.4 Effective Sat`: `~70%–88% (12 completed)` (`st41`) vs. `~72%–90%` (`st18`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 8-node (`32`-TPU) cluster, `cohort-t0-rec` (`cq-borrower`: `16` TPUs, `cq-lender`: `16` TPUs, `reclaimWithinCohort: Any`). At `t0`, `cq-borrower` runs 8 `Admitted` `JobSet`s (`32` TPUs = `16` nominal + `16` borrowed, `34s` remaining, `22s` restart). At `t=11s`, `cq-lender` submits 4 `JobSet`s (`16` TPUs, `18s`).
- **Expected Runtime Behavior**: During `0s–11s`, `cq-borrower` reports `200%` (`32/16` TPUs). At `t=11s`, Kueue evicts **exact 4** `init-bor-*` `t0` `PodGroup`s (`cq-borrower`'s own `16` nominal TPUs remain protected), admits all 4 `arr-lend-*` jobs in `< 2.5s`, dynamically resubmits the 4 reclaimed `init-bor-*` `JobSet`s (`[+4 preempted-t0 resubmit]`), and completes all 12 workloads.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `cq-borrower P90/P99=200%, Cohort ~85%–98%` |
| **Section `5.2` Queue Wait Time** | `cq-lender wait < 2.5s` |
| **Section `5.3` Evictions & Reclaims** | `4 reclaims (4 unique)` |
| **Section `5.4` Effective Saturation** | `~70%–88% (12 completed)` |

### Detailed Metric Checkpoints

- `Section 5.1`: `cq-borrower` hits **`200.0%`** during `0s–11s`, then `100.0%` alongside `cq-lender = 100.0%` during `11s–29s`.
- `Section 5.2`: `arr-lend-*` wait **`< 2.5s`**.
- `Section 5.3`: **`4`** evictions (`4` unique) reclaiming `cq-lender`'s nominal quota.
- `Section 5.4`: `Qualifying Workloads: 12 (completed=12 [12 pods])`, **`EFFECTIVE SAT ≈ 70%–88%`**.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ More than 4 `init-bor-*` `t0` `PodGroup`s evicted (`cq-borrower`'s 4 nominal slots are untouchable).
- ❌ `arr-lend-*` waiting `23s` until `t=34s` when `reclaimWithinCohort: Any`.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
