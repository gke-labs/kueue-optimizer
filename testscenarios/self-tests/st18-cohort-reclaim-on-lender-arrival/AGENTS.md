# Agent Execution & Cross-Examination Guide — `st18-cohort-reclaim-on-lender-arrival`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st18-cohort-reclaim-on-lender-arrival`
- **Category**: 4. Multi-CQ & Cohort Borrowing/Reclaim
- **Title**: Cohort Quota Reclaim When Lending Queue Submits Work (`reclaimWithinCohort: Any`)
- **Primary Verification Target**: Cross-CQ quota reclaim eviction (`4` borrowed jobs evicted at `t=9s` so `cq-lender` immediately reclaims its `16` nominal TPUs)
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st18` (`--namespace kueue-optimizer-chamber-st18`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16418` | Prometheus `19065` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st19-cohort-reclaim-disabled-lender-waits`](../st19-cohort-reclaim-disabled-lender-waits/AGENTS.md), [`st41-t0-cohort-borrowing-reclaim-any`](../st41-t0-cohort-borrowing-reclaim-any/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST18 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st18' (API port 16418, Prom port 19065):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st18-cohort-reclaim-on-lender-arrival --namespace kueue-optimizer-chamber-st18 --port 16418 > /tmp/st18.log 2>&1 &
PID_ST18=$!

# Simultaneously launch paired ST19 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st19' (API port 16419, Prom port 19066):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st19-cohort-reclaim-disabled-lender-waits --namespace kueue-optimizer-chamber-st19 --port 16419 > /tmp/st19.log 2>&1 &
PID_ST19=$!

# Simultaneously launch paired ST41 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st41' (API port 16441, Prom port 19088):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st41-t0-cohort-borrowing-reclaim-any --no-skip-initial-offset --namespace kueue-optimizer-chamber-st41 --port 16441 > /tmp/st41.log 2>&1 &
PID_ST41=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST18" "$PID_ST19" "$PID_ST41"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST18 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st18' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st18-cohort-reclaim-on-lender-arrival --namespace kueue-optimizer-chamber-st18 --port 16418 | tee /tmp/st18.log

# 2. Run paired ST19 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st19':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st19-cohort-reclaim-disabled-lender-waits --namespace kueue-optimizer-chamber-st19 --port 16419 | tee /tmp/st19.log

# 3. Run paired ST41 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st41':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st41-t0-cohort-borrowing-reclaim-any --no-skip-initial-offset --namespace kueue-optimizer-chamber-st41 --port 16441 | tee /tmp/st41.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st18-cohort-reclaim-on-lender-arrival --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st18`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st18-cohort-reclaim-on-lender-arrival` (`kueue-optimizer-chamber-st18`) ↔ [`st19-cohort-reclaim-disabled-lender-waits`](../st19-cohort-reclaim-disabled-lender-waits/AGENTS.md) (`kueue-optimizer-chamber-st19`) — Exact A/B Twin (`reclaimWithinCohort: Any` vs. `reclaimWithinCohort: Never`)

- **Controlled Variable Changed**: ONLY `reclaimWithinCohort` changes (`Any` in `st18` vs. `Never` in `st19`); all 8 borrower jobs (`t=0s`) and 4 lender jobs (`t=9s`) are 100% identical.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st19-cohort-reclaim-disabled-lender-waits --namespace kueue-optimizer-chamber-st19 --port 16419`
- **Cross-Examination Assertion**: `Sec 5.3 Evictions` flips from `4` (`st18`, evicting only the 4 borrowed slots while protecting `cq-borrower`'s own 4 nominal slots) to `0` (`st19`), and `cq-lender` wait time jumps from `< 2.0s` (`st18`) to `~21s` (`st19`).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `Cohort 85%–100% throughout` (`st18`) vs. `~77%–82% across 50s makespan` (`st19`)
  - `Sec 5.2 Wait Time`: `cq-lender wait < 2.0s` (`st18`) vs. `cq-lender wait ~21s` (`st19`)
  - `Sec 5.3 Evictions`: `4 reclaims (4 unique)` (`st18`) vs. `0 evictions` (`st19`)
  - `Sec 5.4 Effective Sat`: `~72%–90%` (`st18`) vs. `~77%–82% (Excluded: 0)` (`st19`)

### Pair: `st18-cohort-reclaim-on-lender-arrival` (`kueue-optimizer-chamber-st18`) ↔ [`st41-t0-cohort-borrowing-reclaim-any`](../st41-t0-cohort-borrowing-reclaim-any/AGENTS.md) (`kueue-optimizer-chamber-st41`) — Scheduled Borrower Reclaim (`st18`) vs. Bootstrapped `t0` Borrower Reclaim (`st41`) Pair

- **Controlled Variable Changed**: `st18` admits borrower jobs at `t=0s`, whereas `st41` bootstraps all 8 borrower jobs as pre-admitted `t0` `PodGroup`s (`200%` quota at startup).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st41-t0-cohort-borrowing-reclaim-any --no-skip-initial-offset --namespace kueue-optimizer-chamber-st41 --port 16441`
- **Cross-Examination Assertion**: Both scenarios evict **exact 4** borrower jobs when `cq-lender` arrives, admit `cq-lender` in `< 2.5s`, and complete all 12 workloads.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `Cohort 85%–100% throughout` (`st18`) vs. `cq-borrower P90/P99=200%, Cohort ~85%–98%` (`st41`)
  - `Sec 5.2 Wait Time`: `cq-lender wait < 2.0s` (`st18`) vs. `cq-lender wait < 2.5s` (`st41`)
  - `Sec 5.3 Evictions`: `4 reclaims (4 unique)` (`st18`) vs. `4 reclaims (4 unique)` (`st41`)
  - `Sec 5.4 Effective Sat`: `~72%–90%` (`st18`) vs. `~70%–88% (12 completed)` (`st41`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 8-node (`32`-TPU) cluster, `cohort-reclaim` (`cq-borrower`: `16` TPUs, `cq-lender`: `16` TPUs, `reclaimWithinCohort: Any`). At `t=0s`, `cq-borrower` starts 8 jobs (`32` TPUs, `35s`). At `t=9s`, `cq-lender` submits 4 jobs (`16` TPUs, `20s`).
- **Expected Runtime Behavior**: At `t=9s`, Kueue sees `cq-borrower` is over its `16`-TPU nominal quota (`using 32`) while `cq-lender` is under its `16`-TPU nominal quota (`using 0`). Kueue immediately evicts **exactly 4** of `cq-borrower`'s jobs (`cq-borrower`'s own `16`-TPU nominal quota is untouchable), admits all 4 `cq-lender` jobs within `<2s`, and re-admits the 4 evicted `cq-borrower` jobs as soon as capacity frees at `t=29s`.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `Cohort 85%–100% throughout` |
| **Section `5.2` Queue Wait Time** | `cq-lender wait < 2.0s` |
| **Section `5.3` Evictions & Reclaims** | `4 reclaims (4 unique)` |
| **Section `5.4` Effective Saturation** | `~72%–90%` |

### Detailed Metric Checkpoints

- `Section 5.3 (Total Evictions and Reclaims)`: **`4`** evictions (`Unique workloads evicted once = 4`).
- `Section 5.1`: From `0s–9s`, `cq-borrower = 200%, cq-lender = 0%`; from `9s–29s`, `cq-borrower = 100% (16/16), cq-lender = 100% (16/16)`.
- `Section 5.2`: `cq-lender` jobs wait **`< 2.0s`**.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ More than 4 `cq-borrower` workloads evicted (`cq-lender` may only reclaim its own `16` nominal TPUs, never invade `cq-borrower`'s `16` nominal TPUs).
- ❌ `cq-lender` jobs waiting until `t=35s` when `reclaimWithinCohort: Any`.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
