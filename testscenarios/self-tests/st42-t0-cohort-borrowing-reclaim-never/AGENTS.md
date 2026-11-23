# Agent Execution & Cross-Examination Guide — `st42-t0-cohort-borrowing-reclaim-never`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st42-t0-cohort-borrowing-reclaim-never`
- **Category**: 9. Initial t0 Workloads — Reclaims, Limits & Fair Sharing
- **Title**: Non-Preemptive `t0` Cohort Borrowing (`reclaimWithinCohort: Never` — Twin of st41)
- **Primary Verification Target**: Direct A/B twin of `st41` with `reclaimWithinCohort: Never`: `t0` borrower holds `200%` quota uninterrupted while `cq-lender` waits `~19s`
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st42` (`--namespace kueue-optimizer-chamber-st42`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16442` | Prometheus `19089` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st41-t0-cohort-borrowing-reclaim-any`](../st41-t0-cohort-borrowing-reclaim-any/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST42 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st42' (API port 16442, Prom port 19089):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st42-t0-cohort-borrowing-reclaim-never --no-skip-initial-offset --namespace kueue-optimizer-chamber-st42 --port 16442 > /tmp/st42.log 2>&1 &
PID_ST42=$!

# Simultaneously launch paired ST41 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st41' (API port 16441, Prom port 19088):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st41-t0-cohort-borrowing-reclaim-any --no-skip-initial-offset --namespace kueue-optimizer-chamber-st41 --port 16441 > /tmp/st41.log 2>&1 &
PID_ST41=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST42" "$PID_ST41"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST42 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st42' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st42-t0-cohort-borrowing-reclaim-never --no-skip-initial-offset --namespace kueue-optimizer-chamber-st42 --port 16442 | tee /tmp/st42.log

# 2. Run paired ST41 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st41':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st41-t0-cohort-borrowing-reclaim-any --no-skip-initial-offset --namespace kueue-optimizer-chamber-st41 --port 16441 | tee /tmp/st41.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st42-t0-cohort-borrowing-reclaim-never --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st42`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st42-t0-cohort-borrowing-reclaim-never` (`kueue-optimizer-chamber-st42`) ↔ [`st41-t0-cohort-borrowing-reclaim-any`](../st41-t0-cohort-borrowing-reclaim-any/AGENTS.md) (`kueue-optimizer-chamber-st41`) — Exact A/B Twin (`t0` Non-Preemptive Borrowing `st42` vs. Instant Cohort Reclaim `st41`)

- **Controlled Variable Changed**: ONLY `reclaimWithinCohort` changes (`Never` in `st42` vs. `Any` in `st41`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st41-t0-cohort-borrowing-reclaim-any --no-skip-initial-offset --namespace kueue-optimizer-chamber-st41 --port 16441`
- **Cross-Examination Assertion**: `Sec 5.3` is strictly `0` in `st42` vs. `4` in `st41`, and `cq-lender` waits `~19s` (`st42`) vs. `< 2.5s` (`st41`).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `cq-borrower 200% in 0s–30s, Cohort ~78%` (`st42`) vs. `cq-borrower P90/P99=200%, Cohort ~85%–98%` (`st41`)
  - `Sec 5.2 Wait Time`: `cq-lender wait ~18s–20s` (`st42`) vs. `cq-lender wait < 2.5s` (`st41`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st42`) vs. `4 reclaims (4 unique)` (`st41`)
  - `Sec 5.4 Effective Sat`: `~74%–82% (Excluded: 0)` (`st42`) vs. `~70%–88% (12 completed)` (`st41`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: Identical cohort quotas (`16` + `16` TPUs), 8 `t0` borrower jobs (`30s`), and 4 `cq-lender` arrivals at `t=11s` (`18s`) as `st41`, changing ONLY `reclaimWithinCohort` from `Any` to `Never`.
- **Expected Runtime Behavior**: Because `cq-borrower`'s `32`-TPU `t0` usage consumes the entire Cohort and `reclaimWithinCohort: Never` forbids eviction, all 8 `init-bor-*` jobs finish naturally at `t=30s` (`0` evictions), and `arr-lend-*` wait `~19s` (`t=11s -> t=30s`) before running `30s–48s`.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `cq-borrower 200% in 0s–30s, Cohort ~78%` |
| **Section `5.2` Queue Wait Time** | `cq-lender wait ~18s–20s` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `~74%–82% (Excluded: 0)` |

### Detailed Metric Checkpoints

- `Section 5.2`: `arr-lend-*` wait **`~16s–21s`** (vs `< 2.5s` in `st41`).
- `Section 5.3`: Strictly **`0`** evictions (vs `4` in `st41`).
- `Section 5.4`: `Qualifying Workloads: 12 (completed=12 [12 pods])`, `Excluded: 0`, **`EFFECTIVE SAT ≈ 74%–82%`**.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ Any eviction (`Section 5.3 > 0`) when `reclaimWithinCohort: Never`.
- ❌ `arr-lend-*` admitting at `t=11s` (which would mean `cq-borrower`'s `t0` borrowed quota was not tracked in the Cohort).

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
