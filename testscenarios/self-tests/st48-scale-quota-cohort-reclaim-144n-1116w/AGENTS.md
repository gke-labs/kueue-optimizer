# Agent Execution & Cross-Examination Guide — `st48-scale-quota-cohort-reclaim-144n-1116w`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st48-scale-quota-cohort-reclaim-144n-1116w`
- **Category**: 10. Large-Scale Quota-Only Admission (>100 Nodes, >1,000 Workloads — No TAS)
- **Title**: 144-Node, 1,116-Workload Quota-Only Cohort Borrowing (`200%`) & Instant Reclamation
- **Primary Verification Target**: Verifies Cohort borrowing (`1,152/576 = 200%`) at `t0` across `144` nodes and exact `72`-workload (`576`-TPU) reclamation when `cq-scale-lender` arrives at `t=3s`
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st48` (`--namespace kueue-optimizer-chamber-st48`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16448` | Prometheus `19095` (collision-free across all 95 scenarios `st01`–`st95`)
- **Mandatory Paired Counterpart(s)**: [`st57-scale-tas-cohort-borrow-reclaim-128n-1088w`](../st57-scale-tas-cohort-borrow-reclaim-128n-1088w/AGENTS.md), [`st49-scale-quota-fair-sharing-160n-1160w`](../st49-scale-quota-fair-sharing-160n-1160w/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st95`) and non-overlapping port pair (`16401/19048` .. `16495/19142`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST48 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st48' (API port 16448, Prom port 19095):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st48-scale-quota-cohort-reclaim-144n-1116w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st48 --port 16448 > /tmp/st48.log 2>&1 &
PID_ST48=$!

# Simultaneously launch paired ST57 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st57' (API port 16457, Prom port 19104):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st57-scale-tas-cohort-borrow-reclaim-128n-1088w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st57 --port 16457 > /tmp/st57.log 2>&1 &
PID_ST57=$!

# Simultaneously launch paired ST49 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st49' (API port 16449, Prom port 19096):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st49-scale-quota-fair-sharing-160n-1160w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st49 --port 16449 > /tmp/st49.log 2>&1 &
PID_ST49=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST48" "$PID_ST57" "$PID_ST49"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST48 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st48' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st48-scale-quota-cohort-reclaim-144n-1116w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st48 --port 16448 | tee /tmp/st48.log

# 2. Run paired ST57 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st57':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st57-scale-tas-cohort-borrow-reclaim-128n-1088w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st57 --port 16457 | tee /tmp/st57.log

# 3. Run paired ST49 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st49':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st49-scale-quota-fair-sharing-160n-1160w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st49 --port 16449 | tee /tmp/st49.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st48-scale-quota-cohort-reclaim-144n-1116w --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st48`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st48-scale-quota-cohort-reclaim-144n-1116w` (`kueue-optimizer-chamber-st48`) ↔ [`st57-scale-tas-cohort-borrow-reclaim-128n-1088w`](../st57-scale-tas-cohort-borrow-reclaim-128n-1088w/AGENTS.md) (`kueue-optimizer-chamber-st57`) — Quota-Only (`144` Nodes) vs. TAS (`128` Nodes) Cohort Borrowing & Reclamation Pair

- **Controlled Variable Changed**: Quota-only Cohort borrowing (`1,152/576 = 200%`, `72` reclaims in `st48`) vs. TAS Cohort borrowing (`1,024/512 = 200%`, `64` reclaims in `st57`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st57-scale-tas-cohort-borrow-reclaim-128n-1088w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st57 --port 16457`
- **Cross-Examination Assertion**: Both hit `200.0%` borrower `Sec 5.1` at `t0` and reclaim exact `50%` of cluster TPUs (`72` vs. `64` `8`-TPU `t0` `PodGroup`s) within `< 5s` of lender arrival.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `cq-scale-borrower P90/P99=200%, Cohort ~75%–95%` (`st48`) vs. `cq-tas-borrower P90/P99=200%, Cohort ~75%–95%` (`st57`)
  - `Sec 5.2 Wait Time`: `t=3s lender burst waits < 5s` (`st48`) vs. `t=3s lender TAS burst waits < 5s` (`st57`)
  - `Sec 5.3 Evictions`: `72 Cohort reclaims (72 unique)` (`st48`) vs. `64 TAS Cohort reclaims (64 unique)` (`st57`)
  - `Sec 5.4 Effective Sat`: `~62%–90% (1,116 completed)` (`st48`) vs. `~62%–90% (1,088 completed)` (`st57`)

### Pair: `st48-scale-quota-cohort-reclaim-144n-1116w` (`kueue-optimizer-chamber-st48`) ↔ [`st49-scale-quota-fair-sharing-160n-1160w`](../st49-scale-quota-fair-sharing-160n-1160w/AGENTS.md) (`kueue-optimizer-chamber-st49`) — Nominal Cohort Reclamation (`st48`) vs. Zero-Nominal 4-CQ Fair Sharing (`st49`)

- **Controlled Variable Changed**: 2 CQs with `576` nominal TPUs each (`st48`) vs. 4 CQs with `0` nominal TPUs and `1:1:1:1` `fairSharing.weight` (`st49`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st49-scale-quota-fair-sharing-160n-1160w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st49 --port 16449`
- **Cross-Examination Assertion**: `st48` reclaims `72` `t0` `PodGroup`s (`50%` of `144`), whereas `st49` preempts `~120` `t0` `PodGroup`s (`75%` of `160`) via `InCohortFairSharing`.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `cq-scale-borrower P90/P99=200%, Cohort ~75%–95%` (`st48`) vs. `Cohort ~75%–95%, 4 CQs equalized ~320 TPUs` (`st49`)
  - `Sec 5.2 Wait Time`: `t=3s lender burst waits < 5s` (`st48`) vs. `q2..q4 burst waits < 6s` (`st49`)
  - `Sec 5.3 Evictions`: `72 Cohort reclaims (72 unique)` (`st48`) vs. `~120 InCohortFairSharing evictions` (`st49`)
  - `Sec 5.4 Effective Sat`: `~62%–90% (1,116 completed)` (`st48`) vs. `~60%–88% (1,160 completed)` (`st49`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 144 nodes (`1,152` TPUs, no TAS), `cohort-scale-quota` (`cq-scale-borrower`: `576` TPUs, `cq-scale-lender`: `576` TPUs, `reclaimWithinCohort: Any`). At `t0`, `cq-scale-borrower` runs `144` `8`-TPU `Job`s (`1,152` TPUs = `200%` of nominal). At `t=3s`, `cq-scale-lender` submits `72` `8`-TPU `Job`s (`576` TPUs), followed by `900` `4`-TPU `Job`s (`450` per CQ) across `t=10s..20s` (**`1,116` workloads**).
- **Expected Runtime Behavior**: `cq-scale-borrower` starts at `200%` (`1,152/576`); at `t=3s` Kueue reclaims **exact `72`** `t0` `PodGroup`s so `cq-scale-borrower` and `cq-scale-lender` run side-by-side at `576` TPUs (`100%` each), dynamically resubmitting the `72` reclaimed `Job`s and completing all `1,116` workloads.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `cq-scale-borrower P90/P99=200%, Cohort ~75%–95%` |
| **Section `5.2` Queue Wait Time** | `t=3s lender burst waits < 5s` |
| **Section `5.3` Evictions & Reclaims** | `72 Cohort reclaims (72 unique)` |
| **Section `5.4` Effective Saturation** | `~62%–90% (1,116 completed)` |

### Detailed Metric Checkpoints

- `Section 5.1`: `cq-scale-borrower` hits **`200.0%`** before `t=3s`, then settles to `~100%` alongside `cq-scale-lender`.
- `Section 5.3`: **`72`** Cohort reclamation evictions (`72` unique).
- `Section 5.4`: `Qualifying Workloads: 1116 (completed=1116)`, `Excluded: 72`.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ More than `72` `t0` borrower `PodGroup`s reclaimed (`cq-scale-borrower`'s `576` nominal TPUs are untouchable).
- ❌ `cq-scale-lender` waiting `25s` until `t=28s` for `t0` borrowers to drain naturally.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
