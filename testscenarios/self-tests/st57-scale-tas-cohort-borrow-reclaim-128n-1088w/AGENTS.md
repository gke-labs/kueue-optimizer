# Agent Execution & Cross-Examination Guide — `st57-scale-tas-cohort-borrow-reclaim-128n-1088w`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st57-scale-tas-cohort-borrow-reclaim-128n-1088w`
- **Category**: 12. Large-Scale TAS Preemption, Reclaim, Fair Sharing & Hybrid (>100 Nodes, >1,000 Workloads)
- **Title**: 128-Node, 1,088-Workload TAS Cohort Borrowing (`200%`) & Reclamation (`reclaimWithinCohort: Any`)
- **Primary Verification Target**: TAS counterpart to `st48`: verifies `cq-tas-lender` reclaims **exact `64`** `t0` TAS `PodGroup`s (`512` TPUs) from `200%`-borrowing `cq-tas-borrower` at `t=3s`
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st57` (`--namespace kueue-optimizer-chamber-st57`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16457` | Prometheus `19104` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st59-scale-tas-borrowing-limit-128n-1048w`](../st59-scale-tas-borrowing-limit-128n-1048w/AGENTS.md), [`st48-scale-quota-cohort-reclaim-144n-1116w`](../st48-scale-quota-cohort-reclaim-144n-1116w/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST57 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st57' (API port 16457, Prom port 19104):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st57-scale-tas-cohort-borrow-reclaim-128n-1088w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st57 --port 16457 > /tmp/st57.log 2>&1 &
PID_ST57=$!

# Simultaneously launch paired ST59 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st59' (API port 16459, Prom port 19106):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st59-scale-tas-borrowing-limit-128n-1048w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st59 --port 16459 > /tmp/st59.log 2>&1 &
PID_ST59=$!

# Simultaneously launch paired ST48 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st48' (API port 16448, Prom port 19095):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st48-scale-quota-cohort-reclaim-144n-1116w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st48 --port 16448 > /tmp/st48.log 2>&1 &
PID_ST48=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST57" "$PID_ST59" "$PID_ST48"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST57 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st57' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st57-scale-tas-cohort-borrow-reclaim-128n-1088w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st57 --port 16457 | tee /tmp/st57.log

# 2. Run paired ST59 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st59':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st59-scale-tas-borrowing-limit-128n-1048w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st59 --port 16459 | tee /tmp/st59.log

# 3. Run paired ST48 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st48':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st48-scale-quota-cohort-reclaim-144n-1116w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st48 --port 16448 | tee /tmp/st48.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st57-scale-tas-cohort-borrow-reclaim-128n-1088w --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st57`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st57-scale-tas-cohort-borrow-reclaim-128n-1088w` (`kueue-optimizer-chamber-st57`) ↔ [`st59-scale-tas-borrowing-limit-128n-1048w`](../st59-scale-tas-borrowing-limit-128n-1048w/AGENTS.md) (`kueue-optimizer-chamber-st59`) — Unbounded TAS Cohort Borrowing (`200%`, `st57`) vs. Capped TAS `borrowingLimit: 256` (`150%`, `st59`)

- **Controlled Variable Changed**: Unbounded Cohort borrowing + lender reclaim (`st57`) vs. `borrowingLimit: 256` / `lendingLimit: 256` with idle donor (`st59`) on `128` TAS nodes.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st59-scale-tas-borrowing-limit-128n-1048w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st59 --port 16459`
- **Cross-Examination Assertion**: Borrower `Sec 5.1` hits `200.0%` (`1,024/512`) in `st57` with `64` reclaims, whereas `cq-tas-capped` in `st59` is strictly capped at `150.0%` (`768/512`) with `0` evictions and `32` idle nodes.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `cq-tas-borrower P90/P99=200%, Cohort ~75%–95%` (`st57`) vs. `cq-tas-capped P90/P99 = 150.0% (768/512 TPUs)` (`st59`)
  - `Sec 5.2 Wait Time`: `t=3s lender TAS burst waits < 5s` (`st57`) vs. `Capped at 768 TPUs (32 nodes idle)` (`st59`)
  - `Sec 5.3 Evictions`: `64 TAS Cohort reclaims (64 unique)` (`st57`) vs. `0 evictions` (`st59`)
  - `Sec 5.4 Effective Sat`: `~62%–90% (1,088 completed)` (`st57`) vs. `~50%–75% (<= 75% physical cap, 1,048 completed)` (`st59`)

### Pair: `st57-scale-tas-cohort-borrow-reclaim-128n-1088w` (`kueue-optimizer-chamber-st57`) ↔ [`st48-scale-quota-cohort-reclaim-144n-1116w`](../st48-scale-quota-cohort-reclaim-144n-1116w/AGENTS.md) (`kueue-optimizer-chamber-st48`) — TAS (`st57`) vs. Quota-Only (`st48`) Cohort Borrowing & Reclamation Pair

- **Controlled Variable Changed**: 7-level TAS (`st57`) vs. quota-only (`st48`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st48-scale-quota-cohort-reclaim-144n-1116w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st48 --port 16448`
- **Cross-Examination Assertion**: Both reclaim exact `50%` of cluster TPUs from `200%`-borrowing `t0` `PodGroup`s upon lender arrival.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `cq-tas-borrower P90/P99=200%, Cohort ~75%–95%` (`st57`) vs. `cq-scale-borrower P90/P99=200%, Cohort ~75%–95%` (`st48`)
  - `Sec 5.2 Wait Time`: `t=3s lender TAS burst waits < 5s` (`st57`) vs. `t=3s lender burst waits < 5s` (`st48`)
  - `Sec 5.3 Evictions`: `64 TAS Cohort reclaims (64 unique)` (`st57`) vs. `72 Cohort reclaims (72 unique)` (`st48`)
  - `Sec 5.4 Effective Sat`: `~62%–90% (1,088 completed)` (`st57`) vs. `~62%–90% (1,116 completed)` (`st48`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 128 nodes (`32` racks, `1,024` TPUs) with TAS, `cohort-tas-reclaim` (`512` + `512` TPUs, `reclaimWithinCohort: Any`). At `t0`, `cq-tas-borrower` runs `128` `8`-TPU TAS jobs (`1,024/512 = 200%`, `28s` remaining). At `t=3s`, `cq-tas-lender` submits `64` `8`-TPU TAS jobs (`512` TPUs), followed by `896` `4`-TPU TAS jobs (`448` per CQ) across `t=10s..20s` (**`1,088` workloads**).
- **Expected Runtime Behavior**: At `t=3s`, Kueue reclaims **exact `64`** `t0` TAS `PodGroup`s (`512` TPUs), admits `cq-tas-lender`'s `64` TAS jobs onto the freed nodes, dynamically resubmits the `64` reclaimed TAS jobs (`[+64 preempted-t0 resubmit]`), and completes all `1,088` workloads.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `cq-tas-borrower P90/P99=200%, Cohort ~75%–95%` |
| **Section `5.2` Queue Wait Time** | `t=3s lender TAS burst waits < 5s` |
| **Section `5.3` Evictions & Reclaims** | `64 TAS Cohort reclaims (64 unique)` |
| **Section `5.4` Effective Saturation** | `~62%–90% (1,088 completed)` |

### Detailed Metric Checkpoints

- `Section 5.1`: `cq-tas-borrower` hits **`200.0%`** during `0s–3s`, then `100.0%` alongside `cq-tas-lender = 100.0%`.
- `Section 5.3`: **`64`** Cohort TAS reclaims (`64` unique).
- `Section 5.4`: `Qualifying Workloads: 1088 (completed=1088)`, `Excluded: 64`.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ More than `64` `t0` TAS borrower `PodGroup`s reclaimed.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
