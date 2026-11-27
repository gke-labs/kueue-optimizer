# Agent Execution & Cross-Examination Guide — `st59-scale-tas-borrowing-limit-128n-1048w`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st59-scale-tas-borrowing-limit-128n-1048w`
- **Category**: 12. Large-Scale TAS Preemption, Reclaim, Fair Sharing & Hybrid (>100 Nodes, >1,000 Workloads)
- **Title**: 128-Node, 1,048-Workload TAS `borrowingLimit: 256` & `lendingLimit: 256` (`150.0%` Ceiling)
- **Primary Verification Target**: Verifies `cq-tas-capped` (`512` nominal + `256` `borrowingLimit`) never exceeds **`768` TPUs (`150.0%`)** across `128` TAS nodes even with `64` `t0` TAS jobs (`512` TPUs) + `984` queued TAS arrivals
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st59` (`--namespace kueue-optimizer-chamber-st59`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16459` | Prometheus `19106` (collision-free across all 95 scenarios `st01`–`st95`)
- **Mandatory Paired Counterpart(s)**: [`st57-scale-tas-cohort-borrow-reclaim-128n-1088w`](../st57-scale-tas-cohort-borrow-reclaim-128n-1088w/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st95`) and non-overlapping port pair (`16401/19048` .. `16495/19142`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST59 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st59' (API port 16459, Prom port 19106):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st59-scale-tas-borrowing-limit-128n-1048w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st59 --port 16459 > /tmp/st59.log 2>&1 &
PID_ST59=$!

# Simultaneously launch paired ST57 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st57' (API port 16457, Prom port 19104):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st57-scale-tas-cohort-borrow-reclaim-128n-1088w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st57 --port 16457 > /tmp/st57.log 2>&1 &
PID_ST57=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST59" "$PID_ST57"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST59 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st59' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st59-scale-tas-borrowing-limit-128n-1048w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st59 --port 16459 | tee /tmp/st59.log

# 2. Run paired ST57 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st57':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st57-scale-tas-cohort-borrow-reclaim-128n-1088w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st57 --port 16457 | tee /tmp/st57.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st59-scale-tas-borrowing-limit-128n-1048w --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st59`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st59-scale-tas-borrowing-limit-128n-1048w` (`kueue-optimizer-chamber-st59`) ↔ [`st57-scale-tas-cohort-borrow-reclaim-128n-1088w`](../st57-scale-tas-cohort-borrow-reclaim-128n-1088w/AGENTS.md) (`kueue-optimizer-chamber-st57`) — TAS `borrowingLimit` `150%` Ceiling (`st59`) vs. `200%` Full Cohort Borrowing (`st57`)

- **Controlled Variable Changed**: `borrowingLimit: 256` (`st59`) vs. unlimited Cohort borrowing (`st57`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st57-scale-tas-cohort-borrow-reclaim-128n-1088w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st57 --port 16457`
- **Cross-Examination Assertion**: `cq-tas-capped` `Sec 5.1 P90/P99` stays `<= 150.0%` (`768/512`) and `Sec 5.4 <= 75.0%` in `st59`, vs. `200.0%` peak in `st57`.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `cq-tas-capped P90/P99 = 150.0% (768/512 TPUs)` (`st59`) vs. `cq-tas-borrower P90/P99=200%, Cohort ~75%–95%` (`st57`)
  - `Sec 5.2 Wait Time`: `Capped at 768 TPUs (32 nodes idle)` (`st59`) vs. `t=3s lender TAS burst waits < 5s` (`st57`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st59`) vs. `64 TAS Cohort reclaims (64 unique)` (`st57`)
  - `Sec 5.4 Effective Sat`: `~50%–75% (<= 75% physical cap, 1,048 completed)` (`st59`) vs. `~62%–90% (1,088 completed)` (`st57`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 128 nodes (`32` racks, `1,024` TPUs) with TAS, `cohort-tas-limit`: `cq-tas-capped` (`nominalQuota=512, borrowingLimit=256`) + `cq-tas-donor` (`nominalQuota=512, lendingLimit=256`, idle). At `t0`, `cq-tas-capped` runs `64` `8`-TPU TAS jobs (`512` TPUs = `100%` nominal, `14s` remaining). `984` `4`-TPU TAS jobs (`8s`) arrive on `cq-tas-capped` across `t=1s..17s` (**`1,048` workloads**).
- **Expected Runtime Behavior**: `cq-tas-capped` borrows **at most `256` TPUs** on top of its `512`-TPU `t0` load (`768 / 512 = 150.0%`, leaving `256` TPUs = `32` full nodes idle throughout the run!) and drains all `1,048` workloads cleanly.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `cq-tas-capped P90/P99 = 150.0% (768/512 TPUs)` |
| **Section `5.2` Queue Wait Time** | `Capped at 768 TPUs (32 nodes idle)` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `~50%–75% (<= 75% physical cap, 1,048 completed)` |

### Detailed Metric Checkpoints

- `Section 5.1 (cq-tas-capped)`: `P90 / P99 = 150.0%` (`768 / 512` TPUs) and NEVER `> 150.0%`.
- `Section 5.4`: Cluster `EFFECTIVE SAT <= 75.0%` (`768 / 1,024` physical TPUs ceiling), `1,048` completed.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ `cq-tas-capped` exceeding `150.0%` (`768` TPUs) at `t=1s` (which would occur if `t0` TAS `PodGroup`s were missing from CQ quota accounting).

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
