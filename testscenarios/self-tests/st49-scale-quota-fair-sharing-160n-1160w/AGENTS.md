# Agent Execution & Cross-Examination Guide — `st49-scale-quota-fair-sharing-160n-1160w`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st49-scale-quota-fair-sharing-160n-1160w`
- **Category**: 10. Large-Scale Quota-Only Admission (>100 Nodes, >1,000 Workloads — No TAS)
- **Title**: 160-Node, 1,160-Workload 4-CQ Preemption-Based Fair Sharing Against `t0` Monopolizer
- **Primary Verification Target**: Verifies `InCohortFairSharing` across `160` nodes (`1,280` TPUs) when `cq-fs-q1` holds `100%` of the cohort at `t0` (`160` jobs) and 3 equal-weight peer CQs arrive at `t=3s`
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st49` (`--namespace kueue-optimizer-chamber-st49`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16449` | Prometheus `19096` (collision-free across all 95 scenarios `st01`–`st95`)
- **Mandatory Paired Counterpart(s)**: [`st58-scale-tas-fair-sharing-160n-1140w`](../st58-scale-tas-fair-sharing-160n-1140w/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st95`) and non-overlapping port pair (`16401/19048` .. `16495/19142`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST49 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st49' (API port 16449, Prom port 19096):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st49-scale-quota-fair-sharing-160n-1160w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st49 --port 16449 > /tmp/st49.log 2>&1 &
PID_ST49=$!

# Simultaneously launch paired ST58 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st58' (API port 16458, Prom port 19105):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st58-scale-tas-fair-sharing-160n-1140w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st58 --port 16458 > /tmp/st58.log 2>&1 &
PID_ST58=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST49" "$PID_ST58"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST49 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st49' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st49-scale-quota-fair-sharing-160n-1160w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st49 --port 16449 | tee /tmp/st49.log

# 2. Run paired ST58 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st58':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st58-scale-tas-fair-sharing-160n-1140w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st58 --port 16458 | tee /tmp/st58.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st49-scale-quota-fair-sharing-160n-1160w --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st49`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st49-scale-quota-fair-sharing-160n-1160w` (`kueue-optimizer-chamber-st49`) ↔ [`st58-scale-tas-fair-sharing-160n-1140w`](../st58-scale-tas-fair-sharing-160n-1140w/AGENTS.md) (`kueue-optimizer-chamber-st58`) — 160-Node Quota-Only vs. TAS 4-CQ Fair Sharing Pair

- **Controlled Variable Changed**: Quota-only 4-CQ Fair Sharing (`st49`, `1,160` workloads) vs. 7-level TAS 4-CQ Fair Sharing (`st58`, `1,140` workloads) on `160` nodes (`1,280` TPUs).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st58-scale-tas-fair-sharing-160n-1140w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st58 --port 16458`
- **Cross-Examination Assertion**: Both preempt `~120` of `160` `t0` monopolizer `PodGroup`s (`InCohortFairSharing`) to equalize all 4 CQs around `320` TPUs (`25%` each).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `Cohort ~75%–95%, 4 CQs equalized ~320 TPUs` (`st49`) vs. `Cohort ~75%–95%, 4 TAS CQs equalized ~320 TPUs` (`st58`)
  - `Sec 5.2 Wait Time`: `q2..q4 burst waits < 6s` (`st49`) vs. `fs2..fs4 burst waits < 6s` (`st58`)
  - `Sec 5.3 Evictions`: `~120 InCohortFairSharing evictions` (`st49`) vs. `~120 InCohortFairSharing TAS evictions` (`st58`)
  - `Sec 5.4 Effective Sat`: `~60%–88% (1,160 completed)` (`st49`) vs. `~60%–88% (1,140 completed)` (`st58`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 160 nodes (`1,280` TPUs, no TAS), `cohort-scale-fs` (`1,280` TPUs) shared by `cq-fs-q1..q4` (`nominalQuota=0`, `fairSharing.weight=1`, `reclaimWithinCohort: Any`). At `t0`, `cq-fs-q1` monopolizes all `1,280` TPUs (`160` `8`-TPU `Job`s, `28s` remaining). At `t=3s`, `cq-fs-q2..q4` each submit `40` `8`-TPU `Job`s (`320` TPUs each), followed by `880` `4`-TPU `Job`s (`220` per CQ) across `t=11s..21s` (**`1,160` workloads**).
- **Expected Runtime Behavior**: At `t=3s`, Fair Sharing preempts `120` of `cq-fs-q1`'s `160` `t0` `PodGroup`s (`InCohortFairSharing`), equalizing all 4 CQs at `320` TPUs (`25%` each), dynamically resubmits the `120` preempted `Job`s, and completes all `1,160` workloads.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `Cohort ~75%–95%, 4 CQs equalized ~320 TPUs` |
| **Section `5.2` Queue Wait Time** | `q2..q4 burst waits < 6s` |
| **Section `5.3` Evictions & Reclaims** | `~120 InCohortFairSharing evictions` |
| **Section `5.4` Effective Saturation** | `~60%–88% (1,160 completed)` |

### Detailed Metric Checkpoints

- `Section 5.1`: Cohort `cohort-scale-fs` maintains high saturation while `cq-fs-q1..q4` equalize around `320` TPUs each during contention.
- `Section 5.3`: **`~120`** `InCohortFairSharing` evictions against `cq-fs-q1`.
- `Section 5.4`: `Qualifying Workloads: 1160 (completed=1160)`.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ `0` evictions at `t=3s` while `cq-fs-q1` monopolizes `100%` of the `1,280`-TPU cohort.
- ❌ Evicting more than `120` `t0` `PodGroup`s below `cq-fs-q1`'s `320`-TPU (`25%`) fair share.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
