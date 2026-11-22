# Agent Execution & Cross-Examination Guide — `st58-scale-tas-fair-sharing-160n-1140w`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and re-run:
> ```bash
> python3 testscenarios/self-tests/generate_self_test_suite.py
> python3 deploy/scripts/generate_scenario_readme.py --all
> python3 testscenarios/self-tests/generate_self_test_suite.py --check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st58-scale-tas-fair-sharing-160n-1140w`
- **Category**: 12. Large-Scale TAS Preemption, Reclaim, Fair Sharing & Hybrid (>100 Nodes, >1,000 Workloads)
- **Title**: 160-Node, 1,140-Workload 4-CQ TAS Preemption-Based Fair Sharing
- **Primary Verification Target**: TAS counterpart to `st49`: verifies `InCohortFairSharing` + TAS domain eviction & reassignment across `160` nodes (`40` racks, `1,280` TPUs) and `1,140` TAS workloads
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st58` (`--namespace kueue-optimizer-chamber-st58`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16458` | Prometheus `19105` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st49-scale-quota-fair-sharing-160n-1160w`](../st49-scale-quota-fair-sharing-160n-1160w/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST58 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st58' (API port 16458, Prom port 19105):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st58-scale-tas-fair-sharing-160n-1140w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st58 --port 16458 > /tmp/st58.log 2>&1 &
PID_ST58=$!

# Simultaneously launch paired ST49 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st49' (API port 16449, Prom port 19096):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st49-scale-quota-fair-sharing-160n-1160w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st49 --port 16449 > /tmp/st49.log 2>&1 &
PID_ST49=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST58" "$PID_ST49"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST58 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st58' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st58-scale-tas-fair-sharing-160n-1140w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st58 --port 16458 | tee /tmp/st58.log

# 2. Run paired ST49 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st49':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st49-scale-quota-fair-sharing-160n-1160w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st49 --port 16449 | tee /tmp/st49.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st58-scale-tas-fair-sharing-160n-1140w --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st58`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st58-scale-tas-fair-sharing-160n-1140w` (`kueue-optimizer-chamber-st58`) ↔ [`st49-scale-quota-fair-sharing-160n-1160w`](../st49-scale-quota-fair-sharing-160n-1160w/AGENTS.md) (`kueue-optimizer-chamber-st49`) — 160-Node TAS (`st58`) vs. Quota-Only (`st49`) 4-CQ Fair Sharing Pair

- **Controlled Variable Changed**: 7-level TAS (`st58`, `1,140` workloads) vs. quota-only (`st49`, `1,160` workloads) on `160` nodes (`1,280` TPUs).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st49-scale-quota-fair-sharing-160n-1160w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st49 --port 16449`
- **Cross-Examination Assertion**: Both trigger `~120` `InCohortFairSharing` evictions against `t0` monopolizer `PodGroup`s so all 4 CQs converge to `320` TPUs (`25%` each).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `Cohort ~75%–95%, 4 TAS CQs equalized ~320 TPUs` (`st58`) vs. `Cohort ~75%–95%, 4 CQs equalized ~320 TPUs` (`st49`)
  - `Sec 5.2 Wait Time`: `fs2..fs4 burst waits < 6s` (`st58`) vs. `q2..q4 burst waits < 6s` (`st49`)
  - `Sec 5.3 Evictions`: `~120 InCohortFairSharing TAS evictions` (`st58`) vs. `~120 InCohortFairSharing evictions` (`st49`)
  - `Sec 5.4 Effective Sat`: `~60%–88% (1,140 completed)` (`st58`) vs. `~60%–88% (1,160 completed)` (`st49`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 160 nodes (`40` racks, `1,280` TPUs) with TAS, `cohort-tas-fs` (`1,280` TPUs) shared by `cq-tas-fs1..4` (`weight=1`, `reclaimWithinCohort: Any`). At `t0`, `cq-tas-fs1` occupies `100%` of the cohort (`160` `8`-TPU TAS jobs, `28s` remaining). At `t=3s`, `cq-tas-fs2..4` each submit `40` `8`-TPU TAS jobs (`320` TPUs each), followed by `860` `4`-TPU TAS jobs across `t=11s..21s` (**`1,140` workloads**).
- **Expected Runtime Behavior**: Fair Sharing preempts `120` of `cq-tas-fs1`'s `160` `t0` TAS `PodGroup`s at `t=3s`, frees their TAS node domains for `cq-tas-fs2..4` (`320` TPUs each), dynamically resubmits the `120` victims, and drains all `1,140` TAS workloads.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `Cohort ~75%–95%, 4 TAS CQs equalized ~320 TPUs` |
| **Section `5.2` Queue Wait Time** | `fs2..fs4 burst waits < 6s` |
| **Section `5.3` Evictions & Reclaims** | `~120 InCohortFairSharing TAS evictions` |
| **Section `5.4` Effective Saturation** | `~60%–88% (1,140 completed)` |

### Detailed Metric Checkpoints

- `Section 5.3`: **`~120`** `InCohortFairSharing` TAS evictions.
- `Section 5.4`: `Qualifying Workloads: 1140 (completed=1140)`.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ TAS topology cache retaining stale `t0` victim domain reservations after Fair-Sharing eviction.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
python3 testscenarios/self-tests/generate_self_test_suite.py
python3 deploy/scripts/generate_scenario_readme.py --all
python3 testscenarios/self-tests/generate_self_test_suite.py --check
```
