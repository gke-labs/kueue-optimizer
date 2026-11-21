# Agent Execution & Cross-Examination Guide — `st44-t0-fair-sharing-preemption-over-monopolizer`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and re-run:
> ```bash
> python3 testscenarios/self-tests/generate_self_test_suite.py
> python3 deploy/scripts/generate_scenario_readme.py --all
> python3 testscenarios/self-tests/generate_self_test_suite.py --check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st44-t0-fair-sharing-preemption-over-monopolizer`
- **Category**: 9. Initial t0 Workloads — Reclaims, Limits & Fair Sharing
- **Title**: Cohort Fair-Sharing Preemption (`1 : 1` Weights) Evicting **Exact 4** of 8 `t0` Monopolizer `PodGroup`s
- **Primary Verification Target**: Proves both (1) `t0` `PodGroup`s are preemptible via Fair Sharing and (2) `cq-team-a`'s remaining `16` `t0` TPUs anchor its DRS at `0.5` so `cq-team-b` (`6`-job demand) stops at **exact 4** evictions (`50/50` split)
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st44` (`--namespace kueue-optimizer-chamber-st44`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16444` | Prometheus `19091` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st45-t0-no-fair-sharing-monopolizer-control`](../st45-t0-no-fair-sharing-monopolizer-control/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST44 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st44' (API port 16444, Prom port 19091):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st44-t0-fair-sharing-preemption-over-monopolizer --no-skip-initial-offset --namespace kueue-optimizer-chamber-st44 --port 16444 > /tmp/st44.log 2>&1 &
PID_ST44=$!

# Simultaneously launch paired ST45 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st45' (API port 16445, Prom port 19092):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st45-t0-no-fair-sharing-monopolizer-control --no-skip-initial-offset --namespace kueue-optimizer-chamber-st45 --port 16445 > /tmp/st45.log 2>&1 &
PID_ST45=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST44" "$PID_ST45"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST44 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st44' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st44-t0-fair-sharing-preemption-over-monopolizer --no-skip-initial-offset --namespace kueue-optimizer-chamber-st44 --port 16444 | tee /tmp/st44.log

# 2. Run paired ST45 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st45':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st45-t0-no-fair-sharing-monopolizer-control --no-skip-initial-offset --namespace kueue-optimizer-chamber-st45 --port 16445 | tee /tmp/st45.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st44-t0-fair-sharing-preemption-over-monopolizer --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st44`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st44-t0-fair-sharing-preemption-over-monopolizer` (`kueue-optimizer-chamber-st44`) ↔ [`st45-t0-no-fair-sharing-monopolizer-control`](../st45-t0-no-fair-sharing-monopolizer-control/AGENTS.md) (`kueue-optimizer-chamber-st45`) — Exact A/B Twin (`t0` Cohort Monopolizer With `1:1` Fair-Sharing Preemption `st44` vs. Without Preemption `st45`)

- **Controlled Variable Changed**: `fairSharing.weight: '1'` + `reclaimWithinCohort: Any` (`st44`) vs. unweighted `reclaimWithinCohort: Never` (`st45`) against identical 8 `t0` monopolizer `PodGroup`s and 6 peer arrivals at `t=11s`.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st45-t0-no-fair-sharing-monopolizer-control --no-skip-initial-offset --namespace kueue-optimizer-chamber-st45 --port 16445`
- **Cross-Examination Assertion**: In `st44`, Fair Sharing evicts **exact 4** (never `0` and never `6`) of `cq-team-a`'s `t0` `PodGroup`s so `cq-team-b` immediately gets its `16`-TPU (`50%`) share while 2 of `cq-team-b`'s 6 jobs wait; in `st45`, `0` evictions occur and all 6 peer jobs wait `~19s`.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `Cohort ~85%–98% (100% -> 50/50 split)` (`st44`) vs. `Cohort ~85%–93%` (`st45`)
  - `Sec 5.2 Wait Time`: `4 arr-tb < 2.5s, 2 arr-tb ~18s` (`st44`) vs. `cq-victim waits ~18s–20s` (`st45`)
  - `Sec 5.3 Evictions`: `4 FairSharing evictions (never 6)` (`st44`) vs. `0 evictions` (`st45`)
  - `Sec 5.4 Effective Sat`: `~72%–90% (14 completed)` (`st44`) vs. `~82%–92% (14 completed, Excluded: 0)` (`st45`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 8-node (`32`-TPU) cluster, `cohort-t0-fs` (`32` Cohort TPUs). `cq-team-a` (`nominalQuota=0, weight=1, reclaimWithinCohort=Any`) starts at `t0` with 8 `Admitted` `JobSet`s (`32/32` TPUs, `34s` remaining, `20s` restart). At `t=11s`, `cq-team-b` (`nominalQuota=0, weight=1, reclaimWithinCohort=Any`) submits **6** `JobSet`s (`24` TPUs = `75%` demand, `18s`).
- **Expected Runtime Behavior**: At `t=11s`, Preemption-Based Fair Sharing (`LessThanOrEqualToFinalShare`) evicts **exact 4** of `cq-team-a`'s `t0` `PodGroup`s (`16` TPUs = `50%` fair share — refusing to evict a 5th or 6th `t0` job because `cq-team-a` and `cq-team-b` have equal `1 : 1` weights). 4 of `cq-team-b`'s jobs admit immediately while its 5th & 6th jobs wait; the 4 evicted `init-ta-*` `JobSet`s are dynamically resubmitted and all 14 workloads complete.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `Cohort ~85%–98% (100% -> 50/50 split)` |
| **Section `5.2` Queue Wait Time** | `4 arr-tb < 2.5s, 2 arr-tb ~18s` |
| **Section `5.3` Evictions & Reclaims** | `4 FairSharing evictions (never 6)` |
| **Section `5.4` Effective Saturation** | `~72%–90% (14 completed)` |

### Detailed Metric Checkpoints

- `Execution Summary`: `Workflows Submitted: 10/10 [+4 preempted-t0 resubmit]`.
- `Section 5.3`: **`4`** evictions (`4` unique, `FairSharing` / cohort preemption) — strictly `4`, NEVER `6`.
- `Section 5.2`: 4 of `cq-team-b`'s jobs admit in `< 2.5s` while 2 wait `~18s`.
- `Section 5.4`: `Qualifying Workloads: 14 (completed=14 [14 pods])`, **`EFFECTIVE SAT ≈ 72%–90%`**.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ `Section 5.3 Total Evictions == 0` (would mean Fair Sharing failed to preempt `t0` `PodGroup` monopolizers).
- ❌ `Section 5.3 Total Evictions == 6` (would mean `cq-team-a`'s surviving `t0` `PodGroup`s were missing from `DominantResourceShare` calculation).

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
python3 testscenarios/self-tests/generate_self_test_suite.py
python3 deploy/scripts/generate_scenario_readme.py --all
python3 testscenarios/self-tests/generate_self_test_suite.py --check
```
