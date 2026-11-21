# Agent Execution & Cross-Examination Guide — `st21-no-fair-sharing-fcfs-hogging`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and re-run:
> ```bash
> python3 testscenarios/self-tests/generate_self_test_suite.py
> python3 deploy/scripts/generate_scenario_readme.py --all
> python3 testscenarios/self-tests/generate_self_test_suite.py --check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st21-no-fair-sharing-fcfs-hogging`
- **Category**: 5. Fair Sharing & Priority Thresholds
- **Title**: Zero-Nominal Shared Pool Without Fair Sharing (First-Mover Monopolization)
- **Primary Verification Target**: Baseline control for `st22`: shared `32`-TPU Cohort pool (`0` CQ nominal quotas, `reclaimWithinCohort: Never`)
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st21` (`--namespace kueue-optimizer-chamber-st21`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16421` | Prometheus `19068` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st22-equal-fair-sharing-50-50-split`](../st22-equal-fair-sharing-50-50-split/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST21 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st21' (API port 16421, Prom port 19068):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st21-no-fair-sharing-fcfs-hogging --namespace kueue-optimizer-chamber-st21 --port 16421 > /tmp/st21.log 2>&1 &
PID_ST21=$!

# Simultaneously launch paired ST22 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st22' (API port 16422, Prom port 19069):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st22-equal-fair-sharing-50-50-split --namespace kueue-optimizer-chamber-st22 --port 16422 > /tmp/st22.log 2>&1 &
PID_ST22=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST21" "$PID_ST22"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST21 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st21' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st21-no-fair-sharing-fcfs-hogging --namespace kueue-optimizer-chamber-st21 --port 16421 | tee /tmp/st21.log

# 2. Run paired ST22 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st22':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st22-equal-fair-sharing-50-50-split --namespace kueue-optimizer-chamber-st22 --port 16422 | tee /tmp/st22.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st21-no-fair-sharing-fcfs-hogging --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st21`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st21-no-fair-sharing-fcfs-hogging` (`kueue-optimizer-chamber-st21`) ↔ [`st22-equal-fair-sharing-50-50-split`](../st22-equal-fair-sharing-50-50-split/AGENTS.md) (`kueue-optimizer-chamber-st22`) — No Fair Sharing (`st21`) vs. Equal `1:1` Fair Sharing (`st22`) Pair

- **Controlled Variable Changed**: Unweighted shared Cohort pool (`st21`) vs. `fairSharing: {weight: '1'}` on both CQs (`st22`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st22-equal-fair-sharing-50-50-split --namespace kueue-optimizer-chamber-st22 --port 16422`
- **Cross-Examination Assertion**: In `st21`, first-mover `cq-hog` monopolizes `32/32` TPUs while `cq-victim` waits `~24s`; in `st22`, both teams split the `32`-TPU pool `50/50` (`16` vs. `16` TPUs) across every wave.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `Cohort ~80%–92%` (`st21`) vs. `Cohort 92%–100% (50/50 split)` (`st22`)
  - `Sec 5.2 Wait Time`: `cq-victim waits ~24s` (`st21`) vs. `Symmetric across both CQs (AVG ~16s)` (`st22`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st21`) vs. `0 evictions` (`st22`)
  - `Sec 5.4 Effective Sat`: `~78%–88%` (`st21`) vs. `90%–99%` (`st22`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 8-node (`32`-TPU) cluster. All `32` TPUs live on `Cohort: cohort-nofs`; both `cq-hog` and `cq-victim` have `nominalQuota: 0` and `reclaimWithinCohort: Never`. `cq-hog` submits 8 jobs (`32` TPUs, `28s`) at `t=0s`; `cq-victim` submits 4 jobs (`16` TPUs, `18s`) at `t=4s`.
- **Expected Runtime Behavior**: `cq-hog` grabs 100% of the shared `32`-TPU cohort pool at `t=0s`. When `cq-victim` arrives at `t=4s`, `reclaimWithinCohort: Never` prevents cohort preemption, so `cq-victim` waits (`0` TPUs) for `24s` until `cq-hog` finishes at `t=28s`.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `Cohort ~80%–92%` |
| **Section `5.2` Queue Wait Time** | `cq-victim waits ~24s` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `~78%–88%` |

### Detailed Metric Checkpoints

- `Section 5.1 (Cohort: cohort-nofs)`: `AVG ≈ 80%–95%` (`32/32` from `0s–28s`, `16/32` from `28s–46s`).
- `Section 5.2`: `cq-victim` jobs wait **`~24s`** while `cq-hog` jobs wait `~0s`.
- `Section 5.3`: **`0`** evictions.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ `cq-victim` preempting `cq-hog` when both have `0` nominal quota and `reclaimWithinCohort: Never`.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
python3 testscenarios/self-tests/generate_self_test_suite.py
python3 deploy/scripts/generate_scenario_readme.py --all
python3 testscenarios/self-tests/generate_self_test_suite.py --check
```
