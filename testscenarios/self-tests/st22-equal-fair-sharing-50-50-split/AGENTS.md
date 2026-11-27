# Agent Execution & Cross-Examination Guide — `st22-equal-fair-sharing-50-50-split`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st22-equal-fair-sharing-50-50-split`
- **Category**: 5. Fair Sharing & Priority Thresholds
- **Title**: Equal-Weight (`1 : 1`) Cohort Fair Sharing Convergence Under Heavy Dual Backlog
- **Primary Verification Target**: Equal `fairSharing.weight: '1'` across `cq-team-a` and `cq-team-b` splitting `32` Cohort TPUs symmetrically (`16` vs `16`)
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st22` (`--namespace kueue-optimizer-chamber-st22`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16422` | Prometheus `19069` (collision-free across all 95 scenarios `st01`–`st95`)
- **Mandatory Paired Counterpart(s)**: [`st23-heavy-asymmetric-fair-sharing-7-to-1`](../st23-heavy-asymmetric-fair-sharing-7-to-1/AGENTS.md), [`st44-t0-fair-sharing-preemption-over-monopolizer`](../st44-t0-fair-sharing-preemption-over-monopolizer/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st95`) and non-overlapping port pair (`16401/19048` .. `16495/19142`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST22 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st22' (API port 16422, Prom port 19069):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st22-equal-fair-sharing-50-50-split --namespace kueue-optimizer-chamber-st22 --port 16422 > /tmp/st22.log 2>&1 &
PID_ST22=$!

# Simultaneously launch paired ST23 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st23' (API port 16423, Prom port 19070):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st23-heavy-asymmetric-fair-sharing-7-to-1 --namespace kueue-optimizer-chamber-st23 --port 16423 > /tmp/st23.log 2>&1 &
PID_ST23=$!

# Simultaneously launch paired ST44 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st44' (API port 16444, Prom port 19091):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st44-t0-fair-sharing-preemption-over-monopolizer --no-skip-initial-offset --namespace kueue-optimizer-chamber-st44 --port 16444 > /tmp/st44.log 2>&1 &
PID_ST44=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST22" "$PID_ST23" "$PID_ST44"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST22 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st22' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st22-equal-fair-sharing-50-50-split --namespace kueue-optimizer-chamber-st22 --port 16422 | tee /tmp/st22.log

# 2. Run paired ST23 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st23':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st23-heavy-asymmetric-fair-sharing-7-to-1 --namespace kueue-optimizer-chamber-st23 --port 16423 | tee /tmp/st23.log

# 3. Run paired ST44 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st44':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st44-t0-fair-sharing-preemption-over-monopolizer --no-skip-initial-offset --namespace kueue-optimizer-chamber-st44 --port 16444 | tee /tmp/st44.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st22-equal-fair-sharing-50-50-split --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st22`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st22-equal-fair-sharing-50-50-split` (`kueue-optimizer-chamber-st22`) ↔ [`st23-heavy-asymmetric-fair-sharing-7-to-1`](../st23-heavy-asymmetric-fair-sharing-7-to-1/AGENTS.md) (`kueue-optimizer-chamber-st23`) — Symmetric `1:1` (`st22`) vs. Heavy Asymmetric `7:1` (`st23`) Fair-Sharing Pair

- **Controlled Variable Changed**: Weight ratio changes from `1 : 1` (`4` nodes vs. `4` nodes in `st22`) to `7 : 1` (`7` nodes vs. `1` node in `st23`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st23-heavy-asymmetric-fair-sharing-7-to-1 --namespace kueue-optimizer-chamber-st23 --port 16423`
- **Cross-Examination Assertion**: Active workload split across the two CQs shifts from `4 : 4` (`st22`) to `7 : 1` (`28` TPUs vs. `4` TPUs in `st23`) while keeping total Cohort utilization `~85%–100%`.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `Cohort 92%–100% (50/50 split)` (`st22`) vs. `Cohort ~80%–92% (87.5% vs 12.5% in 0s–32s)` (`st23`)
  - `Sec 5.2 Wait Time`: `Symmetric across both CQs (AVG ~16s)` (`st22`) vs. `cq-gold finishes all 14 in 2 waves (~32s)` (`st23`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st22`) vs. `0 steady-state (<= 6 t=0s convergence FairSharing)` (`st23`)
  - `Sec 5.4 Effective Sat`: `90%–99%` (`st22`) vs. `~76%–85%` (`st23`)

### Pair: `st22-equal-fair-sharing-50-50-split` (`kueue-optimizer-chamber-st22`) ↔ [`st44-t0-fair-sharing-preemption-over-monopolizer`](../st44-t0-fair-sharing-preemption-over-monopolizer/AGENTS.md) (`kueue-optimizer-chamber-st44`) — Simultaneous `1:1` Fair Sharing (`st22`) vs. Preemptive `1:1` Fair Sharing Over `t0` Monopolizer (`st44`) Pair

- **Controlled Variable Changed**: `st22` submits dual backlogs at `t=0s` (`0` evictions), whereas `st44` starts `cq-team-a` at `32/32` TPUs at `t0` and arrives `cq-team-b` at `t=11s` (`4` FairSharing evictions).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st44-t0-fair-sharing-preemption-over-monopolizer --no-skip-initial-offset --namespace kueue-optimizer-chamber-st44 --port 16444`
- **Cross-Examination Assertion**: Both converge to the exact `50/50` (`16` vs. `16` TPUs) Cohort split.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `Cohort 92%–100% (50/50 split)` (`st22`) vs. `Cohort ~85%–98% (100% -> 50/50 split)` (`st44`)
  - `Sec 5.2 Wait Time`: `Symmetric across both CQs (AVG ~16s)` (`st22`) vs. `4 arr-tb < 2.5s, 2 arr-tb ~18s` (`st44`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st22`) vs. `4 FairSharing evictions (never 6)` (`st44`)
  - `Sec 5.4 Effective Sat`: `90%–99%` (`st22`) vs. `~72%–90% (14 completed)` (`st44`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 8-node (`32`-TPU) cluster, `cohort-fs-eq` (`32` Cohort TPUs). `cq-team-a` (`weight: 1`) and `cq-team-b` (`weight: 1`) each submit 12 single-node (`4`-TPU, `16s`) jobs simultaneously at `t=0s` (24 jobs = `96` TPUs = 3 full cluster waves).
- **Expected Runtime Behavior**: Across all 3 waves (`0s–48s`), Kueue DRF fair sharing allocates `16` TPUs (`4` nodes) to `cq-team-a` and `16` TPUs (`4` nodes) to `cq-team-b` in every wave, producing balanced wait-time distributions and `kueue_cluster_queue_weighted_share` values.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `Cohort 92%–100% (50/50 split)` |
| **Section `5.2` Queue Wait Time** | `Symmetric across both CQs (AVG ~16s)` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `90%–99%` |

### Detailed Metric Checkpoints

- `Section 1 Gauge (kueue_cluster_queue_weighted_share)`: `cq-team-a` and `cq-team-b` report balanced `AVG` weighted share.
- `Section 5.1 (Cohort: cohort-fs-eq)`: **`92%–100%`** (`32/32` TPUs full across all 3 waves).
- `Section 5.4 (Effective Saturation)`: **`90%–99%`** (all 24 jobs complete in `~48s`).

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ Severe starvation of either team (one team taking `>75%` of cluster capacity across the run despite identical weights and simultaneous arrivals).

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
