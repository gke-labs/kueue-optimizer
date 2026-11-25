# Agent Execution & Cross-Examination Guide — `st45-t0-no-fair-sharing-monopolizer-control`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st45-t0-no-fair-sharing-monopolizer-control`
- **Category**: 9. Initial t0 Workloads — Reclaims, Limits & Fair Sharing
- **Title**: Zero-Nominal Cohort Monopolized at `t0` Without Fair-Sharing Preemption (`reclaimWithinCohort: Never` — Twin of st44)
- **Primary Verification Target**: Direct A/B control twin of `st44` with `reclaimWithinCohort: Never`: `cq-hog`'s 8 `t0` `PodGroup`s hold `32/32` Cohort TPUs uninterrupted while all 6 `cq-victim` arrivals wait `~19s`
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st45` (`--namespace kueue-optimizer-chamber-st45`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16445` | Prometheus `19092` (collision-free across all 95 scenarios `st01`–`st95`)
- **Mandatory Paired Counterpart(s)**: [`st44-t0-fair-sharing-preemption-over-monopolizer`](../st44-t0-fair-sharing-preemption-over-monopolizer/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st95`) and non-overlapping port pair (`16401/19048` .. `16495/19142`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST45 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st45' (API port 16445, Prom port 19092):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st45-t0-no-fair-sharing-monopolizer-control --no-skip-initial-offset --namespace kueue-optimizer-chamber-st45 --port 16445 > /tmp/st45.log 2>&1 &
PID_ST45=$!

# Simultaneously launch paired ST44 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st44' (API port 16444, Prom port 19091):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st44-t0-fair-sharing-preemption-over-monopolizer --no-skip-initial-offset --namespace kueue-optimizer-chamber-st44 --port 16444 > /tmp/st44.log 2>&1 &
PID_ST44=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST45" "$PID_ST44"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST45 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st45' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st45-t0-no-fair-sharing-monopolizer-control --no-skip-initial-offset --namespace kueue-optimizer-chamber-st45 --port 16445 | tee /tmp/st45.log

# 2. Run paired ST44 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st44':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st44-t0-fair-sharing-preemption-over-monopolizer --no-skip-initial-offset --namespace kueue-optimizer-chamber-st44 --port 16444 | tee /tmp/st44.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st45-t0-no-fair-sharing-monopolizer-control --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st45`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st45-t0-no-fair-sharing-monopolizer-control` (`kueue-optimizer-chamber-st45`) ↔ [`st44-t0-fair-sharing-preemption-over-monopolizer`](../st44-t0-fair-sharing-preemption-over-monopolizer/AGENTS.md) (`kueue-optimizer-chamber-st44`) — Exact A/B Twin (`t0` Monopolizer Control `st45` vs. `1:1` Fair-Sharing Preemption `st44`)

- **Controlled Variable Changed**: `reclaimWithinCohort: Never` without weights (`st45`) vs. `1:1` Fair Sharing with `reclaimWithinCohort: Any` (`st44`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st44-t0-fair-sharing-preemption-over-monopolizer --no-skip-initial-offset --namespace kueue-optimizer-chamber-st44 --port 16444`
- **Cross-Examination Assertion**: `Sec 5.3 Evictions` flips from `0` (`st45`) to `4` (`st44`), proving `t0` `PodGroup`s participate in both Cohort quota blocking (`st45`) and DRS Fair-Sharing preemption (`st44`).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `Cohort ~85%–93%` (`st45`) vs. `Cohort ~85%–98% (100% -> 50/50 split)` (`st44`)
  - `Sec 5.2 Wait Time`: `cq-victim waits ~18s–20s` (`st45`) vs. `4 arr-tb < 2.5s, 2 arr-tb ~18s` (`st44`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st45`) vs. `4 FairSharing evictions (never 6)` (`st44`)
  - `Sec 5.4 Effective Sat`: `~82%–92% (14 completed, Excluded: 0)` (`st45`) vs. `~72%–90% (14 completed)` (`st44`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: Identical `32`-TPU zero-nominal Cohort, 8 `t0` `Admitted` `JobSet`s (`30s`), and 6 peer arrivals at `t=11s` (`18s`) as `st44`, changing `reclaimWithinCohort` from `Any` to `Never` (and omitting explicit weights).
- **Expected Runtime Behavior**: Zero `t0` `PodGroup`s are evicted (`Sec 5.3 = 0` vs `4` in `st44`). All 6 `arr-vic-*` jobs wait until `init-hog-*` finish at `t=30s` (`wait ≈ 19s`), then execute in `30s–48s`.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `Cohort ~85%–93%` |
| **Section `5.2` Queue Wait Time** | `cq-victim waits ~18s–20s` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `~82%–92% (14 completed, Excluded: 0)` |

### Detailed Metric Checkpoints

- `Section 5.2`: `arr-vic-*` wait **`~16s–21s`** (vs `< 2.5s` for the first 4 jobs in `st44`).
- `Section 5.3`: Strictly **`0`** evictions (vs `4` in `st44`).
- `Section 5.4`: `Qualifying Workloads: 14 (completed=14 [14 pods])`, `Excluded: 0`, **`EFFECTIVE SAT ≈ 82%–92%`** (`(32×30 + 24×18) / (32×48) = 1,392 / 1,536 = 90.6%`).

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ Any eviction (`Section 5.3 > 0`) when `reclaimWithinCohort: Never`.
- ❌ `arr-vic-*` admitting before `t=30s` while `init-hog-*` occupy `32/32` Cohort TPUs.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
