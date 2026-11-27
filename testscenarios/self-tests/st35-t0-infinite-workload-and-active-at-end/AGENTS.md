# Agent Execution & Cross-Examination Guide — `st35-t0-infinite-workload-and-active-at-end`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st35-t0-infinite-workload-and-active-at-end`
- **Category**: 7. Initial t0 Workloads — Handling & Accounting
- **Title**: Long-Running / Infinite `t0` Kueue Workloads (`is_infinite=True`) With `ACTIVE AT END` & `FULL-DUR SAT` Guard
- **Primary Verification Target**: Verifies `t0` `PodGroup`s with no `deletionTime` (`864000s` placeholder) report exact `~50%` `ACTIVE AT END` without inflating `FULL-DUR SAT`
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st35` (`--namespace kueue-optimizer-chamber-st35`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16435` | Prometheus `19082` (collision-free across all 95 scenarios `st01`–`st95`)
- **Mandatory Paired Counterpart(s)**: [`st05-mixed-completed-and-active-at-end`](../st05-mixed-completed-and-active-at-end/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st95`) and non-overlapping port pair (`16401/19048` .. `16495/19142`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST35 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st35' (API port 16435, Prom port 19082):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st35-t0-infinite-workload-and-active-at-end --duration 40s --namespace kueue-optimizer-chamber-st35 --port 16435 > /tmp/st35.log 2>&1 &
PID_ST35=$!

# Simultaneously launch paired ST05 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st05' (API port 16405, Prom port 19052):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st05-mixed-completed-and-active-at-end --duration 42s --namespace kueue-optimizer-chamber-st05 --port 16405 > /tmp/st05.log 2>&1 &
PID_ST05=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST35" "$PID_ST05"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST35 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st35' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st35-t0-infinite-workload-and-active-at-end --duration 40s --namespace kueue-optimizer-chamber-st35 --port 16435 | tee /tmp/st35.log

# 2. Run paired ST05 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st05':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st05-mixed-completed-and-active-at-end --duration 42s --namespace kueue-optimizer-chamber-st05 --port 16405 | tee /tmp/st05.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st35-t0-infinite-workload-and-active-at-end --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st35`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st35-t0-infinite-workload-and-active-at-end` (`kueue-optimizer-chamber-st35`) ↔ [`st05-mixed-completed-and-active-at-end`](../st05-mixed-completed-and-active-at-end/AGENTS.md) (`kueue-optimizer-chamber-st05`) — Infinite `t0` `PodGroup` Active-at-End (`st35`) vs. Finite `120s` Active-at-End (`st05`) Pair

- **Controlled Variable Changed**: 4 `t0` `PodGroup`s with omitted `Deleted`/`FullExecution` events (`is_infinite=True`, `864000s` stage timer) in `st35` vs. 4 scheduled `120s` JobSets in `st05`.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st05-mixed-completed-and-active-at-end --duration 42s --namespace kueue-optimizer-chamber-st05 --port 16405`
- **Cross-Examination Assertion**: `Sec 5.4` in `st35` reports `ACTIVE AT END ≈ 48%–50%`, `EFFECTIVE SAT ≈ 90%–98%`, AND `FULL-DUR SAT ≈ 90%–98%` (never inflated by `864000s`).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `~92%–100%` (`st35`) vs. `95%–100%` (`st05`)
  - `Sec 5.2 Wait Time`: `< 1.5s` (`st35`) vs. `< 1.5s` (`st05`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st35`) vs. `0 evictions` (`st05`)
  - `Sec 5.4 Effective Sat`: `COMPLETED ~45% + ACTIVE ~50% = ~95% (FULL-DUR ~95%)` (`st35`) vs. `COMPLETED ~47% + ACTIVE ~49% = ~96%` (`st05`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 8-node (`32`-TPU) cluster, `cq-t0-inf` (`32` TPUs). At `t0`, 4 `Admitted` `JobSet`s (`init-inf-0..3`, no pod `Deleted` event -> `is_infinite=True`) hold `st-node-00..03` (`16` TPUs = `50%`). Two waves of 4 short `18s` jobs run on `st-node-04..07` (`16` TPUs = `50%`) inside `--duration 40s`.
- **Expected Runtime Behavior**: All 4 `init-inf-*` workloads remain actively running at `t=40s` (`active_at_end=4 [4 pods]`), while all 8 short jobs complete (`completed=8 [8 pods]`). `kueue-optimizer.gke.io/infinite-duration: 'true'` prevents the `864000s` KWOK placeholder from inflating `FULL-DUR SAT`.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `~92%–100%` |
| **Section `5.2` Queue Wait Time** | `< 1.5s` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `COMPLETED ~45% + ACTIVE ~50% = ~95% (FULL-DUR ~95%)` |

### Detailed Metric Checkpoints

- `Section 5.1 (cq-t0-inf)`: **`~92%–100%`** (`32/32` TPUs for `36s`, `16/32` for `4s`).
- `Section 5.4`: `COMPLETED ONLY ≈ 42%–48%`, `ACTIVE AT END ≈ 48%–50%`, `EFFECTIVE SAT ≈ 90%–98%`, AND **`FULL-DUR SAT ≈ 90%–98%`** (matching `EFFECTIVE SAT` rather than exploding to `>1,000,000%`).

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ `FULL-DUR SAT` exceeding `110%` due to the `864000s` (`10d`) infinite-pod timer annotation.
- ❌ `ACTIVE AT END == 0.00%` when 4 `init-inf-*` `PodGroup`s ran uninterrupted across the entire `40s` window.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
