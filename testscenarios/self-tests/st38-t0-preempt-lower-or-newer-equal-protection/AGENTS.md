# Agent Execution & Cross-Examination Guide — `st38-t0-preempt-lower-or-newer-equal-protection`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st38-t0-preempt-lower-or-newer-equal-protection`
- **Category**: 8. Initial t0 Workloads — Preemption & TAS
- **Title**: Seniority Protection of `t0` `PodGroup`s Under `withinClusterQueue: LowerOrNewerEqualPriority`
- **Primary Verification Target**: Verifies `t0` `PodGroup`s carry older `creationTimestamp` than `t > 0` arrivals so equal-priority newcomers never preempt `t0` incumbents
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st38` (`--namespace kueue-optimizer-chamber-st38`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16438` | Prometheus `19085` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st13-preempt-within-cq-lower-or-newer-equal`](../st13-preempt-within-cq-lower-or-newer-equal/AGENTS.md), [`st37-t0-preempt-within-cq-lower-priority-resubmit`](../st37-t0-preempt-within-cq-lower-priority-resubmit/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST38 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st38' (API port 16438, Prom port 19085):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st38-t0-preempt-lower-or-newer-equal-protection --no-skip-initial-offset --namespace kueue-optimizer-chamber-st38 --port 16438 > /tmp/st38.log 2>&1 &
PID_ST38=$!

# Simultaneously launch paired ST13 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st13' (API port 16413, Prom port 19060):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st13-preempt-within-cq-lower-or-newer-equal --namespace kueue-optimizer-chamber-st13 --port 16413 > /tmp/st13.log 2>&1 &
PID_ST13=$!

# Simultaneously launch paired ST37 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st37' (API port 16437, Prom port 19084):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st37-t0-preempt-within-cq-lower-priority-resubmit --no-skip-initial-offset --namespace kueue-optimizer-chamber-st37 --port 16437 > /tmp/st37.log 2>&1 &
PID_ST37=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST38" "$PID_ST13" "$PID_ST37"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST38 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st38' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st38-t0-preempt-lower-or-newer-equal-protection --no-skip-initial-offset --namespace kueue-optimizer-chamber-st38 --port 16438 | tee /tmp/st38.log

# 2. Run paired ST13 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st13':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st13-preempt-within-cq-lower-or-newer-equal --namespace kueue-optimizer-chamber-st13 --port 16413 | tee /tmp/st13.log

# 3. Run paired ST37 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st37':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st37-t0-preempt-within-cq-lower-priority-resubmit --no-skip-initial-offset --namespace kueue-optimizer-chamber-st37 --port 16437 | tee /tmp/st37.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st38-t0-preempt-lower-or-newer-equal-protection --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st38`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st38-t0-preempt-lower-or-newer-equal-protection` (`kueue-optimizer-chamber-st38`) ↔ [`st13-preempt-within-cq-lower-or-newer-equal`](../st13-preempt-within-cq-lower-or-newer-equal/AGENTS.md) (`kueue-optimizer-chamber-st13`) — `t0` `PodGroup` Seniority (`st38`) vs. `t=0s` `JobSet` Seniority (`st13`) Under `LowerOrNewerEqualPriority`

- **Controlled Variable Changed**: Pre-admitted `t0` `PodGroup` incumbents (`st38`) vs. `t=0s` `NotAdmitted` incumbents (`st13`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st13-preempt-within-cq-lower-or-newer-equal --namespace kueue-optimizer-chamber-st13 --port 16413`
- **Cross-Examination Assertion**: Both emit exact `2` evictions (evicting only the 2 newer equal-priority backfills) and `0` evictions of `t0` incumbents.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `~75%–95%` (`st38`) vs. `~75%–95%` (`st13`)
  - `Sec 5.2 Wait Time`: `arr-eq-older-3n waits ~10s–12s` (`st38`) vs. `Older 12-TPU job waits ~10s` (`st13`)
  - `Sec 5.3 Evictions`: `2 evictions (0 t0 incumbents evicted)` (`st38`) vs. `2 evictions (InClusterQueue, newer-equal)` (`st13`)
  - `Sec 5.4 Effective Sat`: `~65%–88% (9 completed)` (`st38`) vs. `~65%–88%` (`st13`)

### Pair: `st38-t0-preempt-lower-or-newer-equal-protection` (`kueue-optimizer-chamber-st38`) ↔ [`st37-t0-preempt-within-cq-lower-priority-resubmit`](../st37-t0-preempt-within-cq-lower-priority-resubmit/AGENTS.md) (`kueue-optimizer-chamber-st37`) — Equal-Priority Seniority Protection (`st38`) vs. Higher-Priority Preemption (`st37`) on `t0` `PodGroup`s

- **Controlled Variable Changed**: Equal priority `500` with `LowerOrNewerEqualPriority` (`st38`) vs. `p-high (1000) > p-low (100)` with `LowerPriority` (`st37`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st37-t0-preempt-within-cq-lower-priority-resubmit --no-skip-initial-offset --namespace kueue-optimizer-chamber-st37 --port 16437`
- **Cross-Examination Assertion**: `t0` `PodGroup` evictions drop from `4` (`st37`) to `0` (`st38`).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `~75%–95%` (`st38`) vs. `~82%–96%` (`st37`)
  - `Sec 5.2 Wait Time`: `arr-eq-older-3n waits ~10s–12s` (`st38`) vs. `p-high wait < 2.5s` (`st37`)
  - `Sec 5.3 Evictions`: `2 evictions (0 t0 incumbents evicted)` (`st38`) vs. `4 evictions (InClusterQueue, p-low)` (`st37`)
  - `Sec 5.4 Effective Sat`: `~65%–88% (9 completed)` (`st38`) vs. `~70%–86% (12 completed after resubmit)` (`st37`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 8-node (`32`-TPU) cluster, `withinClusterQueue: LowerOrNewerEqualPriority`, single priority `p-equal (500)`. At `t0`, 6 `Admitted` `1-pod` jobs (`24/32` TPUs: 5 running `32s`, 1 running `14s`) hold `st-node-00..05`. At `t=3s`, `arr-eq-older-3n` (`12` TPUs, `18s`) arrives; at `t=6s`, 2 `1-pod` `arr-eq-newer-*` jobs (`8` TPUs, `28s`) backfill `st-node-06..07`.
- **Expected Runtime Behavior**: `arr-eq-older-3n` (`t=3s`) CANNOT preempt any `t0` `PodGroup` incumbent (`creationTimestamp < t0`). When `init-eq-short` finishes at `t≈14s`, `arr-eq-older-3n` preempts ONLY the 2 newer `t=6s` backfills (`InClusterQueue = 2`), leaving all 5 remaining `t0` `PodGroup`s untouched (`0` `preempted-t0` resubmits).

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `~75%–95%` |
| **Section `5.2` Queue Wait Time** | `arr-eq-older-3n waits ~10s–12s` |
| **Section `5.3` Evictions & Reclaims** | `2 evictions (0 t0 incumbents evicted)` |
| **Section `5.4` Effective Saturation** | `~65%–88% (9 completed)` |

### Detailed Metric Checkpoints

- `Execution Summary`: `Workflows Submitted: 3/3` (zero `[+N preempted-t0 resubmit]`).
- `Section 5.3`: **`2`** evictions (`InClusterQueue = 2` against `arr-eq-newer-*`; `0` evictions of `t0` incumbents).
- `Section 5.4`: `Qualifying Workloads: 9 (completed=9 [11 pods])`, **`EFFECTIVE SAT ≈ 65%–88%`**.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ Any `t0` `PodGroup` (`init-eq-*`) being preempted by an equal-priority `t > 0` arrival.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
