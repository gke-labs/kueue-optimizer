# Agent Execution & Cross-Examination Guide — `st13-preempt-within-cq-lower-or-newer-equal`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st13-preempt-within-cq-lower-or-newer-equal`
- **Category**: 3. Intra-CQ Preemption Matrix
- **Title**: Equal-Priority Timestamp Semantics (`withinClusterQueue: LowerOrNewerEqualPriority`)
- **Primary Verification Target**: Verifies `LowerOrNewerEqualPriority` protects older `t=0s` incumbents while allowing an older `t=2s` waiting job to preempt newer `t=5s` equal-priority backfills without ping-pong loops
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st13` (`--namespace kueue-optimizer-chamber-st13`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16413` | Prometheus `19060` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st38-t0-preempt-lower-or-newer-equal-protection`](../st38-t0-preempt-lower-or-newer-equal-protection/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST13 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st13' (API port 16413, Prom port 19060):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st13-preempt-within-cq-lower-or-newer-equal --namespace kueue-optimizer-chamber-st13 --port 16413 > /tmp/st13.log 2>&1 &
PID_ST13=$!

# Simultaneously launch paired ST38 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st38' (API port 16438, Prom port 19085):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st38-t0-preempt-lower-or-newer-equal-protection --no-skip-initial-offset --namespace kueue-optimizer-chamber-st38 --port 16438 > /tmp/st38.log 2>&1 &
PID_ST38=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST13" "$PID_ST38"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST13 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st13' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st13-preempt-within-cq-lower-or-newer-equal --namespace kueue-optimizer-chamber-st13 --port 16413 | tee /tmp/st13.log

# 2. Run paired ST38 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st38':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st38-t0-preempt-lower-or-newer-equal-protection --no-skip-initial-offset --namespace kueue-optimizer-chamber-st38 --port 16438 | tee /tmp/st38.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st13-preempt-within-cq-lower-or-newer-equal --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st13`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st13-preempt-within-cq-lower-or-newer-equal` (`kueue-optimizer-chamber-st13`) ↔ [`st38-t0-preempt-lower-or-newer-equal-protection`](../st38-t0-preempt-lower-or-newer-equal-protection/AGENTS.md) (`kueue-optimizer-chamber-st38`) — Fresh `t=0s` Seniority (`st13`) vs. Pre-Existing `t0` `PodGroup` Seniority (`st38`) Under `LowerOrNewerEqualPriority`

- **Controlled Variable Changed**: `st13` creates older incumbents as `NotAdmitted` arrivals at `t=0s`, whereas `st38` bootstraps them as pre-admitted `t0` `PodGroup`s before queue activation.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st38-t0-preempt-lower-or-newer-equal-protection --no-skip-initial-offset --namespace kueue-optimizer-chamber-st38 --port 16438`
- **Cross-Examination Assertion**: Both runs protect all `t0` / `t=0s` incumbents (`0` evictions of initial jobs) and evict ONLY the `2` newer equal-priority backfill jobs (`InClusterQueue = 2`).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `~75%–95%` (`st13`) vs. `~75%–95%` (`st38`)
  - `Sec 5.2 Wait Time`: `Older 12-TPU job waits ~10s` (`st13`) vs. `arr-eq-older-3n waits ~10s–12s` (`st38`)
  - `Sec 5.3 Evictions`: `2 evictions (InClusterQueue, newer-equal)` (`st13`) vs. `2 evictions (0 t0 incumbents evicted)` (`st38`)
  - `Sec 5.4 Effective Sat`: `~65%–88%` (`st13`) vs. `~65%–88% (9 completed)` (`st38`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 8-node (`32`-TPU) cluster, `queueingStrategy: BestEffortFIFO`, `withinClusterQueue: LowerOrNewerEqualPriority`, single priority class `p-equal (500)`. 6 older `1-pod` jobs start at `t=0s` (`24/32` TPUs, one finishing at `t=12s`); 1 `3-pod` (`12`-TPU) job arrives at `t=2s` (blocked behind `t=0s` incumbents); 2 newer `1-pod` jobs arrive at `t=5s` and backfill to `32/32` TPUs until `t=12s`, when the older `t=2s` job preempts the newer `t=5s` jobs.
- **Expected Runtime Behavior**: `t=0s` older incumbents are protected at `t=2s`; `t=5s` newer jobs backfill the `8` free TPUs (`t=5s..12s`); at `t=12s` the older `t=2s` job preempts the 2 newer `t=5s` jobs (`InClusterQueue = 2` evictions) with zero counter-preemption ping-pong loop, and all `9/9` jobs complete cleanly.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `~75%–95%` |
| **Section `5.2` Queue Wait Time** | `Older 12-TPU job waits ~10s` |
| **Section `5.3` Evictions & Reclaims** | `2 evictions (InClusterQueue, newer-equal)` |
| **Section `5.4` Effective Saturation** | `~65%–88%` |

### Detailed Metric Checkpoints

- `Section 5.3 (Evictions)`: **`2`** (`InClusterQueue = 2` evictions of the newer `t=5s` jobs by the older `t=2s` job; `0` evictions of `t=0s` incumbents; `0` ping-pong re-evictions).
- `Section 5.4 (Effective Saturation)`: **`~65%–88%`** with `100%` (`9/9`) job completion.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ Ping-pong preemption loop (`Total Evictions >> 4`) between equal-priority workloads.
- ❌ Preemption of any older `t=0s` incumbent by a newer equal-priority arrival.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
