# Agent Execution & Cross-Examination Guide — `st39-t0-late-preemption-badput-accounting`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st39-t0-late-preemption-badput-accounting`
- **Category**: 8. Initial t0 Workloads — Preemption & TAS
- **Title**: Adversarial Late Preemption of `t0` `PodGroup`s (`Sec 5.1 ≈ 97%` vs `Sec 5.4 ≈ 36%`, `Excluded: 8`)
- **Primary Verification Target**: Verifies aborted `t0` `PodGroup` compute (`0s–24s`) is excluded from `Sec 5.4` goodput and pending resubmitted workflows classify under `Excluded (preempted/reclaimed/failed): 8`
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st39` (`--namespace kueue-optimizer-chamber-st39`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16439` | Prometheus `19086` (collision-free across all 95 scenarios `st01`–`st95`)
- **Mandatory Paired Counterpart(s)**: [`st15-preemption-badput-collapse-stress`](../st15-preemption-badput-collapse-stress/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st95`) and non-overlapping port pair (`16401/19048` .. `16495/19142`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST39 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st39' (API port 16439, Prom port 19086):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st39-t0-late-preemption-badput-accounting --no-skip-initial-offset --duration 38s --namespace kueue-optimizer-chamber-st39 --port 16439 > /tmp/st39.log 2>&1 &
PID_ST39=$!

# Simultaneously launch paired ST15 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st15' (API port 16415, Prom port 19062):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st15-preemption-badput-collapse-stress --duration 41s --namespace kueue-optimizer-chamber-st15 --port 16415 > /tmp/st15.log 2>&1 &
PID_ST15=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST39" "$PID_ST15"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST39 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st39' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st39-t0-late-preemption-badput-accounting --no-skip-initial-offset --duration 38s --namespace kueue-optimizer-chamber-st39 --port 16439 | tee /tmp/st39.log

# 2. Run paired ST15 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st15':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st15-preemption-badput-collapse-stress --duration 41s --namespace kueue-optimizer-chamber-st15 --port 16415 | tee /tmp/st15.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st39-t0-late-preemption-badput-accounting --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st39`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st39-t0-late-preemption-badput-accounting` (`kueue-optimizer-chamber-st39`) ↔ [`st15-preemption-badput-collapse-stress`](../st15-preemption-badput-collapse-stress/AGENTS.md) (`kueue-optimizer-chamber-st15`) — `t0` Late-Preemption Badput Collapse (`st39`) vs. Scheduled Late-Preemption Badput Collapse (`st15`) Pair

- **Controlled Variable Changed**: 8 `t0` `PodGroup` victims killed at `t=24s` (`--duration 38s`, `st39`) vs. 8 scheduled `JobSet` victims killed at `t=26s` (`--duration 41s`, `st15`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st15-preemption-badput-collapse-stress --duration 41s --namespace kueue-optimizer-chamber-st15 --port 16415`
- **Cross-Examination Assertion**: Both report `Sec 5.1 ≈ 93%–99%`, `Sec 5.3 = 8` evictions, `Sec 5.4 EFFECTIVE SAT ≈ 31%–41%` (`~55–60pp` badput gap), and `Excluded: 8` (`Unadmitted: 0`).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `92%–99% (raw occupancy)` (`st39`) vs. `93%–99% (raw occupancy)` (`st15`)
  - `Sec 5.2 Wait Time`: `p-urgent wait < 2.0s` (`st39`) vs. `p-urgent wait < 1.5s` (`st15`)
  - `Sec 5.3 Evictions`: `8 evictions` (`st39`) vs. `8 evictions` (`st15`)
  - `Sec 5.4 Effective Sat`: `~31%–41% (Excluded: 8, Unadmitted: 0)` (`st39`) vs. `~32%–40% (~60pp badput gap)` (`st15`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 8-node (`32`-TPU) cluster, `withinClusterQueue: LowerPriority`. At `t0`, 8 `p-batch (100)` `Admitted` `JobSet`s (`32s` remaining) occupy `32/32` TPUs. At `t=24s`, 8 `p-urgent (1000)` `JobSet`s (`18s` duration) arrive and preempt ALL 8 `t0` `PodGroup`s; window ends at `--duration 38s` while `arr-urgent-*` are actively running (`14s` elapsed) and all 8 resubmitted `init-victim-*` sit queued.
- **Expected Runtime Behavior**: All 8 `t0` `PodGroup`s are killed at `t=24s` (`8` evictions) and GC'd; `testrunner.py` resubmits all 8 `init-victim-*` `JobSet`s (`kueue-optimizer.gke.io/preempted-t0-resubmit: 'true'`). At `t=38s`, `Section 5.4` credits ONLY the `~14s` of uninterrupted `arr-urgent-*` execution (`14/38 ≈ 36.8%`) and reports `Excluded (preempted/reclaimed/failed): 8 [8 pods] | Unadmitted: 0`.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `92%–99% (raw occupancy)` |
| **Section `5.2` Queue Wait Time** | `p-urgent wait < 2.0s` |
| **Section `5.3` Evictions & Reclaims** | `8 evictions` |
| **Section `5.4` Effective Saturation** | `~31%–41% (Excluded: 8, Unadmitted: 0)` |

### Detailed Metric Checkpoints

- `Section 5.1 (cq-t0-badput)`: **`AVG ≈ 92%–99%`**.
- `Section 5.3`: **`8`** evictions (`InClusterQueue = 8`, `priority_class=p-batch`).
- `Section 5.4`: **`EFFECTIVE SAT ≈ 31%–41%`** (`ACTIVE AT END ≈ 31%–41%`, `COMPLETED ONLY = 0.00%`), `Excluded (preempted/reclaimed/failed): 8 [8 pods]`, `Unadmitted: 0`.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ `Section 5.4` exceeding `46%` (would mean preempted `t0` `PodGroup` runtime leaked into goodput).
- ❌ Resubmitted `init-victim-*` workloads misreported as `Unadmitted: 8` (`Excluded: 0`) despite having been preempted at `t=24s`.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
