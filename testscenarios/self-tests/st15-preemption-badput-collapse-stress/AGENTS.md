# Agent Execution & Cross-Examination Guide — `st15-preemption-badput-collapse-stress`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and re-run:
> ```bash
> python3 testscenarios/self-tests/generate_self_test_suite.py
> python3 deploy/scripts/generate_scenario_readme.py --all
> python3 testscenarios/self-tests/generate_self_test_suite.py --check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st15-preemption-badput-collapse-stress`
- **Category**: 3. Intra-CQ Preemption Matrix
- **Title**: Adversarial Late-Preemption Goodput Collapse (`Sec 5.1 ≈ 98%` vs `Sec 5.4 ≈ 35%`)
- **Primary Verification Target**: Stress-tests the gap between raw quota occupancy (`Sec 5.1`) and uninterrupted effective saturation (`Sec 5.4`)
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st15` (`--namespace kueue-optimizer-chamber-st15`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16415` | Prometheus `19062` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st39-t0-late-preemption-badput-accounting`](../st39-t0-late-preemption-badput-accounting/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST15 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st15' (API port 16415, Prom port 19062):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st15-preemption-badput-collapse-stress --duration 41s --namespace kueue-optimizer-chamber-st15 --port 16415 > /tmp/st15.log 2>&1 &
PID_ST15=$!

# Simultaneously launch paired ST39 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st39' (API port 16439, Prom port 19086):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st39-t0-late-preemption-badput-accounting --no-skip-initial-offset --duration 38s --namespace kueue-optimizer-chamber-st39 --port 16439 > /tmp/st39.log 2>&1 &
PID_ST39=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST15" "$PID_ST39"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST15 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st15' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st15-preemption-badput-collapse-stress --duration 41s --namespace kueue-optimizer-chamber-st15 --port 16415 | tee /tmp/st15.log

# 2. Run paired ST39 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st39':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st39-t0-late-preemption-badput-accounting --no-skip-initial-offset --duration 38s --namespace kueue-optimizer-chamber-st39 --port 16439 | tee /tmp/st39.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st15-preemption-badput-collapse-stress --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st15`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st15-preemption-badput-collapse-stress` (`kueue-optimizer-chamber-st15`) ↔ [`st39-t0-late-preemption-badput-accounting`](../st39-t0-late-preemption-badput-accounting/AGENTS.md) (`kueue-optimizer-chamber-st39`) — Scheduled Late-Preemption Badput (`st15`) vs. `t0` `PodGroup` Late-Preemption Badput (`st39`) Pair

- **Controlled Variable Changed**: `st15` kills 8 scheduled `t=0s` batch JobSets at `t=26s`, whereas `st39` kills 8 bootstrapped `t0` `PodGroup`s at `t=24s` (triggering GC + resubmission).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st39-t0-late-preemption-badput-accounting --no-skip-initial-offset --duration 38s --namespace kueue-optimizer-chamber-st39 --port 16439`
- **Cross-Examination Assertion**: Both runs expose `~55–60pp` badput gap (`Sec 5.1 ≈ 93%–99%` vs. `Sec 5.4 ≈ 32%–41%`), `8` evictions, and `Excluded: 8` (`Unadmitted: 0`).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `93%–99% (raw occupancy)` (`st15`) vs. `92%–99% (raw occupancy)` (`st39`)
  - `Sec 5.2 Wait Time`: `p-urgent wait < 1.5s` (`st15`) vs. `p-urgent wait < 2.0s` (`st39`)
  - `Sec 5.3 Evictions`: `8 evictions` (`st15`) vs. `8 evictions` (`st39`)
  - `Sec 5.4 Effective Sat`: `~32%–40% (~60pp badput gap)` (`st15`) vs. `~31%–41% (Excluded: 8, Unadmitted: 0)` (`st39`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 8-node (`32`-TPU) cluster, `withinClusterQueue: LowerPriority`. 8 `p-batch (100)` jobs (`30s` duration) occupy `100%` of the cluster from `t=0s` to `t=26s` (`87%` done), when 8 `p-urgent (1000)` jobs (`14s` duration) arrive at `t=26s`, preempt ALL 8 batch jobs, and finish at `t=40s` (window cut at `--duration 41s`).
- **Expected Runtime Behavior**: For `26s` out of `41s` (`63%` of wall-clock time), the cluster runs work that is killed right before completion. Only the 8 `p-urgent` jobs (`14s / 41s ≈ 34%` of wall-clock time) complete uninterrupted.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `93%–99% (raw occupancy)` |
| **Section `5.2` Queue Wait Time** | `p-urgent wait < 1.5s` |
| **Section `5.3` Evictions & Reclaims** | `8 evictions` |
| **Section `5.4` Effective Saturation** | `~32%–40% (~60pp badput gap)` |

### Detailed Metric Checkpoints

- `Section 5.1 (cq-badput)`: **`AVG ≈ 93%–99%`** (Kueue quota was allocated nearly 100% of the time).
- `Section 5.3 (Evictions)`: **`8`** evictions.
- `Section 5.4 (Effective Saturation)`: **`EFFECTIVE SAT ≈ 32%–40%`** (`448 / 1,312 TPU-s`), creating an intentional **`~55–60 percentage-point` gap** below `Section 5.1` that flags severe preemption badput.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ `Section 5.4` exceeding `45%` when evaluated with `--duration 41s` (would mean evicted workloads leaked into completed/active goodput).

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
python3 testscenarios/self-tests/generate_self_test_suite.py
python3 deploy/scripts/generate_scenario_readme.py --all
python3 testscenarios/self-tests/generate_self_test_suite.py --check
```
