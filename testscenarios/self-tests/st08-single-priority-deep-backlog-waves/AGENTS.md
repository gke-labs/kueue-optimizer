# Agent Execution & Cross-Examination Guide — `st08-single-priority-deep-backlog-waves`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and re-run:
> ```bash
> python3 testscenarios/self-tests/generate_self_test_suite.py
> python3 deploy/scripts/generate_scenario_readme.py --all
> python3 testscenarios/self-tests/generate_self_test_suite.py --check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st08-single-priority-deep-backlog-waves`
- **Category**: 2. Queueing, Backfill & Topology
- **Title**: Deterministic 4-Wave Backlog Wait-Time Staircase (`0s, 12s, 24s, 36s`)
- **Primary Verification Target**: Histogram percentile accuracy (`P50`, `P90`, `P99`, `AVG`) in Section 5.2 across 4 discrete admission waves
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st08` (`--namespace kueue-optimizer-chamber-st08`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16408` | Prometheus `19055` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st09-multi-priority-non-preemptive-ordering`](../st09-multi-priority-non-preemptive-ordering/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST08 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st08' (API port 16408, Prom port 19055):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st08-single-priority-deep-backlog-waves --namespace kueue-optimizer-chamber-st08 --port 16408 > /tmp/st08.log 2>&1 &
PID_ST08=$!

# Simultaneously launch paired ST09 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st09' (API port 16409, Prom port 19056):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st09-multi-priority-non-preemptive-ordering --namespace kueue-optimizer-chamber-st09 --port 16409 > /tmp/st09.log 2>&1 &
PID_ST09=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST08" "$PID_ST09"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST08 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st08' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st08-single-priority-deep-backlog-waves --namespace kueue-optimizer-chamber-st08 --port 16408 | tee /tmp/st08.log

# 2. Run paired ST09 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st09':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st09-multi-priority-non-preemptive-ordering --namespace kueue-optimizer-chamber-st09 --port 16409 | tee /tmp/st09.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st08-single-priority-deep-backlog-waves --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st08`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st08-single-priority-deep-backlog-waves` (`kueue-optimizer-chamber-st08`) ↔ [`st09-multi-priority-non-preemptive-ordering`](../st09-multi-priority-non-preemptive-ordering/AGENTS.md) (`kueue-optimizer-chamber-st09`) — Single-Priority FIFO Waves (`st08`) vs. Multi-Priority Reordered Waves (`st09`) Pair

- **Controlled Variable Changed**: All 16 jobs share `p-standard (100)` in `st08`, whereas queued waves in `st09` carry ascending priorities `p-low (50)`, `p-high (500)`, `p-critical (1000)` in reverse arrival order.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st09-multi-priority-non-preemptive-ordering --namespace kueue-optimizer-chamber-st09 --port 16409`
- **Cross-Examination Assertion**: Both scenarios maintain `Sec 5.4 ≈ 91%–99%` and `Sec 5.3 Evictions == 0`, while `Sec 5.2` displays a single row (`AVG ~18s`, `P90 ~36s`) in `st08` vs. 3 priority rows ordered `Wait(p-critical) < Wait(p-high)` in `st09`.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `92%–100%` (`st08`) vs. `93%–100%` (`st09`)
  - `Sec 5.2 Wait Time`: `AVG ~18s, P90 ~36s` (`st08`) vs. `Wait(p-critical)<Wait(p-high)<Wait(p-low,queued)` (`st09`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st08`) vs. `0 evictions` (`st09`)
  - `Sec 5.4 Effective Sat`: `91%–99%` (`st08`) vs. `92%–99%` (`st09`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 8-node (`32`-TPU) cluster. 16 identical 2-node (`8`-TPU, `12s`) workloads with priority `p-standard (100)` submitted simultaneously at `t=0s` (4x cluster capacity).
- **Expected Runtime Behavior**: Exactly 4 jobs (`32` TPUs) execute per wave across 4 crisp waves: Wave 1 waits `0s`, Wave 2 waits `12s`, Wave 3 waits `24s`, Wave 4 waits `36s`. All 16 complete at `t≈48s`.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `92%–100%` |
| **Section `5.2` Queue Wait Time** | `AVG ~18s, P90 ~36s` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `91%–99%` |

### Detailed Metric Checkpoints

- `Section 5.1` & `Section 5.4`: `~92%–100%` continuous cluster utilization across all 4 waves.
- `Section 5.2 (Wait Time for p-standard)`: Mean wait time `AVG = (0 + 12 + 24 + 36) / 4 ≈ 18s`, `P50 ≈ 18s–24s`, `P90/P99 ≈ 36s`.
- `Section 5.3`: `0` evictions.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ More than 4 workloads (`>32` TPUs) running concurrently at any instant.
- ❌ `Section 5.2 AVG` deviating dramatically from `~18s` (within Prometheus histogram bucket interpolation bounds).

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
python3 testscenarios/self-tests/generate_self_test_suite.py
python3 deploy/scripts/generate_scenario_readme.py --all
python3 testscenarios/self-tests/generate_self_test_suite.py --check
```
