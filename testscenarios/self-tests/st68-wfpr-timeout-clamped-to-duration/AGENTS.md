# Agent Execution & Cross-Examination Guide — `st68-wfpr-timeout-clamped-to-duration`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and re-run:
> ```bash
> python3 testscenarios/self-tests/generate_self_test_suite.py
> python3 deploy/scripts/generate_scenario_readme.py --all
> python3 testscenarios/self-tests/generate_self_test_suite.py --check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st68-wfpr-timeout-clamped-to-duration`
- **Category**: 14. waitForPodsReady Timeout Scaling (`--speedup` & `--duration`)
- **Title**: `waitForPodsReady` Long `10m` Timeout Clamped to `--duration 24s` (`effective = 12s`)
- **Primary Verification Target**: Automatic clamping of `--wait-for-pods-ready-timeout 10m` to `50%` of `--duration 24s` (`12s`) so `PodsReadyTimeout` fires inside bounded chamber runs and unblocks healthy peers before test cutoff
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st68` (`--namespace kueue-optimizer-chamber-st68`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16468` | Prometheus `19115` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st66-wfpr-timeout-evicts-stuck-pods-native`](../st66-wfpr-timeout-evicts-stuck-pods-native/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST68 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st68' (API port 16468, Prom port 19115):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st68-wfpr-timeout-clamped-to-duration --duration 24s --wait-for-pods-ready-timeout 10m --wait-for-pods-ready-backoff-limit 0 --namespace kueue-optimizer-chamber-st68 --port 16468 > /tmp/st68.log 2>&1 &
PID_ST68=$!

# Simultaneously launch paired ST66 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st66' (API port 16466, Prom port 19113):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st66-wfpr-timeout-evicts-stuck-pods-native --wait-for-pods-ready-timeout 8s --wait-for-pods-ready-backoff-limit 0 --wait-for-pods-ready-backoff-base 2s --namespace kueue-optimizer-chamber-st66 --port 16466 > /tmp/st66.log 2>&1 &
PID_ST66=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST68" "$PID_ST66"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST68 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st68' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st68-wfpr-timeout-clamped-to-duration --duration 24s --wait-for-pods-ready-timeout 10m --wait-for-pods-ready-backoff-limit 0 --namespace kueue-optimizer-chamber-st68 --port 16468 | tee /tmp/st68.log

# 2. Run paired ST66 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st66':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st66-wfpr-timeout-evicts-stuck-pods-native --wait-for-pods-ready-timeout 8s --wait-for-pods-ready-backoff-limit 0 --wait-for-pods-ready-backoff-base 2s --namespace kueue-optimizer-chamber-st66 --port 16466 | tee /tmp/st66.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st68-wfpr-timeout-clamped-to-duration --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st68`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st68-wfpr-timeout-clamped-to-duration` (`kueue-optimizer-chamber-st68`) ↔ [`st66-wfpr-timeout-evicts-stuck-pods-native`](../st66-wfpr-timeout-evicts-stuck-pods-native/AGENTS.md) (`kueue-optimizer-chamber-st66`) — `--duration`-Clamped `waitForPodsReady` Timeout (`st68`) vs. Native `8s` Timeout (`st66`)

- **Controlled Variable Changed**: `--wait-for-pods-ready-timeout 10m` clamped by `--duration 24s` to `12s` (`st68`) vs. native `8s` (`st66`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st66-wfpr-timeout-evicts-stuck-pods-native --wait-for-pods-ready-timeout 8s --wait-for-pods-ready-backoff-limit 0 --wait-for-pods-ready-backoff-base 2s --namespace kueue-optimizer-chamber-st66 --port 16466`
- **Cross-Examination Assertion**: `st68` banner shows `timeout=12s (raw 600s), clamped-to-duration`, healthy peers wait `~11s–12s` (`+4s` vs. `st66`), and `Sec 5.3 = 1`.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `~85%–98%` (`st68`) vs. `~85%–98% (quota reserved during stuck + healthy)` (`st66`)
  - `Sec 5.2 Wait Time`: `healthy wait ~11s–12s` (`st68`) vs. `AVG ~5s–7s (healthy wait ~7.5s)` (`st66`)
  - `Sec 5.3 Evictions`: `1 eviction (clamped 12s timeout)` (`st68`) vs. `1 eviction (PodsReadyTimeout/Deactivated)` (`st66`)
  - `Sec 5.4 Effective Sat`: `~35%–50% (2 healthy 8s jobs)` (`st68`) vs. `~48%–62% (only healthy 10s wave counts)` (`st66`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 4-node (`16`-TPU) cluster run with `--duration 24s --wait-for-pods-ready-timeout 10m --wait-for-pods-ready-backoff-limit 0`. `js-stuck-unready` (`pod-never-ready: true`, `16` TPUs) arrives at `t=0s`; two healthy `2`-pod (`8`-TPU, `8s`) `JobSet`s arrive at `t=1s`.
- **Expected Runtime Behavior**: `resolve_wait_for_pods_ready_config` clamps `600s` (`10m`) down to `0.5 * 24s = 12s` (`clamped-to-duration`). At `t≈12s`, Kueue evicts and deactivates `js-stuck-unready`, and both `8s` healthy `JobSet`s admit at `t≈12s` and finish at `t≈20s` (well inside the `24s` test budget).

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `~85%–98%` |
| **Section `5.2` Queue Wait Time** | `healthy wait ~11s–12s` |
| **Section `5.3` Evictions & Reclaims** | `1 eviction (clamped 12s timeout)` |
| **Section `5.4` Effective Saturation** | `~35%–50% (2 healthy 8s jobs)` |

### Detailed Metric Checkpoints

- `Chamber Banner`: Prints `WaitForPodsReady: [timeout=12s (raw 600s), clamped-to-duration, backoffLimit=0]`.
- `Section 5.3 (Total Evictions)`: **`1` eviction** (would be `0` if `10m` were left unclamped over `24s`).
- `Execution Summary`: Both healthy `JobSet`s finish (`workloads finished = 3` including deactivated stuck workload).

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ `Section 5.3` reporting `0` evictions or `Section 5.4` reporting `0.0%` goodput over the `24s` window.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
python3 testscenarios/self-tests/generate_self_test_suite.py
python3 deploy/scripts/generate_scenario_readme.py --all
python3 testscenarios/self-tests/generate_self_test_suite.py --check
```
