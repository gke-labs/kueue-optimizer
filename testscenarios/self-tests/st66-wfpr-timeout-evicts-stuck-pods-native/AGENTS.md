# Agent Execution & Cross-Examination Guide — `st66-wfpr-timeout-evicts-stuck-pods-native`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and re-run:
> ```bash
> python3 testscenarios/self-tests/generate_self_test_suite.py
> python3 deploy/scripts/generate_scenario_readme.py --all
> python3 testscenarios/self-tests/generate_self_test_suite.py --check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st66-wfpr-timeout-evicts-stuck-pods-native`
- **Category**: 14. waitForPodsReady Timeout Scaling (`--speedup` & `--duration`)
- **Title**: `waitForPodsReady` Timeout Evicts Unready Stuck Workload at Native `1.0x` Speedup
- **Primary Verification Target**: Baseline `waitForPodsReady.timeout = 8s` (`--wait-for-pods-ready-backoff-limit 0`) evicting & deactivating a slow-starting `60s`-delay `JobSet` at `t≈8s` so healthy queued `JobSet`s admit and complete
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st66` (`--namespace kueue-optimizer-chamber-st66`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16466` | Prometheus `19113` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st67-wfpr-timeout-scales-with-speedup`](../st67-wfpr-timeout-scales-with-speedup/AGENTS.md), [`st68-wfpr-timeout-clamped-to-duration`](../st68-wfpr-timeout-clamped-to-duration/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST66 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st66' (API port 16466, Prom port 19113):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st66-wfpr-timeout-evicts-stuck-pods-native --wait-for-pods-ready-timeout 8s --wait-for-pods-ready-backoff-limit 0 --wait-for-pods-ready-backoff-base 2s --namespace kueue-optimizer-chamber-st66 --port 16466 > /tmp/st66.log 2>&1 &
PID_ST66=$!

# Simultaneously launch paired ST67 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st67' (API port 16467, Prom port 19114):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st67-wfpr-timeout-scales-with-speedup --speedup 5 --wait-for-pods-ready-timeout 40s --wait-for-pods-ready-backoff-limit 0 --wait-for-pods-ready-backoff-base 10s --namespace kueue-optimizer-chamber-st67 --port 16467 > /tmp/st67.log 2>&1 &
PID_ST67=$!

# Simultaneously launch paired ST68 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st68' (API port 16468, Prom port 19115):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st68-wfpr-timeout-clamped-to-duration --duration 24s --wait-for-pods-ready-timeout 10m --wait-for-pods-ready-backoff-limit 0 --namespace kueue-optimizer-chamber-st68 --port 16468 > /tmp/st68.log 2>&1 &
PID_ST68=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST66" "$PID_ST67" "$PID_ST68"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST66 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st66' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st66-wfpr-timeout-evicts-stuck-pods-native --wait-for-pods-ready-timeout 8s --wait-for-pods-ready-backoff-limit 0 --wait-for-pods-ready-backoff-base 2s --namespace kueue-optimizer-chamber-st66 --port 16466 | tee /tmp/st66.log

# 2. Run paired ST67 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st67':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st67-wfpr-timeout-scales-with-speedup --speedup 5 --wait-for-pods-ready-timeout 40s --wait-for-pods-ready-backoff-limit 0 --wait-for-pods-ready-backoff-base 10s --namespace kueue-optimizer-chamber-st67 --port 16467 | tee /tmp/st67.log

# 3. Run paired ST68 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st68':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st68-wfpr-timeout-clamped-to-duration --duration 24s --wait-for-pods-ready-timeout 10m --wait-for-pods-ready-backoff-limit 0 --namespace kueue-optimizer-chamber-st68 --port 16468 | tee /tmp/st68.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st66-wfpr-timeout-evicts-stuck-pods-native --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st66`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st66-wfpr-timeout-evicts-stuck-pods-native` (`kueue-optimizer-chamber-st66`) ↔ [`st67-wfpr-timeout-scales-with-speedup`](../st67-wfpr-timeout-scales-with-speedup/AGENTS.md) (`kueue-optimizer-chamber-st67`) — Exact Time-Compression A/B Twin (`1.0x` Native `timeout=8s` `st66` vs. `5.0x` Speedup `timeout=40s -> 8s` `st67`)

- **Controlled Variable Changed**: All raw trace timings and `--wait-for-pods-ready-timeout` scaled by `5x` (`8s -> 40s`) and replayed with `--speedup 5` in `st67`.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st67-wfpr-timeout-scales-with-speedup --speedup 5 --wait-for-pods-ready-timeout 40s --wait-for-pods-ready-backoff-limit 0 --wait-for-pods-ready-backoff-base 10s --namespace kueue-optimizer-chamber-st67 --port 16467`
- **Cross-Examination Assertion**: Both `st66` and `st67` evict the stuck head `JobSet` at **`~8s` wall clock**, admit both healthy peers at `~8s` wall, and finish in **`~18s–22s` wall clock** with `1` eviction.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `~85%–98% (quota reserved during stuck + healthy)` (`st66`) vs. `~85%–98% (matches st66)` (`st67`)
  - `Sec 5.2 Wait Time`: `AVG ~5s–7s (healthy wait ~7.5s)` (`st66`) vs. `AVG ~5s–7s wall (matches st66)` (`st67`)
  - `Sec 5.3 Evictions`: `1 eviction (PodsReadyTimeout/Deactivated)` (`st66`) vs. `1 eviction (matches st66)` (`st67`)
  - `Sec 5.4 Effective Sat`: `~48%–62% (only healthy 10s wave counts)` (`st66`) vs. `~48%–62% (matches st66)` (`st67`)

### Pair: `st66-wfpr-timeout-evicts-stuck-pods-native` (`kueue-optimizer-chamber-st66`) ↔ [`st68-wfpr-timeout-clamped-to-duration`](../st68-wfpr-timeout-clamped-to-duration/AGENTS.md) (`kueue-optimizer-chamber-st68`) — Explicit Short Timeout (`st66`) vs. `--duration`-Clamped `10m -> 12s` Timeout (`st68`)

- **Controlled Variable Changed**: Raw `--wait-for-pods-ready-timeout 8s` (`st66`) vs. `--wait-for-pods-ready-timeout 10m --duration 24s` auto-clamped to `12s` (`st68`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st68-wfpr-timeout-clamped-to-duration --duration 24s --wait-for-pods-ready-timeout 10m --wait-for-pods-ready-backoff-limit 0 --namespace kueue-optimizer-chamber-st68 --port 16468`
- **Cross-Examination Assertion**: Both evict the stuck head job (`~8s` in `st66` vs. `~12s` in `st68`) and complete both healthy peer jobs within the run window.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `~85%–98% (quota reserved during stuck + healthy)` (`st66`) vs. `~85%–98%` (`st68`)
  - `Sec 5.2 Wait Time`: `AVG ~5s–7s (healthy wait ~7.5s)` (`st66`) vs. `healthy wait ~11s–12s` (`st68`)
  - `Sec 5.3 Evictions`: `1 eviction (PodsReadyTimeout/Deactivated)` (`st66`) vs. `1 eviction (clamped 12s timeout)` (`st68`)
  - `Sec 5.4 Effective Sat`: `~48%–62% (only healthy 10s wave counts)` (`st66`) vs. `~35%–50% (2 healthy 8s jobs)` (`st68`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 4-node (`16`-TPU) cluster with `--wait-for-pods-ready-timeout 8s --wait-for-pods-ready-backoff-limit 0`. `js-stuck-1x` (`4` pods = `16` TPUs, `pod-startup-delay-seconds: 60`) arrives at `t=0s`; two healthy `2`-pod (`8`-TPU, `10s`) `JobSet`s arrive at `t=1s`.
- **Expected Runtime Behavior**: `js-stuck-1x` reserves `16/16` TPUs at `t=0s` but stays `PodsReady=False` past `8s`. At `t≈8s`, Kueue evicts and deactivates `js-stuck-1x` (`RequeuingLimitExceeded` / `PodsReadyTimeout`), freeing all `16` TPUs so `js-healthy-1x-0` and `js-healthy-1x-1` admit at `t≈8s` (`wait ≈ 7s–9s`) and finish at `t≈18s`.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `~85%–98% (quota reserved during stuck + healthy)` |
| **Section `5.2` Queue Wait Time** | `AVG ~5s–7s (healthy wait ~7.5s)` |
| **Section `5.3` Evictions & Reclaims** | `1 eviction (PodsReadyTimeout/Deactivated)` |
| **Section `5.4` Effective Saturation** | `~48%–62% (only healthy 10s wave counts)` |

### Detailed Metric Checkpoints

- `Section 5.3 (Total Evictions)`: **`1` eviction** (`Deactivated` / `PodsReadyTimeout`).
- `Section 5.2`: Healthy jobs wait `~7s–9s` (`AVG` across all 3 admissions `~5s–6.5s`).
- `cluster-scraper`: Records `PartialExecution` (`interruptionReason: 'PodsReadyTimeout'`, `preempted: false`) for `js-stuck-1x` and `FullExecution` (`~10s`) for both healthy jobs.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ `js-stuck-1x` holding `16/16` TPUs for `60s` instead of getting evicted at `~8s`.
- ❌ `run_scenario_replay` hanging forever waiting for deactivated `js-stuck-1x` (`spec.active=false`).

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
python3 testscenarios/self-tests/generate_self_test_suite.py
python3 deploy/scripts/generate_scenario_readme.py --all
python3 testscenarios/self-tests/generate_self_test_suite.py --check
```
