# Agent Execution & Cross-Examination Guide — `st67-wfpr-timeout-scales-with-speedup`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st67-wfpr-timeout-scales-with-speedup`
- **Category**: 14. waitForPodsReady Timeout Scaling (`--speedup` & `--duration`)
- **Title**: `waitForPodsReady` Timeout Scaled by `--speedup 5` (`40s` Raw -> `8s` Wall — Exact Twin of st66)
- **Primary Verification Target**: Direct `5x` time-compressed twin of `st66`: `--speedup 5 --wait-for-pods-ready-timeout 40s` automatically compresses `waitForPodsReady.timeout` (`40s -> 8s`), `backoffBaseSeconds` (`10s -> 2s`), and `pod-startup-delay-seconds` (`300s -> 60s`) to match `st66` wall-for-wall
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st67` (`--namespace kueue-optimizer-chamber-st67`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16467` | Prometheus `19114` (collision-free across all 95 scenarios `st01`–`st95`)
- **Mandatory Paired Counterpart(s)**: [`st66-wfpr-timeout-evicts-stuck-pods-native`](../st66-wfpr-timeout-evicts-stuck-pods-native/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st95`) and non-overlapping port pair (`16401/19048` .. `16495/19142`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST67 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st67' (API port 16467, Prom port 19114):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st67-wfpr-timeout-scales-with-speedup --speedup 5 --wait-for-pods-ready-timeout 40s --wait-for-pods-ready-backoff-limit 0 --wait-for-pods-ready-backoff-base 10s --namespace kueue-optimizer-chamber-st67 --port 16467 > /tmp/st67.log 2>&1 &
PID_ST67=$!

# Simultaneously launch paired ST66 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st66' (API port 16466, Prom port 19113):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st66-wfpr-timeout-evicts-stuck-pods-native --wait-for-pods-ready-timeout 8s --wait-for-pods-ready-backoff-limit 0 --wait-for-pods-ready-backoff-base 2s --namespace kueue-optimizer-chamber-st66 --port 16466 > /tmp/st66.log 2>&1 &
PID_ST66=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST67" "$PID_ST66"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST67 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st67' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st67-wfpr-timeout-scales-with-speedup --speedup 5 --wait-for-pods-ready-timeout 40s --wait-for-pods-ready-backoff-limit 0 --wait-for-pods-ready-backoff-base 10s --namespace kueue-optimizer-chamber-st67 --port 16467 | tee /tmp/st67.log

# 2. Run paired ST66 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st66':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st66-wfpr-timeout-evicts-stuck-pods-native --wait-for-pods-ready-timeout 8s --wait-for-pods-ready-backoff-limit 0 --wait-for-pods-ready-backoff-base 2s --namespace kueue-optimizer-chamber-st66 --port 16466 | tee /tmp/st66.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st67-wfpr-timeout-scales-with-speedup --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st67`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st67-wfpr-timeout-scales-with-speedup` (`kueue-optimizer-chamber-st67`) ↔ [`st66-wfpr-timeout-evicts-stuck-pods-native`](../st66-wfpr-timeout-evicts-stuck-pods-native/AGENTS.md) (`kueue-optimizer-chamber-st66`) — Exact Time-Compression A/B Twin (`5.0x` Speedup `st67` vs. `1.0x` Native `st66`)

- **Controlled Variable Changed**: `--speedup 5 --wait-for-pods-ready-timeout 40s` (`st67`) vs. `--speedup 1 --wait-for-pods-ready-timeout 8s` (`st66`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st66-wfpr-timeout-evicts-stuck-pods-native --wait-for-pods-ready-timeout 8s --wait-for-pods-ready-backoff-limit 0 --wait-for-pods-ready-backoff-base 2s --namespace kueue-optimizer-chamber-st66 --port 16466`
- **Cross-Examination Assertion**: Wall-clock duration (`~18s–22s`), `Sec 5.3 = 1` eviction, and `Sec 5.4 ≈ 48%–62%` match `st66` within timing jitter.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `~85%–98% (matches st66)` (`st67`) vs. `~85%–98% (quota reserved during stuck + healthy)` (`st66`)
  - `Sec 5.2 Wait Time`: `AVG ~5s–7s wall (matches st66)` (`st67`) vs. `AVG ~5s–7s (healthy wait ~7.5s)` (`st66`)
  - `Sec 5.3 Evictions`: `1 eviction (matches st66)` (`st67`) vs. `1 eviction (PodsReadyTimeout/Deactivated)` (`st66`)
  - `Sec 5.4 Effective Sat`: `~48%–62% (matches st66)` (`st67`) vs. `~48%–62% (only healthy 10s wave counts)` (`st66`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: Identical 4-node (`16`-TPU) cluster as `st66` with all raw trace timings multiplied by `5x` (`timeout=40s`, `startup-delay=300s`, healthy arrival `5s`, healthy duration `50s`), replayed at `--speedup 5`.
- **Expected Runtime Behavior**: `resolve_wait_for_pods_ready_config` divides `40s / 5 = 8s` wall timeout (`10s / 5 = 2s` backoff) and `validation.py` divides `300s / 5 = 60s` pod startup delay and `50s / 5 = 10s` execution — reproducing `st66`'s `~8s` wall eviction and `~18s` total wall runtime instead of stalling `40s`!

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `~85%–98% (matches st66)` |
| **Section `5.2` Queue Wait Time** | `AVG ~5s–7s wall (matches st66)` |
| **Section `5.3` Evictions & Reclaims** | `1 eviction (matches st66)` |
| **Section `5.4` Effective Saturation** | `~48%–62% (matches st66)` |

### Detailed Metric Checkpoints

- `Chamber Banner`: Prints `WaitForPodsReady: [timeout=8s (raw 40s), backoffBase=2s, backoffLimit=0]`.
- `Section 5.3 (Total Evictions)`: **`1` eviction**, matching `st66`.
- `Wall-Clock Duration`: **`~18s–22s`** (matching `st66`, NOT `50s+` as would happen if `40s` timeout were left unscaled).

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ Wall-clock runtime exceeding `35s` (which would indicate `waitForPodsReady.timeout=40s` was NOT divided by `--speedup 5`).

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
