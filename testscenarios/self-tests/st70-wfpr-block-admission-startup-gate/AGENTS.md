# Agent Execution & Cross-Examination Guide — `st70-wfpr-block-admission-startup-gate`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st70-wfpr-block-admission-startup-gate`
- **Category**: 14. waitForPodsReady Timeout Scaling (`--speedup` & `--duration`)
- **Title**: `waitForPodsReady.blockAdmission` Serializes Follower Admissions Behind `8s` Pod Startup Delay
- **Primary Verification Target**: `--wait-for-pods-ready-block-admission` holding follower `JobSet` admissions for `~7s` until `js-slow-startup-head` (`pod-startup-delay-seconds: 8`) reaches `PodsReady=True` despite `75%` free cluster quota
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st70` (`--namespace kueue-optimizer-chamber-st70`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16470` | Prometheus `19117` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st66-wfpr-timeout-evicts-stuck-pods-native`](../st66-wfpr-timeout-evicts-stuck-pods-native/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST70 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st70' (API port 16470, Prom port 19117):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st70-wfpr-block-admission-startup-gate --wait-for-pods-ready-timeout 25s --wait-for-pods-ready-block-admission --namespace kueue-optimizer-chamber-st70 --port 16470 > /tmp/st70.log 2>&1 &
PID_ST70=$!

# Simultaneously launch paired ST66 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st66' (API port 16466, Prom port 19113):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st66-wfpr-timeout-evicts-stuck-pods-native --wait-for-pods-ready-timeout 8s --wait-for-pods-ready-backoff-limit 0 --wait-for-pods-ready-backoff-base 2s --namespace kueue-optimizer-chamber-st66 --port 16466 > /tmp/st66.log 2>&1 &
PID_ST66=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST70" "$PID_ST66"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST70 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st70' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st70-wfpr-block-admission-startup-gate --wait-for-pods-ready-timeout 25s --wait-for-pods-ready-block-admission --namespace kueue-optimizer-chamber-st70 --port 16470 | tee /tmp/st70.log

# 2. Run paired ST66 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st66':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st66-wfpr-timeout-evicts-stuck-pods-native --wait-for-pods-ready-timeout 8s --wait-for-pods-ready-backoff-limit 0 --wait-for-pods-ready-backoff-base 2s --namespace kueue-optimizer-chamber-st66 --port 16466 | tee /tmp/st66.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st70-wfpr-block-admission-startup-gate --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st70`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st70-wfpr-block-admission-startup-gate` (`kueue-optimizer-chamber-st70`) ↔ [`st66-wfpr-timeout-evicts-stuck-pods-native`](../st66-wfpr-timeout-evicts-stuck-pods-native/AGENTS.md) (`kueue-optimizer-chamber-st66`) — Sub-Timeout `blockAdmission` Startup Gate (`st70`) vs. Over-Timeout Eviction (`st66`)

- **Controlled Variable Changed**: `8s` pod startup delay `< 25s` timeout with `--wait-for-pods-ready-block-admission` (`st70`) vs. `60s` startup delay `> 8s` timeout (`st66`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st66-wfpr-timeout-evicts-stuck-pods-native --wait-for-pods-ready-timeout 8s --wait-for-pods-ready-backoff-limit 0 --wait-for-pods-ready-backoff-base 2s --namespace kueue-optimizer-chamber-st66 --port 16466`
- **Cross-Examination Assertion**: `st70` records **`0` evictions** while holding followers for `~7s` (`Sec 5.2 P50 >= 5s` despite `75%` free quota), whereas `st66` records `1` eviction.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `~55%–75% (24/32 TPUs peak)` (`st70`) vs. `~85%–98% (quota reserved during stuck + healthy)` (`st66`)
  - `Sec 5.2 Wait Time`: `P50/P90 ~6.5s–8.5s (followers gated)` (`st70`) vs. `AVG ~5s–7s (healthy wait ~7.5s)` (`st66`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st70`) vs. `1 eviction (PodsReadyTimeout/Deactivated)` (`st66`)
  - `Sec 5.4 Effective Sat`: `~42%–58% (3 completed)` (`st70`) vs. `~48%–62% (only healthy 10s wave counts)` (`st66`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 8-node (`32`-TPU) cluster with `--wait-for-pods-ready-timeout 25s --wait-for-pods-ready-block-admission`. `js-slow-startup-head` (`2` pods = `8` TPUs, `pod-startup-delay-seconds: 8`, `10s` run) arrives at `t=0s`; two `2`-pod (`8`-TPU, `0s` delay, `10s` run) followers arrive at `t=1s` (`24` TPUs of quota sit free).
- **Expected Runtime Behavior**: `js-slow-startup-head` admits at `t=0s` (`8/32` TPUs) and stays `PodsReady=False` for `8s` (`< 25s` timeout, so `0` evictions). Because `blockAdmission=true`, Kueue blocks `js-follower-0` and `js-follower-1` until `t≈8s` when `js-slow-startup-head` transitions to `PodsReady=True`, after which both followers admit (`wait ≈ 7s`) and all `3` jobs succeed.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `~55%–75% (24/32 TPUs peak)` |
| **Section `5.2` Queue Wait Time** | `P50/P90 ~6.5s–8.5s (followers gated)` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `~42%–58% (3 completed)` |

### Detailed Metric Checkpoints

- `Section 5.2 (Wait Time)`: Follower jobs wait **`~6.5s–8.5s`** (`P50/P90 >= 5s` despite `24/32` idle TPUs!).
- `Section 5.3 (Total Evictions)`: Strictly **`0`** (`8s` startup delay `< 25s` timeout).
- `Execution Summary`: All **`3/3` `JobSet`s** succeed.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ Follower jobs admitting at `t=1s` (`wait < 2s`) while `js-slow-startup-head` is still `PodsReady=False` under `blockAdmission=true`.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
