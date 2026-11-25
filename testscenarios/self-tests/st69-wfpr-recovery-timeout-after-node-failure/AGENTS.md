# Agent Execution & Cross-Examination Guide — `st69-wfpr-recovery-timeout-after-node-failure`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st69-wfpr-recovery-timeout-after-node-failure`
- **Category**: 14. waitForPodsReady Timeout Scaling (`--speedup` & `--duration`)
- **Title**: `waitForPodsReady.recoveryTimeout` Eviction After Mid-Run Node Failure Frees Healthy Nodes for Peers
- **Primary Verification Target**: `waitForPodsReady.recoveryTimeout = 6s` evicting a running `4`-node `Job` whose replacement pod stalls `Pending` after `st-node-00` fails at `t=4s`, freeing the `3` healthy nodes for `1`-node peer jobs before `st-node-00` recovers at `t=18s`
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st69` (`--namespace kueue-optimizer-chamber-st69`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16469` | Prometheus `19116` (collision-free across all 95 scenarios `st01`–`st95`)
- **Mandatory Paired Counterpart(s)**: [`st66-wfpr-timeout-evicts-stuck-pods-native`](../st66-wfpr-timeout-evicts-stuck-pods-native/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st95`) and non-overlapping port pair (`16401/19048` .. `16495/19142`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST69 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st69' (API port 16469, Prom port 19116):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st69-wfpr-recovery-timeout-after-node-failure --wait-for-pods-ready-timeout 20s --wait-for-pods-ready-recovery-timeout 6s --wait-for-pods-ready-backoff-limit 2 --wait-for-pods-ready-backoff-base 2s --namespace kueue-optimizer-chamber-st69 --port 16469 > /tmp/st69.log 2>&1 &
PID_ST69=$!

# Simultaneously launch paired ST66 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st66' (API port 16466, Prom port 19113):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st66-wfpr-timeout-evicts-stuck-pods-native --wait-for-pods-ready-timeout 8s --wait-for-pods-ready-backoff-limit 0 --wait-for-pods-ready-backoff-base 2s --namespace kueue-optimizer-chamber-st66 --port 16466 > /tmp/st66.log 2>&1 &
PID_ST66=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST69" "$PID_ST66"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST69 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st69' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st69-wfpr-recovery-timeout-after-node-failure --wait-for-pods-ready-timeout 20s --wait-for-pods-ready-recovery-timeout 6s --wait-for-pods-ready-backoff-limit 2 --wait-for-pods-ready-backoff-base 2s --namespace kueue-optimizer-chamber-st69 --port 16469 | tee /tmp/st69.log

# 2. Run paired ST66 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st66':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st66-wfpr-timeout-evicts-stuck-pods-native --wait-for-pods-ready-timeout 8s --wait-for-pods-ready-backoff-limit 0 --wait-for-pods-ready-backoff-base 2s --namespace kueue-optimizer-chamber-st66 --port 16466 | tee /tmp/st66.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st69-wfpr-recovery-timeout-after-node-failure --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st69`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st69-wfpr-recovery-timeout-after-node-failure` (`kueue-optimizer-chamber-st69`) ↔ [`st66-wfpr-timeout-evicts-stuck-pods-native`](../st66-wfpr-timeout-evicts-stuck-pods-native/AGENTS.md) (`kueue-optimizer-chamber-st66`) — Post-Running `recoveryTimeout` Eviction (`st69`) vs. Initial-Startup `timeout` Eviction (`st66`)

- **Controlled Variable Changed**: Workload reaches `PodsReady=True` first and loses 1 node at `t=4s` (`recoveryTimeout=6s` in `st69`) vs. never reaching `PodsReady=True` (`timeout=8s` in `st66`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st66-wfpr-timeout-evicts-stuck-pods-native --wait-for-pods-ready-timeout 8s --wait-for-pods-ready-backoff-limit 0 --wait-for-pods-ready-backoff-base 2s --namespace kueue-optimizer-chamber-st66 --port 16466`
- **Cross-Examination Assertion**: In `st69`, `recoveryTimeout` evicts `job-4n-recover` at `t≈10s` (`4s + 6s`), unblocking `2` `1`-node peers before `st-node-00` recovers at `t=18s` so all `3/3` jobs succeed.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `~65%–88%` (`st69`) vs. `~85%–98% (quota reserved during stuck + healthy)` (`st66`)
  - `Sec 5.2 Wait Time`: `peers admit right after ~10s recoveryTimeout` (`st69`) vs. `AVG ~5s–7s (healthy wait ~7.5s)` (`st66`)
  - `Sec 5.3 Evictions`: `1–2 evictions (PodsReadyTimeout)` (`st69`) vs. `1 eviction (PodsReadyTimeout/Deactivated)` (`st66`)
  - `Sec 5.4 Effective Sat`: `~40%–70% (all 3 jobs succeed)` (`st69`) vs. `~48%–62% (only healthy 10s wave counts)` (`st66`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 4-node (`16`-TPU) non-TAS cluster with `timeout=20s`, `recoveryTimeout=6s`, `backoffLimit=2`, `backoffBase=2s`. `job-4n-recover` (`4` pods = `16` TPUs, `12s`) reaches `PodsReady=True` at `t=0s`; two `1`-node (`4`-TPU, `7s`) peer `Job`s arrive at `t=1s`. `st-node-00` fails at `t=4s` and recovers at `t=18s`.
- **Expected Runtime Behavior**: At `t=4s`, `st-node-00` fails, killing 1 pod of `job-4n-recover`; its replacement pod stays `Pending` (`3` healthy nodes < `4` needed), flipping `PodsReady` to `False`. At `t≈10s` (`4s + 6s recoveryTimeout`), Kueue evicts `job-4n-recover` (`PodsReadyTimeout`), allowing both `1`-node peer `Job`s to run (`10s–17s`) on healthy nodes! Once `st-node-00` recovers at `t=18s`, `job-4n-recover` re-admits across all 4 nodes and succeeds.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `~65%–88%` |
| **Section `5.2` Queue Wait Time** | `peers admit right after ~10s recoveryTimeout` |
| **Section `5.3` Evictions & Reclaims** | `1–2 evictions (PodsReadyTimeout)` |
| **Section `5.4` Effective Saturation** | `~40%–70% (all 3 jobs succeed)` |

### Detailed Metric Checkpoints

- `Section 5.3 (Total Evictions)`: **`>= 1` eviction** (`PodsReadyTimeout` triggered by `recoveryTimeout`).
- `Execution Summary`: All **`3/3` `Job`s** succeed (`workloads finished = 3`).
- `cluster-scraper`: Captures `st-node-00` failure (`t=4s`), `PodsReadyTimeout` `PartialExecution` (`t≈10s`), `st-node-00` recovery (`t=18s`), and `3` `FullExecution` records.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ `job-4n-recover` holding `3` healthy nodes idle from `t=4s` to `t=18s` without `recoveryTimeout` firing.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
