# Agent Execution & Cross-Examination Guide — `st73-partial-exec-node-failure-multiplier-replay`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and re-run:
> ```bash
> python3 testscenarios/self-tests/generate_self_test_suite.py
> python3 deploy/scripts/generate_scenario_readme.py --all
> python3 testscenarios/self-tests/generate_self_test_suite.py --check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st73-partial-exec-node-failure-multiplier-replay`
- **Category**: 15. Interrupted-Workload Recording, Multiplier & True-Length Replay
- **Title**: Node-Failure Interrupted TAS Workloads (`PartialExecution = 7s`, `missingDurationMultiplier = 2.5`) Replayed Alongside `FullExecution` Peers
- **Primary Verification Target**: Schema `interruptionReason: 'NodeFailure'` + `missingDurationMultiplier: 2.5` scaling interrupted TAS `JobSet`s from `7.0s -> 17.5s` while leaving `FullExecution = 14.0s` peer `JobSet`s untouched
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st73` (`--namespace kueue-optimizer-chamber-st73`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16473` | Prometheus `19120` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st71-partial-exec-preempted-multiplier-replay`](../st71-partial-exec-preempted-multiplier-replay/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST73 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st73' (API port 16473, Prom port 19120):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st73-partial-exec-node-failure-multiplier-replay --namespace kueue-optimizer-chamber-st73 --port 16473 > /tmp/st73.log 2>&1 &
PID_ST73=$!

# Simultaneously launch paired ST71 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st71' (API port 16471, Prom port 19118):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st71-partial-exec-preempted-multiplier-replay --interrupted-duration-multiplier 2.5 --namespace kueue-optimizer-chamber-st71 --port 16471 > /tmp/st71.log 2>&1 &
PID_ST71=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST73" "$PID_ST71"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST73 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st73' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st73-partial-exec-node-failure-multiplier-replay --namespace kueue-optimizer-chamber-st73 --port 16473 | tee /tmp/st73.log

# 2. Run paired ST71 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st71':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st71-partial-exec-preempted-multiplier-replay --interrupted-duration-multiplier 2.5 --namespace kueue-optimizer-chamber-st71 --port 16471 | tee /tmp/st71.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st73-partial-exec-node-failure-multiplier-replay --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st73`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st73-partial-exec-node-failure-multiplier-replay` (`kueue-optimizer-chamber-st73`) ↔ [`st71-partial-exec-preempted-multiplier-replay`](../st71-partial-exec-preempted-multiplier-replay/AGENTS.md) (`kueue-optimizer-chamber-st71`) — Schema `NodeFailure` `missingDurationMultiplier` + `FullExecution` Mix (`st73`) vs. CLI `Preempted` Multiplier (`st71`)

- **Controlled Variable Changed**: `NodeFailure` `PartialExecution` (`7s * 2.5 = 17.5s`) mixed with `FullExecution` (`14s`) peers (`st73`) vs. all-`Preempted` `PartialExecution` (`8s * 2.5 = 20s`, `st71`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st71-partial-exec-preempted-multiplier-replay --interrupted-duration-multiplier 2.5 --namespace kueue-optimizer-chamber-st71 --port 16471`
- **Cross-Examination Assertion**: In `st73`, `FullExecution` peers stay at exact `14.0s` while `NodeFailure` interrupted jobs expand to `17.5s` (`> 7.0s`).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `~86%–96%` (`st73`) vs. `~92%–100% (16/16 TPUs for 20s)` (`st71`)
  - `Sec 5.2 Wait Time`: `< 1.5s` (`st73`) vs. `< 1.5s` (`st71`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st73`) vs. `0 evictions` (`st71`)
  - `Sec 5.4 Effective Sat`: `~84%–95% (90.0% theoretical)` (`st73`) vs. `~90%–100% (20s > 8s partial)` (`st71`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 8-node (`32`-TPU) 7-level TAS cluster with 4 `2`-node (`8`-TPU, `2x2x2-id`) `JobSet`s at `t=0s`: `js-nf-interrupted-0..1` have ONLY `PartialExecution` (`7.0s`, `interruptionReason='NodeFailure'`, `missingDurationMultiplier=2.5`), while `js-full-peer-0..1` have `FullExecution` (`14.0s`).
- **Expected Runtime Behavior**: On the healthy replay cluster, `js-nf-interrupted-0..1` run for **`7.0s * 2.5 = 17.5s`** (`> 7.0s`), while `js-full-peer-0..1` run for their exact **`14.0s`** `FullExecution` duration.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `~86%–96%` |
| **Section `5.2` Queue Wait Time** | `< 1.5s` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `~84%–95% (90.0% theoretical)` |

### Detailed Metric Checkpoints

- `Pre-Run Outline`: `Execution Durations: min=14.0s, max=17.5s` (`2 PartialExecution-only [NodeFailure=2]`).
- `Section 5.3`: `0` evictions on healthy replay cluster.
- `Section 5.4`: **`EFFECTIVE SAT ≈ 84%–95%`** (`(2*14 + 2*17.5) / (4*17.5) = 63 / 70 = 90.0%`).

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ `missingDurationMultiplier` altering `FullExecution` workloads (`js-full-peer-0..1` must stay `14.0s`).
- ❌ `js-nf-interrupted-0..1` terminating at `7.0s`.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
python3 testscenarios/self-tests/generate_self_test_suite.py
python3 deploy/scripts/generate_scenario_readme.py --all
python3 testscenarios/self-tests/generate_self_test_suite.py --check
```
