# Agent Execution & Cross-Examination Guide — `st71-partial-exec-preempted-multiplier-replay`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st71-partial-exec-preempted-multiplier-replay`
- **Category**: 15. Interrupted-Workload Recording, Multiplier & True-Length Replay
- **Title**: Preemption-Interrupted Workloads (`PartialExecution = 8s` Only) Replayed With `--interrupted-duration-multiplier 2.5` (`20s`)
- **Primary Verification Target**: Workloads scraped from admission to preemption (`PartialExecution = 8.0s`, no `FullExecution`) replayed without preemption using `--interrupted-duration-multiplier 2.5` so each runs for `20.0s` (`> 8.0s` interrupted recording)
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st71` (`--namespace kueue-optimizer-chamber-st71`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16471` | Prometheus `19118` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st72-partial-exec-preempted-true-length-override`](../st72-partial-exec-preempted-true-length-override/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST71 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st71' (API port 16471, Prom port 19118):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st71-partial-exec-preempted-multiplier-replay --interrupted-duration-multiplier 2.5 --namespace kueue-optimizer-chamber-st71 --port 16471 > /tmp/st71.log 2>&1 &
PID_ST71=$!

# Simultaneously launch paired ST72 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st72' (API port 16472, Prom port 19119):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st72-partial-exec-preempted-true-length-override --interrupted-duration-override js-preempted-rec-2=24s,js-preempted-rec-3=24s --namespace kueue-optimizer-chamber-st72 --port 16472 > /tmp/st72.log 2>&1 &
PID_ST72=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST71" "$PID_ST72"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST71 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st71' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st71-partial-exec-preempted-multiplier-replay --interrupted-duration-multiplier 2.5 --namespace kueue-optimizer-chamber-st71 --port 16471 | tee /tmp/st71.log

# 2. Run paired ST72 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st72':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st72-partial-exec-preempted-true-length-override --interrupted-duration-override js-preempted-rec-2=24s,js-preempted-rec-3=24s --namespace kueue-optimizer-chamber-st72 --port 16472 | tee /tmp/st72.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st71-partial-exec-preempted-multiplier-replay --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st71`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st71-partial-exec-preempted-multiplier-replay` (`kueue-optimizer-chamber-st71`) ↔ [`st72-partial-exec-preempted-true-length-override`](../st72-partial-exec-preempted-true-length-override/AGENTS.md) (`kueue-optimizer-chamber-st72`) — Exact Interrupted-Workload Replay A/B Twin (`--interrupted-duration-multiplier 2.5` `st71` vs. Explicit True-Length Override `st72`)

- **Controlled Variable Changed**: Uniform `2.5x` multiplier (`8s -> 20s` in `st71`) vs. schema `trueDurationSeconds: 16s` + CLI `--interrupted-duration-override ...=24s` (`st72`) on the exact same `8.0s` preempted `PartialExecution`-only trace.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st72-partial-exec-preempted-true-length-override --interrupted-duration-override js-preempted-rec-2=24s,js-preempted-rec-3=24s --namespace kueue-optimizer-chamber-st72 --port 16472`
- **Cross-Examination Assertion**: Both replay strictly longer than the `8.0s` interrupted recording (`20s` uniform in `st71`, `Sec 5.4 ≈ 95%`; `16s` + `24s` two-tier in `st72`, `Sec 5.4 ≈ 83%`).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `~92%–100% (16/16 TPUs for 20s)` (`st71`) vs. `~80%–90% (16 TPUs 0-16s, 8 TPUs 16-24s)` (`st72`)
  - `Sec 5.2 Wait Time`: `< 1.5s` (`st71`) vs. `< 1.5s` (`st72`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st71`) vs. `0 evictions` (`st72`)
  - `Sec 5.4 Effective Sat`: `~90%–100% (20s > 8s partial)` (`st71`) vs. `~78%–88% (83.3% theoretical)` (`st72`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 4-node (`16`-TPU) cluster where 4 `1`-node (`4`-TPU) `JobSet`s (`js-preempted-rec-0..3`) have ONLY `Admitted` (`t=0s`) + `PartialExecution` (`endTime=8s`, `executionDurationSeconds=8.0`, `interruptionReason='Preempted'`, no `FullExecution`, no pod annotation fallback). Replayed with `--interrupted-duration-multiplier 2.5`.
- **Expected Runtime Behavior**: Because `FullExecution` is absent and no preemption occurs during replay, `testrunner.py` multiplies each job's observed `8.0s` partial duration by `2.5x` -> **`20.0s`** (`> 8.0s`). All 4 `JobSet`s run uninterrupted for `20s` across all `16` TPUs.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `~92%–100% (16/16 TPUs for 20s)` |
| **Section `5.2` Queue Wait Time** | `< 1.5s` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `~90%–100% (20s > 8s partial)` |

### Detailed Metric Checkpoints

- `Pre-Run Outline`: Reports `4 PartialExecution-only [Preempted=4] -> replay policy: CLI multiplier 2.5x` and `Execution Durations: min=20.0s, max=20.0s`.
- `Wall-Clock Duration`: **`~20s–23s`** (strictly longer than the `8s` interrupted recording!).
- `Section 5.4`: **`EFFECTIVE SAT ≈ 90%–100%`** over the `~20s` run (`4` completed).
- `cluster-scraper`: Records `FullExecution ≈ 20s` (`> 8s`) for all 4 jobs.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ Workloads terminating after only `8s` (`<= interrupted recording`) when replayed without preemption.
- ❌ Workloads falling back to `30s` default instead of `8.0s * 2.5 = 20.0s`.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
