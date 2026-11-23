# Agent Execution & Cross-Examination Guide — `st74-partial-exec-t0-interrupted-and-scrape-cutoff`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st74-partial-exec-t0-interrupted-and-scrape-cutoff`
- **Category**: 15. Interrupted-Workload Recording, Multiplier & True-Length Replay
- **Title**: Interrupted `t0` `PodGroup` Multiplier Extension (`6s -> 15s`) + Mid-Flight `ScrapeEnded` Cutoff Recording
- **Primary Verification Target**: Extends `t0` pre-admitted pods whose `deletionTime=6s` came from preemption (`PartialExecution` only) to `15.0s` (`2.5x`) when replayed without preemption, and verifies `cluster-scraper` flushes `PartialExecution (ScrapeEnded)` at `--duration 22s`
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st74` (`--namespace kueue-optimizer-chamber-st74`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16474` | Prometheus `19121` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st71-partial-exec-preempted-multiplier-replay`](../st71-partial-exec-preempted-multiplier-replay/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST74 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st74' (API port 16474, Prom port 19121):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st74-partial-exec-t0-interrupted-and-scrape-cutoff --no-skip-initial-offset --duration 22s --interrupted-duration-multiplier 2.5 --namespace kueue-optimizer-chamber-st74 --port 16474 > /tmp/st74.log 2>&1 &
PID_ST74=$!

# Simultaneously launch paired ST71 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st71' (API port 16471, Prom port 19118):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st71-partial-exec-preempted-multiplier-replay --interrupted-duration-multiplier 2.5 --namespace kueue-optimizer-chamber-st71 --port 16471 > /tmp/st71.log 2>&1 &
PID_ST71=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST74" "$PID_ST71"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST74 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st74' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st74-partial-exec-t0-interrupted-and-scrape-cutoff --no-skip-initial-offset --duration 22s --interrupted-duration-multiplier 2.5 --namespace kueue-optimizer-chamber-st74 --port 16474 | tee /tmp/st74.log

# 2. Run paired ST71 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st71':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st71-partial-exec-preempted-multiplier-replay --interrupted-duration-multiplier 2.5 --namespace kueue-optimizer-chamber-st71 --port 16471 | tee /tmp/st71.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st74-partial-exec-t0-interrupted-and-scrape-cutoff --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st74`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st74-partial-exec-t0-interrupted-and-scrape-cutoff` (`kueue-optimizer-chamber-st74`) ↔ [`st71-partial-exec-preempted-multiplier-replay`](../st71-partial-exec-preempted-multiplier-replay/AGENTS.md) (`kueue-optimizer-chamber-st71`) — Interrupted `t0` `PodGroup` Extension + `ScrapeEnded` Flush (`st74`) vs. Arriving `PartialExecution` Multiplier (`st71`)

- **Controlled Variable Changed**: Pre-admitted `t0` `PodGroup`s (`6s -> 15s` at `2.5x`) + mid-flight `--duration 22s` cutoff (`st74`) vs. `NotAdmitted` arriving jobs (`st71`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st71-partial-exec-preempted-multiplier-replay --interrupted-duration-multiplier 2.5 --namespace kueue-optimizer-chamber-st71 --port 16471`
- **Cross-Examination Assertion**: `st74` proves `t0` interrupted-only pods extend past `6s` to `15s` when unpreempted AND `cluster-scraper` flushes `ScrapeEnded` `PartialExecution` for all `4` jobs still running at `t=22s`.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `~75%–88%` (`st74`) vs. `~92%–100% (16/16 TPUs for 20s)` (`st71`)
  - `Sec 5.2 Wait Time`: `< 1.5s` (`st74`) vs. `< 1.5s` (`st71`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st74`) vs. `0 evictions` (`st71`)
  - `Sec 5.4 Effective Sat`: `~72%–86% (4 completed @15s + 4 active)` (`st74`) vs. `~90%–100% (20s > 8s partial)` (`st71`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 8-node (`32`-TPU) cluster: 4 `t0` `PodGroup`s (`16` TPUs) recorded `PartialExecution = 6.0s` (`Preempted`, `deletionTime = t0+6s`, no `FullExecution`); 4 arriving `JobSet`s (`16` TPUs, `32s`) enter at `t=3s`. Run with `--no-skip-initial-offset --duration 22s --interrupted-duration-multiplier 2.5`.
- **Expected Runtime Behavior**: Because the `4` `t0` workloads are NOT preempted during replay, `loader.py` extends their `t0` pod lifetimes from `6s` to **`6.0s * 2.5 = 15.0s`**, so they complete at `t≈15s` (`COMPLETED ONLY = 4`). Meanwhile, the `4` `t=3s` `JobSet`s (`32s`) are still running at `t=22s` (`ACTIVE AT END = 4`), and `cluster-scraper` flushes `PartialExecution` (`interruptionReason: 'ScrapeEnded'`, `~19s`) for all 4 in-flight jobs on shutdown!

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `~75%–88%` |
| **Section `5.2` Queue Wait Time** | `< 1.5s` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `~72%–86% (4 completed @15s + 4 active)` |

### Detailed Metric Checkpoints

- `Section 5.4`: Additive split of `4` completed `t0` `PodGroup`s (`15s` each) + `4` active-at-end `JobSet`s (`~19s` each) -> **`EFFECTIVE SAT ≈ 72%–86%`** (`(4*4*15 + 4*4*19) / (32*22) = 544 / 704 = 77.3%`, vs `59%` if `t0` pods had died early at `6s`!).
- `cluster-scraper`: Emits `4` `FullExecution` (`~15s`) + `4` `PartialExecution` (`interruptionReason: 'ScrapeEnded'`) records.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ `t0` interrupted-only pods terminating at `t=6s` instead of `t≈15s` (`2.5x`).
- ❌ `cluster-scraper` dropping in-flight `Admitted` workloads without a `ScrapeEnded` `PartialExecution` at shutdown.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
