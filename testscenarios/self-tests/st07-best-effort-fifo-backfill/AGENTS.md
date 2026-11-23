# Agent Execution & Cross-Examination Guide — `st07-best-effort-fifo-backfill`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st07-best-effort-fifo-backfill`
- **Category**: 2. Queueing, Backfill & Topology
- **Title**: BestEffortFIFO Out-of-Order Backfill Eliminating Head-of-Line Blocking (Twin of st06)
- **Primary Verification Target**: Direct A/B twin of `st06` with `queueingStrategy: BestEffortFIFO` proving out-of-order backfill
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st07` (`--namespace kueue-optimizer-chamber-st07`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16407` | Prometheus `19054` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st06-strict-fifo-hol-blocking`](../st06-strict-fifo-hol-blocking/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST07 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st07' (API port 16407, Prom port 19054):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st07-best-effort-fifo-backfill --namespace kueue-optimizer-chamber-st07 --port 16407 > /tmp/st07.log 2>&1 &
PID_ST07=$!

# Simultaneously launch paired ST06 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st06' (API port 16406, Prom port 19053):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st06-strict-fifo-hol-blocking --namespace kueue-optimizer-chamber-st06 --port 16406 > /tmp/st06.log 2>&1 &
PID_ST06=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST07" "$PID_ST06"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST07 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st07' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st07-best-effort-fifo-backfill --namespace kueue-optimizer-chamber-st07 --port 16407 | tee /tmp/st07.log

# 2. Run paired ST06 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st06':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st06-strict-fifo-hol-blocking --namespace kueue-optimizer-chamber-st06 --port 16406 | tee /tmp/st06.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st07-best-effort-fifo-backfill --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st07`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st07-best-effort-fifo-backfill` (`kueue-optimizer-chamber-st07`) ↔ [`st06-strict-fifo-hol-blocking`](../st06-strict-fifo-hol-blocking/AGENTS.md) (`kueue-optimizer-chamber-st06`) — Exact A/B Twin (`BestEffortFIFO` Backfill vs. `StrictFIFO` HoL Blocking)

- **Controlled Variable Changed**: ONLY `ClusterQueue.spec.queueingStrategy` changes (`BestEffortFIFO` in `st07` vs. `StrictFIFO` in `st06`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st06-strict-fifo-hol-blocking --namespace kueue-optimizer-chamber-st06 --port 16406`
- **Cross-Examination Assertion**: `st07` MUST backfill all 6 small 1-node jobs ahead of `js-blocker-7n` at `t=2s`, reducing wall-clock makespan by `~15s` (`~45s` vs. `~60s`), boosting `Sec 5.4 EFFECTIVE SAT` by `+15pp..+20pp`, and collapsing `Sec 5.2 P50 Wait Time` from `~35s` to `< 2.0s`.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `~66%–75% (+16pp over st06)` (`st07`) vs. `~49%–58% (HoL blocked)` (`st06`)
  - `Sec 5.2 Wait Time`: `P50 < 2.0s (vs ~35s in st06)` (`st07`) vs. `P50 ~30s–40s` (`st06`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st07`) vs. `0 evictions` (`st06`)
  - `Sec 5.4 Effective Sat`: `~66%–75%` (`st07`) vs. `~49%–58%` (`st06`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: Identical 8-node cluster, identical job sizes, and identical arrival timestamps as `st06`, changing ONLY `queueingStrategy` from `StrictFIFO` to `BestEffortFIFO`.
- **Expected Runtime Behavior**: When `js-blocker-7n` cannot fit at `t=1s`, `BestEffortFIFO` immediately admits all 6 small 1-node jobs (`24` TPUs) at `t=2s` into the 6 free nodes (`100%` cluster fill from `t=2s` to `t=17s`), completes them all before `js-anchor-2n` finishes at `t=30s`, and then runs `js-blocker-7n` from `t=30s` to `t=45s` — cutting total makespan from `60s` (`st06`) down to `45s`.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `~66%–75% (+16pp over st06)` |
| **Section `5.2` Queue Wait Time** | `P50 < 2.0s (vs ~35s in st06)` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `~66%–75%` |

### Detailed Metric Checkpoints

- `Section 5.1` & `Section 5.4`: Higher utilization **`~66%–75%`** (vs `~52%` in `st06`) because total makespan shrinks by `25%` (`45s` vs `60s`) for the exact same total TPU-seconds (`1,020 TPU-s`).
- `Section 5.2 (Wait Time)`: Median wait time drops from `~35s` (`st06`) to **`< 2.0s`** because 7 of the 8 jobs (`js-anchor-2n` + all 6 `js-small-*`) start immediately upon arrival.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ `P50` wait time matching `st06` (`>20s`) — would mean `BestEffortFIFO` failed to backfill past `js-blocker-7n`.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
