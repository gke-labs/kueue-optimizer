# Agent Execution & Cross-Examination Guide — `st06-strict-fifo-hol-blocking`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st06-strict-fifo-hol-blocking`
- **Category**: 2. Queueing, Backfill & Topology
- **Title**: StrictFIFO Head-of-Line Blocking Under Heterogeneous Job Sizes
- **Primary Verification Target**: `queueingStrategy: StrictFIFO` blocking 6 small 1-node jobs behind an inadmissible 7-node head-of-line job
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st06` (`--namespace kueue-optimizer-chamber-st06`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16406` | Prometheus `19053` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st07-best-effort-fifo-backfill`](../st07-best-effort-fifo-backfill/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST06 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st06' (API port 16406, Prom port 19053):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st06-strict-fifo-hol-blocking --namespace kueue-optimizer-chamber-st06 --port 16406 > /tmp/st06.log 2>&1 &
PID_ST06=$!

# Simultaneously launch paired ST07 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st07' (API port 16407, Prom port 19054):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st07-best-effort-fifo-backfill --namespace kueue-optimizer-chamber-st07 --port 16407 > /tmp/st07.log 2>&1 &
PID_ST07=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST06" "$PID_ST07"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST06 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st06' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st06-strict-fifo-hol-blocking --namespace kueue-optimizer-chamber-st06 --port 16406 | tee /tmp/st06.log

# 2. Run paired ST07 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st07':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st07-best-effort-fifo-backfill --namespace kueue-optimizer-chamber-st07 --port 16407 | tee /tmp/st07.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st06-strict-fifo-hol-blocking --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st06`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st06-strict-fifo-hol-blocking` (`kueue-optimizer-chamber-st06`) ↔ [`st07-best-effort-fifo-backfill`](../st07-best-effort-fifo-backfill/AGENTS.md) (`kueue-optimizer-chamber-st07`) — Exact A/B Twin (`StrictFIFO` vs. `BestEffortFIFO`)

- **Controlled Variable Changed**: ONLY `ClusterQueue.spec.queueingStrategy` changes (`StrictFIFO` in `st06` vs. `BestEffortFIFO` in `st07`); nodes, job sizes, durations, and arrival times are 100% identical.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st07-best-effort-fifo-backfill --namespace kueue-optimizer-chamber-st07 --port 16407`
- **Cross-Examination Assertion**: `st06` MUST show severe head-of-line blocking behind `js-blocker-7n`: total wall duration `~60s` (vs. `~45s` in `st07`), `Sec 5.4 EFFECTIVE SAT ≈ 49%–58%` (vs. `~66%–75%` in `st07`), and `Sec 5.2 P50 Wait Time ≈ 30s–40s` (vs. `< 2.0s` in `st07`).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `~49%–58% (HoL blocked)` (`st06`) vs. `~66%–75% (+16pp over st06)` (`st07`)
  - `Sec 5.2 Wait Time`: `P50 ~30s–40s` (`st06`) vs. `P50 < 2.0s (vs ~35s in st06)` (`st07`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st06`) vs. `0 evictions` (`st07`)
  - `Sec 5.4 Effective Sat`: `~49%–58%` (`st06`) vs. `~66%–75%` (`st07`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 8-node (`32`-TPU) cluster, `cq-strict` (`StrictFIFO`). At `t=0s`, a 2-node (`8`-TPU, `30s`) anchor job starts, leaving `24` TPUs (`75%` of cluster) free. At `t=1s`, a 7-node (`28`-TPU, `15s`) blocker arrives. At `t=2s`, six 1-node (`4`-TPU, `15s`) jobs arrive.
- **Expected Runtime Behavior**: Because `js-blocker-7n` needs `28` TPUs and only `24` are free, `StrictFIFO` keeps `js-blocker-7n` AND all 6 small jobs behind it pending for `29s` while 6 out of 8 cluster nodes sit idle. At `t=30s`, `js-anchor-2n` completes, `js-blocker-7n` + one 1-node job run (`30s–45s`), and the remaining 5 small jobs finish (`45s–60s`).

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `~49%–58% (HoL blocked)` |
| **Section `5.2` Queue Wait Time** | `P50 ~30s–40s` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `~49%–58%` |

### Detailed Metric Checkpoints

- `Section 5.1 (cq-strict)` & `Section 5.4`: Low utilization **`~49%–58%`** (`25%` during the first `30s` due to HoL blocking, then `100%` for `15s`, then `62.5%` for `15s`).
- `Section 5.2 (Wait Time)`: High wait time on the small jobs (`~28s–43s`), `P50 ≈ 35s`.
- `Section 1 Gauge (kueue_pending_workloads)`: Remains `7` for almost `30s`.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ Small jobs `js-small-*` admitting before `js-blocker-7n` while `queueingStrategy == StrictFIFO`.
- ❌ Cluster utilization exceeding `65%` in this trace (which would mean out-of-order backfilling occurred despite `StrictFIFO`).

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
