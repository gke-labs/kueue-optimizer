# Agent Execution & Cross-Examination Guide — `st30-inadmissible-oversized-workload-rejection`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st30-inadmissible-oversized-workload-rejection`
- **Category**: 6. Tricky Accounting & Edge Cases
- **Title**: Permanently Inadmissible Oversized Workloads (`64` TPUs Requested on a `32`-TPU Cluster)
- **Primary Verification Target**: Graceful handling of permanently `inadmissible` jobs (`Unadmitted: 2 [32 pods]`) without hang, crash, or saturation inflation
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st30` (`--namespace kueue-optimizer-chamber-st30`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16430` | Prometheus `19077` (collision-free across all 95 scenarios `st01`–`st95`)
- **Mandatory Paired Counterpart(s)**: [`st01-50pct-single-cq-quota-cap`](../st01-50pct-single-cq-quota-cap/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st95`) and non-overlapping port pair (`16401/19048` .. `16495/19142`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST30 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st30' (API port 16430, Prom port 19077):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st30-inadmissible-oversized-workload-rejection --duration 42s --namespace kueue-optimizer-chamber-st30 --port 16430 > /tmp/st30.log 2>&1 &
PID_ST30=$!

# Simultaneously launch paired ST01 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st01' (API port 16401, Prom port 19048):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st01-50pct-single-cq-quota-cap --namespace kueue-optimizer-chamber-st01 --port 16401 > /tmp/st01.log 2>&1 &
PID_ST01=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST30" "$PID_ST01"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST30 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st30' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st30-inadmissible-oversized-workload-rejection --duration 42s --namespace kueue-optimizer-chamber-st30 --port 16430 | tee /tmp/st30.log

# 2. Run paired ST01 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st01':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st01-50pct-single-cq-quota-cap --namespace kueue-optimizer-chamber-st01 --port 16401 | tee /tmp/st01.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st30-inadmissible-oversized-workload-rejection --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st30`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st30-inadmissible-oversized-workload-rejection` (`kueue-optimizer-chamber-st30`) ↔ [`st01-50pct-single-cq-quota-cap`](../st01-50pct-single-cq-quota-cap/AGENTS.md) (`kueue-optimizer-chamber-st01`) — Oversized `inadmissible` Monster Guard (`st30`) vs. Clean Half-Cluster Load (`st01`) Pair

- **Controlled Variable Changed**: `st30` injects two impossible `16`-node (`64`-TPU > `32`-TPU cluster) JobSets alongside 4 valid `1`-node (`4`-TPU) JobSets (`--duration 42s`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st01-50pct-single-cq-quota-cap --namespace kueue-optimizer-chamber-st01 --port 16401`
- **Cross-Examination Assertion**: The 2 oversized `64`-TPU jobs MUST remain in `Unadmitted: 2 [32 pods]` (`pending=2`), contributing `0.00%` to `Sec 5.4` (`EFFECTIVE SAT ≈ 40%–45%` from the 4 valid jobs only).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `~40%–45% (16/32 TPUs for 35s of 42s)` (`st30`) vs. `95%–100% (16/16 TPUs)` (`st01`)
  - `Sec 5.2 Wait Time`: `Valid jobs < 1.0s` (`st30`) vs. `P50 <=1s, AVG ~0.5*T_wave (~10s @1x)` (`st01`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st30`) vs. `0 evictions` (`st01`)
  - `Sec 5.4 Effective Sat`: `~40%–45% (Unadmitted: 2 [32 pods])` (`st30`) vs. `~50.0% (640 / 1,280 TPU-s)` (`st01`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 8-node (`32`-TPU) cluster, `cq-guard` (`32` TPUs). 4 valid 1-node (`4`-TPU, `35s`) jobs arrive at `t=0s` (`16` TPUs = `50%` of cluster), plus 2 impossible 16-node (`64`-TPU) jobs at `t=1s` that exceed both CQ quota and physical cluster size (bounded by `--duration 42s`).
- **Expected Runtime Behavior**: All 4 valid jobs admit at `t=0s` and complete cleanly at `t=35s`. Both `js-oversized-*` workloads remain permanently `Pending` (`status=inadmissible` in `kueue_pending_workloads`) until `--duration 42s` concludes the test.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `~40%–45% (16/32 TPUs for 35s of 42s)` |
| **Section `5.2` Queue Wait Time** | `Valid jobs < 1.0s` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `~40%–45% (Unadmitted: 2 [32 pods])` |

### Detailed Metric Checkpoints

- `Execution Summary`: `Workloads Status: admitted=0, pending=2, finished=4` (surfacing the 2 unrunnable workloads).
- `Section 1 Gauge (kueue_pending_workloads{status='inadmissible'})`: Constant `2.00` throughout the run.
- `Section 5.4 (Effective Saturation)`: Subtitle shows `Qualifying Workloads: 4 (completed=4 [4 pods], active_at_end=0 [0 pods]) | Excluded (preempted/reclaimed/failed): 0 [0 pods] | Unadmitted: 2 [32 pods]`. `EFFECTIVE SAT` reflects ONLY the 4 completed valid jobs (`560 / 1,344 TPU-s ≈ 40%–45%`).

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ Unadmitted `64`-TPU workloads being counted as `Active at End` (which would push `EFFECTIVE SAT` above `400%`).
- ❌ `testrunner.py` or `kueue_metrics_report.py` crashing when workloads have `status.admission == None`.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
