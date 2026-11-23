# Agent Execution & Cross-Examination Guide — `st53-scale-tas-backfill-best-effort-128n-1056w`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st53-scale-tas-backfill-best-effort-128n-1056w`
- **Category**: 11. Large-Scale TAS Packing, Slices & Multi-Pool (>100 Nodes, >1,000 Workloads)
- **Title**: 128-Node, 1,056-Workload TAS Rack-Fragmentation Blocking & `BestEffortFIFO` Backfill
- **Primary Verification Target**: Pins 1 full node (`8` TPUs) in every one of the `32` racks at `t0` (`256/1,024` TPUs), proving `4`-node full-rack TAS jobs block on topology while `1,016` `4`-TPU jobs backfill `768` free TPUs
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st53` (`--namespace kueue-optimizer-chamber-st53`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16453` | Prometheus `19100` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st54-scale-tas-preferred-and-partial-t0-144n-1104w`](../st54-scale-tas-preferred-and-partial-t0-144n-1104w/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST53 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st53' (API port 16453, Prom port 19100):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st53-scale-tas-backfill-best-effort-128n-1056w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st53 --port 16453 > /tmp/st53.log 2>&1 &
PID_ST53=$!

# Simultaneously launch paired ST54 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st54' (API port 16454, Prom port 19101):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st54-scale-tas-preferred-and-partial-t0-144n-1104w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st54 --port 16454 > /tmp/st54.log 2>&1 &
PID_ST54=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST53" "$PID_ST54"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST53 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st53' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st53-scale-tas-backfill-best-effort-128n-1056w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st53 --port 16453 | tee /tmp/st53.log

# 2. Run paired ST54 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st54':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st54-scale-tas-preferred-and-partial-t0-144n-1104w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st54 --port 16454 | tee /tmp/st54.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st53-scale-tas-backfill-best-effort-128n-1056w --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st53`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st53-scale-tas-backfill-best-effort-128n-1056w` (`kueue-optimizer-chamber-st53`) ↔ [`st54-scale-tas-preferred-and-partial-t0-144n-1104w`](../st54-scale-tas-preferred-and-partial-t0-144n-1104w/AGENTS.md) (`kueue-optimizer-chamber-st54`) — TAS Rack-Blocked Backfill (`st53`) vs. Mixed Required/Preferred/Unconstrained TAS (`st54`) Pair

- **Controlled Variable Changed**: `32` rack-pinning `t0` blockers delaying `8` full-rack `32`-TPU jobs while `1,016` host jobs backfill (`st53`) vs. `48` `t0` `2`-pod TAS jobs (half partially bound at `t0`) + `1,056` required/preferred/unconstrained arrivals (`st54`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st54-scale-tas-preferred-and-partial-t0-144n-1104w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st54 --port 16454`
- **Cross-Examination Assertion**: `st53` delays `arr-fullrack-*` until `t >= 16s` while maintaining `> 90%` backfill utilization; `st54` reconstructs all `96` `t0` pods (`48` bound + `24`+`24` Tier-4 synthesized) and completes `1,104` workloads.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `Backfill > 90% during rack block, AVG ~68%–90%` (`st53`) vs. `Peak 100% (1,152/1,152 TPUs), AVG ~68%–90%` (`st54`)
  - `Sec 5.2 Wait Time`: `Full-rack jobs wait >= 15s; small jobs backfill` (`st53`) vs. `3 TAS modes co-scheduled cleanly` (`st54`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st53`) vs. `0 evictions` (`st54`)
  - `Sec 5.4 Effective Sat`: `~64%–90% (1,056 completed)` (`st53`) vs. `~62%–90% (1,104 completed)` (`st54`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 128 nodes (`32` `4`-node racks, `8` TPUs/node = `1,024` TPUs), `BestEffortFIFO`. At `t0`, `32` `8`-TPU TAS jobs pin `1` node in each of the `32` racks (`16s` remaining -> `0` completely empty `32`-TPU racks, `768` TPUs free). At `t=1s`, `8` full-rack `32`-TPU (`4×8`) TAS jobs (`2x2x4-id`) arrive, followed by `1,016` `4`-TPU `hostname` TAS jobs across `t=2s..14s` (**`1,056` workloads**).
- **Expected Runtime Behavior**: `arr-fullrack-00..07` cannot fit any single `2x2x4-id` rack at `t=1s` despite `768` free TPUs (`32 × 8 = 256` needed), proving TAS enforces rack-level `t0` occupancy; meanwhile `BestEffortFIFO` backfills the `768` free TPUs with `arr-bf-small-*` jobs and drains all `1,056` workloads once racks clear.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `Backfill > 90% during rack block, AVG ~68%–90%` |
| **Section `5.2` Queue Wait Time** | `Full-rack jobs wait >= 15s; small jobs backfill` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `~64%–90% (1,056 completed)` |

### Detailed Metric Checkpoints

- `Section 5.1`: `cq-tas-backfill` climbs above `90%` during `2s–16s` via `BestEffortFIFO` backfill instead of stalling at `25%` (`256/1,024`).
- `Section 5.4`: `Qualifying Workloads: 1056 (completed=1056 [1080 pods])`.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ `arr-fullrack-*` admitting at `t=1s` onto racks whose 4th node is already full at `t0`.
- ❌ Head-of-line blocking holding cluster utilization at `25%` under `BestEffortFIFO`.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
