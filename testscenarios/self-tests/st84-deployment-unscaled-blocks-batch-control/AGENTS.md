# Agent Execution & Cross-Examination Guide — `st84-deployment-unscaled-blocks-batch-control`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st84-deployment-unscaled-blocks-batch-control`
- **Category**: 17. Deployment Scaling & Rolling Upgrades
- **Title**: Unscaled `8`-Replica `Deployment` Blocking Queued Batch `Job`s (Control Twin of st83)
- **Primary Verification Target**: Direct A/B control twin of `st83` omitting `ScaleDown` updates: `srv-pool` holds `16/16` TPUs continuously, keeping both `batch-0` and `batch-1` `Unadmitted: 2` for the entire `34s` window
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st84` (`--namespace kueue-optimizer-chamber-st84`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16484` | Prometheus `19131` (collision-free across all 95 scenarios `st01`–`st95`)
- **Mandatory Paired Counterpart(s)**: [`st83-deployment-scale-down-unblocks-batch-and-zero`](../st83-deployment-scale-down-unblocks-batch-and-zero/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st95`) and non-overlapping port pair (`16401/19048` .. `16495/19142`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST84 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st84' (API port 16484, Prom port 19131):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st84-deployment-unscaled-blocks-batch-control --duration 34s --namespace kueue-optimizer-chamber-st84 --port 16484 > /tmp/st84.log 2>&1 &
PID_ST84=$!

# Simultaneously launch paired ST83 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st83' (API port 16483, Prom port 19130):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st83-deployment-scale-down-unblocks-batch-and-zero --duration 34s --namespace kueue-optimizer-chamber-st83 --port 16483 > /tmp/st83.log 2>&1 &
PID_ST83=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST84" "$PID_ST83"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST84 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st84' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st84-deployment-unscaled-blocks-batch-control --duration 34s --namespace kueue-optimizer-chamber-st84 --port 16484 | tee /tmp/st84.log

# 2. Run paired ST83 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st83':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st83-deployment-scale-down-unblocks-batch-and-zero --duration 34s --namespace kueue-optimizer-chamber-st83 --port 16483 | tee /tmp/st83.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st84-deployment-unscaled-blocks-batch-control --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st84`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st84-deployment-unscaled-blocks-batch-control` (`kueue-optimizer-chamber-st84`) ↔ [`st83-deployment-scale-down-unblocks-batch-and-zero`](../st83-deployment-scale-down-unblocks-batch-and-zero/AGENTS.md) (`kueue-optimizer-chamber-st83`) — Exact `Deployment` Scale-Down A/B Twin (Unscaled Control `st84` vs. `ScaleDown 8 -> 4 -> 0` `st83`)

- **Controlled Variable Changed**: Omitting `ScaleDown` updates (`st84`) vs. applying `8 -> 4 -> 0` (`st83`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st83-deployment-scale-down-unblocks-batch-and-zero --duration 34s --namespace kueue-optimizer-chamber-st83 --port 16483`
- **Cross-Examination Assertion**: Confirms `srv-pool`'s `8` long-running pods (`restartPolicy: Always`) never prematurely complete and permanently hold `16/16` TPUs until an explicit `ScaleDown` or `Deleted` event frees quota.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `95%–100% (16/16 TPUs steady)` (`st84`) vs. `100% (t=0..19s) -> 50% (t=19..26s) -> 0%` (`st83`)
  - `Sec 5.2 Wait Time`: `p-std < 2.0s (8 serving; 2 batch unadmitted)` (`st84`) vs. `batch wait ~8s–11s (10 total admitted)` (`st83`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st84`) vs. `0 evictions` (`st83`)
  - `Sec 5.4 Effective Sat`: `92%–99% (active_at_end=8, Unadmitted=2)` (`st84`) vs. `75%–92% (completed=10, active_at_end=0)` (`st83`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: Identical 4 nodes (`16` TPUs), `cq-dep-unscaled` (`16` TPUs), `srv-pool` (`replicas: 8`, `16` TPUs at `t=0s`), and `2` `4`-TPU batch `Job`s (`t=+2s`) as `st83`, omitting the `t=+10s` and `t=+26s` `ScaleDown` updates (`--duration 34s`).
- **Expected Runtime Behavior**: `srv-pool` occupies `16/16` TPUs (`100%`) across the full `34s` window (`withinClusterQueue: Never`), so `batch-0` and `batch-1` never admit (`pending=2`, `pods succeeded=0`).

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `95%–100% (16/16 TPUs steady)` |
| **Section `5.2` Queue Wait Time** | `p-std < 2.0s (8 serving; 2 batch unadmitted)` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `92%–99% (active_at_end=8, Unadmitted=2)` |

### Detailed Metric Checkpoints

- `Execution Summary`: `Workflows Submitted: 3/3`, `admitted=8, pending=2`, `pods running=8, succeeded=0`.
- `Section 5.1`: `cq-dep-unscaled` `google.com/tpu` stays at **`~95%–100%` (`16/16` TPUs)**.
- `Section 5.3`: **`0`** evictions.
- `Section 5.4`: `Qualifying Workloads: 8 (completed=0, active_at_end=8)`, `Unadmitted: 2 [4 pods]`, `EFFECTIVE SAT ≈ 92%–99%`.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ Either `batch-0` or `batch-1` admitting while `srv-pool` holds `16/16` TPUs.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
