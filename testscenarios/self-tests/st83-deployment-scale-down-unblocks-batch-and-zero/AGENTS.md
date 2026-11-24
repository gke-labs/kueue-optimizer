# Agent Execution & Cross-Examination Guide — `st83-deployment-scale-down-unblocks-batch-and-zero`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st83-deployment-scale-down-unblocks-batch-and-zero`
- **Category**: 17. Deployment Scaling & Rolling Upgrades
- **Title**: `Deployment` Scale-Down (`8 -> 4 -> 0`) Unblocking Batch `Job`s & Scale-to-Zero
- **Primary Verification Target**: Verifies `ScaleDown` (`8 -> 4` at `t=+10s`) releases `8` TPUs of `ClusterQueue` quota so `2` queued `4`-TPU batch `Job`s admit and complete, followed by full scale-to-zero (`4 -> 0` at `t=+26s`) crediting all `8` retired serving replicas + `2` batch `Job`s in `Section 5.4`
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st83` (`--namespace kueue-optimizer-chamber-st83`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16483` | Prometheus `19130` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st84-deployment-unscaled-blocks-batch-control`](../st84-deployment-unscaled-blocks-batch-control/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST83 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st83' (API port 16483, Prom port 19130):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st83-deployment-scale-down-unblocks-batch-and-zero --duration 34s --namespace kueue-optimizer-chamber-st83 --port 16483 > /tmp/st83.log 2>&1 &
PID_ST83=$!

# Simultaneously launch paired ST84 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st84' (API port 16484, Prom port 19131):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st84-deployment-unscaled-blocks-batch-control --duration 34s --namespace kueue-optimizer-chamber-st84 --port 16484 > /tmp/st84.log 2>&1 &
PID_ST84=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST83" "$PID_ST84"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST83 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st83' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st83-deployment-scale-down-unblocks-batch-and-zero --duration 34s --namespace kueue-optimizer-chamber-st83 --port 16483 | tee /tmp/st83.log

# 2. Run paired ST84 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st84':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st84-deployment-unscaled-blocks-batch-control --duration 34s --namespace kueue-optimizer-chamber-st84 --port 16484 | tee /tmp/st84.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st83-deployment-scale-down-unblocks-batch-and-zero --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st83`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st83-deployment-scale-down-unblocks-batch-and-zero` (`kueue-optimizer-chamber-st83`) ↔ [`st84-deployment-unscaled-blocks-batch-control`](../st84-deployment-unscaled-blocks-batch-control/AGENTS.md) (`kueue-optimizer-chamber-st84`) — Exact `Deployment` Scale-Down A/B Twin (`ScaleDown 8 -> 4 -> 0` `st83` vs. Unscaled `8`-Replica Blocker `st84`)

- **Controlled Variable Changed**: Replaying post-`t0` `ScaleDown` updates (`8 -> 4` at `t=+10s`, `4 -> 0` at `t=+26s` in `st83`) vs. omitting `ScaleDown` updates (`st84`) on identical `srv-pool` + `2` `4`-TPU batch `Job`s.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st84-deployment-unscaled-blocks-batch-control --duration 34s --namespace kueue-optimizer-chamber-st84 --port 16484`
- **Cross-Examination Assertion**: `st83` unblocks both `batch-0` and `batch-1` at `t=+10s` (`pods succeeded=4`, `Unadmitted=0`) and scales cleanly to `0` at `t=+26s` (`completed=10, active_at_end=0`), whereas `st84` starves both batch `Job`s (`pods succeeded=0`, `Unadmitted=2`, `active_at_end=8`).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `100% (t=0..19s) -> 50% (t=19..26s) -> 0%` (`st83`) vs. `95%–100% (16/16 TPUs steady)` (`st84`)
  - `Sec 5.2 Wait Time`: `batch wait ~8s–11s (10 total admitted)` (`st83`) vs. `p-std < 2.0s (8 serving; 2 batch unadmitted)` (`st84`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st83`) vs. `0 evictions` (`st84`)
  - `Sec 5.4 Effective Sat`: `75%–92% (completed=10, active_at_end=0)` (`st83`) vs. `92%–99% (active_at_end=8, Unadmitted=2)` (`st84`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 4 nodes (`16` TPUs), `cq-dep-scaledown` (`16` TPUs), `--duration 34s`. `srv-pool` (`replicas: 8`, `2` TPUs/pod = `16` TPUs) saturates quota at `t=0s`; `batch-0` & `batch-1` (`4` TPUs each, `8s`) queue at `t=+2s`; `srv-pool` scales `8 -> 4` at `t=+10s` and `4 -> 0` at `t=+26s`.
- **Expected Runtime Behavior**: `batch-0` and `batch-1` stay queued (`t=2..10s`) while `srv-pool` holds `16/16` TPUs. At `t=+10s`, `ScaleDown 8 -> 4` terminates `4` serving pods (`8` TPUs freed), unblocking both batch `Job`s (`wait ≈ 8s–10.5s`, finishing at `~t=19s`). At `t=+26s`, `ScaleDown 4 -> 0` retires the last `4` serving pods (`replicas: 0` preserved, never coerced to `1`), and replay completes cleanly with `10` completed Qualifying Workloads (`8` retired serving + `2` batch).

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `100% (t=0..19s) -> 50% (t=19..26s) -> 0%` |
| **Section `5.2` Queue Wait Time** | `batch wait ~8s–11s (10 total admitted)` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `75%–92% (completed=10, active_at_end=0)` |

### Detailed Metric Checkpoints

- `Execution Summary`: `Workflows Submitted: 3/3`, `pods running=0, succeeded=4` (`batch-0`, `batch-1`).
- `Section 5.2`: `p-std` records `10` admitted workloads (`8` initial `t=0s` serving replicas + `2` batch `Job`s waiting `~8s–11s`).
- `Section 5.3`: **`0`** evictions (clean `ScaleDown` retirement is NOT counted as Kueue eviction/badput).
- `Section 5.4`: `Qualifying Workloads: 10 (completed=10 [12 pods], active_at_end=0 [0 pods])`, `Excluded: 0`, `Unadmitted: 0`.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ Coercing `replicas: 0` to `1` (`pods running` at end MUST be `0`).
- ❌ Dropping retired `srv-pool` replicas from `Section 5.4` or misclassifying clean scale-down as `Excluded (Interrupted)`.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
