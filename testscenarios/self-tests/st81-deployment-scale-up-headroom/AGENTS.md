# Agent Execution & Cross-Examination Guide — `st81-deployment-scale-up-headroom`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st81-deployment-scale-up-headroom`
- **Category**: 17. Deployment Scaling & Rolling Upgrades
- **Title**: `Deployment` Scale-Up (`2 -> 6` Replicas) With Full `ClusterQueue` Headroom
- **Primary Verification Target**: Verifies post-`t0` `Deployment` `ScaleUp` (`updateTypes: ['ScaleUp']`, `replicas: 2 -> 6` at `t=+10s`) creates `4` additional per-pod Kueue `Workload`s that admit immediately onto `16`-TPU `ClusterQueue` (`0` pending, long-running pods never prematurely terminated by KWOK `Stage/pod-complete`)
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st81` (`--namespace kueue-optimizer-chamber-st81`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16481` | Prometheus `19128` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st82-deployment-scale-up-quota-cap`](../st82-deployment-scale-up-quota-cap/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST81 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st81' (API port 16481, Prom port 19128):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st81-deployment-scale-up-headroom --duration 36s --namespace kueue-optimizer-chamber-st81 --port 16481 > /tmp/st81.log 2>&1 &
PID_ST81=$!

# Simultaneously launch paired ST82 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st82' (API port 16482, Prom port 19129):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st82-deployment-scale-up-quota-cap --duration 36s --namespace kueue-optimizer-chamber-st82 --port 16482 > /tmp/st82.log 2>&1 &
PID_ST82=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST81" "$PID_ST82"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST81 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st81' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st81-deployment-scale-up-headroom --duration 36s --namespace kueue-optimizer-chamber-st81 --port 16481 | tee /tmp/st81.log

# 2. Run paired ST82 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st82':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st82-deployment-scale-up-quota-cap --duration 36s --namespace kueue-optimizer-chamber-st82 --port 16482 | tee /tmp/st82.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st81-deployment-scale-up-headroom --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st81`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st81-deployment-scale-up-headroom` (`kueue-optimizer-chamber-st81`) ↔ [`st82-deployment-scale-up-quota-cap`](../st82-deployment-scale-up-quota-cap/AGENTS.md) (`kueue-optimizer-chamber-st82`) — Exact `Deployment` Scale-Up A/B Twin (`16`-TPU Full Headroom `st81` vs. `8`-TPU Quota Cap `st82`)

- **Controlled Variable Changed**: `ClusterQueue` `google.com/tpu` `nominalQuota` changes from `16` (`st81`) to `8` (`st82`) on identical `infer-api` `ScaleUp 2 -> 6` (`2` TPUs/pod) at `t=+10s`.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st82-deployment-scale-up-quota-cap --duration 36s --namespace kueue-optimizer-chamber-st82 --port 16482`
- **Cross-Examination Assertion**: `st81` admits all `6` replicas (`admitted=6, pending=0`, `Sec 5.4 active_at_end=6`), whereas `st82` caps admission at `4` replicas (`admitted=4, pending=2`, `Sec 5.1 = 100%` of `8`-TPU CQ, `Sec 5.4 Unadmitted=2`).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `Steps 25% -> 75% (12/16 TPUs, AVG ~60%)` (`st81`) vs. `Steps 50% -> 100% (8/8 CQ TPUs, AVG ~85%)` (`st82`)
  - `Sec 5.2 Wait Time`: `p-std < 2.0s (6 replica Workloads)` (`st81`) vs. `p-std < 2.0s (4 admitted; 2 pending)` (`st82`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st81`) vs. `0 evictions` (`st82`)
  - `Sec 5.4 Effective Sat`: `54%–64% (active_at_end=6, pending=0)` (`st81`) vs. `38%–45% (active_at_end=4, Unadmitted=2)` (`st82`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 4 nodes (`16` TPUs), `cq-dep-scaleup` (`16` TPUs), `--duration 36s`. `Deployment` `infer-api` (`2` TPUs/pod, `restartPolicy: Always`) arrives at `t=0s` with `replicas: 2` (`4` TPUs) and scales up to `replicas: 6` (`12` TPUs) at `t=+10s`.
- **Expected Runtime Behavior**: At `t=0s`, `2` replica `Workload`s admit (`4/16` TPUs = `25%`). At `t=+10s`, `ScheduledWorkflowUpdate` applies `replicas: 6`; `replicaset-controller` creates `4` new pods, `mpod.kb.io` gates them, and Kueue admits all `4` (`12/16` TPUs = `75%`). All `6` serving pods remain `Running` until `36s` (`succeeded=0`, `pending=0`).

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `Steps 25% -> 75% (12/16 TPUs, AVG ~60%)` |
| **Section `5.2` Queue Wait Time** | `p-std < 2.0s (6 replica Workloads)` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `54%–64% (active_at_end=6, pending=0)` |

### Detailed Metric Checkpoints

- `Execution Summary`: `Workflows Submitted: 1/1`, `admitted=6, pending=0`, `pods running=6, succeeded=0`.
- `Section 5.1`: `cq-dep-scaleup` `google.com/tpu` steps from `25%` (`4/16`) to **`75%` (`12/16` TPUs, `AVG ≈ 55%–65%`)**.
- `Section 5.3`: **`0`** evictions.
- `Section 5.4`: `Qualifying Workloads: 6 (completed=0, active_at_end=6)`, `EFFECTIVE SAT ≈ 54%–64%` (`16` physical TPUs).

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ KWOK `Stage/pod-complete` terminating `Deployment` pods after `10s` (`pods succeeded` MUST stay `0`).
- ❌ Ignoring the `t=+10s` `Update` event and staying stuck at `2` replicas (`admitted=2`).

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
