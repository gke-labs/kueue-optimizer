# Agent Execution & Cross-Examination Guide — `st82-deployment-scale-up-quota-cap`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st82-deployment-scale-up-quota-cap`
- **Category**: 17. Deployment Scaling & Rolling Upgrades
- **Title**: `Deployment` Scale-Up (`2 -> 6` Replicas) Capped at `8`-TPU `ClusterQueue` Ceiling (Twin of st81)
- **Primary Verification Target**: Direct A/B twin of `st81` halving `ClusterQueue` quota from `16` to `8` TPUs: `ScaleUp 2 -> 6` at `t=+10s` admits `2` new replicas (`4/6` running = `8/8` TPUs) while holding exact `2` excess replica `Workload`s gated in `LocalQueue` (`pending=2`, `Unadmitted: 2`)
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st82` (`--namespace kueue-optimizer-chamber-st82`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16482` | Prometheus `19129` (collision-free across all 95 scenarios `st01`–`st95`)
- **Mandatory Paired Counterpart(s)**: [`st81-deployment-scale-up-headroom`](../st81-deployment-scale-up-headroom/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st95`) and non-overlapping port pair (`16401/19048` .. `16495/19142`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST82 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st82' (API port 16482, Prom port 19129):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st82-deployment-scale-up-quota-cap --duration 36s --namespace kueue-optimizer-chamber-st82 --port 16482 > /tmp/st82.log 2>&1 &
PID_ST82=$!

# Simultaneously launch paired ST81 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st81' (API port 16481, Prom port 19128):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st81-deployment-scale-up-headroom --duration 36s --namespace kueue-optimizer-chamber-st81 --port 16481 > /tmp/st81.log 2>&1 &
PID_ST81=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST82" "$PID_ST81"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST82 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st82' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st82-deployment-scale-up-quota-cap --duration 36s --namespace kueue-optimizer-chamber-st82 --port 16482 | tee /tmp/st82.log

# 2. Run paired ST81 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st81':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st81-deployment-scale-up-headroom --duration 36s --namespace kueue-optimizer-chamber-st81 --port 16481 | tee /tmp/st81.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st82-deployment-scale-up-quota-cap --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st82`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st82-deployment-scale-up-quota-cap` (`kueue-optimizer-chamber-st82`) ↔ [`st81-deployment-scale-up-headroom`](../st81-deployment-scale-up-headroom/AGENTS.md) (`kueue-optimizer-chamber-st81`) — Exact `Deployment` Scale-Up A/B Twin (`8`-TPU Quota Cap `st82` vs. `16`-TPU Full Headroom `st81`)

- **Controlled Variable Changed**: `ClusterQueue` `google.com/tpu` `nominalQuota` (`8` in `st82` vs. `16` in `st81`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st81-deployment-scale-up-headroom --duration 36s --namespace kueue-optimizer-chamber-st81 --port 16481`
- **Cross-Examination Assertion**: Proves Kueue gates individual `Deployment` scale-up pods at the `ClusterQueue` quota boundary (`4` running + `2` gated in `st82` vs. `6` running in `st81`, `0` evictions in both).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `Steps 50% -> 100% (8/8 CQ TPUs, AVG ~85%)` (`st82`) vs. `Steps 25% -> 75% (12/16 TPUs, AVG ~60%)` (`st81`)
  - `Sec 5.2 Wait Time`: `p-std < 2.0s (4 admitted; 2 pending)` (`st82`) vs. `p-std < 2.0s (6 replica Workloads)` (`st81`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st82`) vs. `0 evictions` (`st81`)
  - `Sec 5.4 Effective Sat`: `38%–45% (active_at_end=4, Unadmitted=2)` (`st82`) vs. `54%–64% (active_at_end=6, pending=0)` (`st81`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: Identical 4 nodes (`16` physical TPUs) and `infer-api` `Deployment` (`2 -> 6` replicas at `t=+10s`, `2` TPUs/pod) as `st81`, changing ONLY `cq-dep-scaleup-cap` `nominalQuota` from `16` to `8` TPUs (`--duration 36s`).
- **Expected Runtime Behavior**: At `t=0s`, `2` replicas admit (`4/8` CQ TPUs = `50%`). At `t=+10s`, `infer-api` scales to `6` replicas: Kueue admits `2` more replicas (`8/8` CQ TPUs = `100%`) and keeps the remaining `2` pods gated (`schedulingGates: [kueue.x-k8s.io/admission]`, `pending=2`).

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `Steps 50% -> 100% (8/8 CQ TPUs, AVG ~85%)` |
| **Section `5.2` Queue Wait Time** | `p-std < 2.0s (4 admitted; 2 pending)` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `38%–45% (active_at_end=4, Unadmitted=2)` |

### Detailed Metric Checkpoints

- `Execution Summary`: `admitted=4, pending=2`, `pods running=4, pending=2, succeeded=0`.
- `Section 5.1`: `cq-dep-scaleup-cap` `google.com/tpu` hits **`100%` (`8/8` CQ TPUs)** after `t=+10s` (`AVG ≈ 80%–90%`).
- `Section 5.3`: **`0`** evictions.
- `Section 5.4`: `Qualifying Workloads: 4 (completed=0, active_at_end=4)`, `Unadmitted: 2 [2 pods]`, physical `EFFECTIVE SAT ≈ 38%–45%` (`8/16` cluster TPUs after `t=10s`).

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ Admitting more than `4` replicas (`> 8` TPUs) or scheduling gated pods onto idle physical nodes.
- ❌ Counting the `2` gated pending replica `Workload`s in `Section 5.4` saturation.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
