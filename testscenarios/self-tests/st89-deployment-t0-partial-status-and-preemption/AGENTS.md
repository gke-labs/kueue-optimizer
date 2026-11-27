# Agent Execution & Cross-Examination Guide — `st89-deployment-t0-partial-status-and-preemption`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st89-deployment-t0-partial-status-and-preemption`
- **Category**: 18. Deployment Pauses, `t0` Status & Lifecycle
- **Title**: `t0` `Deployment` Full (`2/2`) & Partial (`4/6`) Readiness + `LowerPriority` Preemption & Recovery
- **Primary Verification Target**: Verifies `InitialDeploymentBootstrap` pre-admits exact `readyReplicas` (`steady-srv` `2/2`, `bg-embed` `2/2`, `partial-api` `4/6` with `2` gated in `LocalQueue`) at `t0` with `0` extra `admitted_workloads_total`, then preempts `4` `p-low` serving replicas when `p-high` `urgent-eval` (`8` TPUs, `10s`) arrives at `t=+8s`, and automatically heals via `ReplicaSet` after `t=+18s`
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st89` (`--namespace kueue-optimizer-chamber-st89`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16489` | Prometheus `19136` (collision-free across all 95 scenarios `st01`–`st95`)
- **Mandatory Paired Counterpart(s)**: [`st90-deployment-t0-delete-and-recreate-lifecycle`](../st90-deployment-t0-delete-and-recreate-lifecycle/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st95`) and non-overlapping port pair (`16401/19048` .. `16495/19142`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST89 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st89' (API port 16489, Prom port 19136):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st89-deployment-t0-partial-status-and-preemption --no-skip-initial-offset --duration 38s --namespace kueue-optimizer-chamber-st89 --port 16489 > /tmp/st89.log 2>&1 &
PID_ST89=$!

# Simultaneously launch paired ST90 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st90' (API port 16490, Prom port 19137):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st90-deployment-t0-delete-and-recreate-lifecycle --no-skip-initial-offset --duration 38s --namespace kueue-optimizer-chamber-st90 --port 16490 > /tmp/st90.log 2>&1 &
PID_ST90=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST89" "$PID_ST90"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST89 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st89' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st89-deployment-t0-partial-status-and-preemption --no-skip-initial-offset --duration 38s --namespace kueue-optimizer-chamber-st89 --port 16489 | tee /tmp/st89.log

# 2. Run paired ST90 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st90':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st90-deployment-t0-delete-and-recreate-lifecycle --no-skip-initial-offset --duration 38s --namespace kueue-optimizer-chamber-st90 --port 16490 | tee /tmp/st90.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st89-deployment-t0-partial-status-and-preemption --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st89`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st89-deployment-t0-partial-status-and-preemption` (`kueue-optimizer-chamber-st89`) ↔ [`st90-deployment-t0-delete-and-recreate-lifecycle`](../st90-deployment-t0-delete-and-recreate-lifecycle/AGENTS.md) (`kueue-optimizer-chamber-st90`) — `t0` `Deployment` Partial Status & Preemption Healing (`st89`) vs. `t0` `Deployment` Deletion & Re-Creation Lifecycle (`st90`)

- **Controlled Variable Changed**: `st89` tests `t0` partial readiness (`4/6`) + `LowerPriority` preemption (`4` evictions) + `ReplicaSet` self-healing; `st90` tests graceful `t0` `Deployment` `Deleted` (`t=+8s`), incarnation-`1` same-name re-creation (`t=+12s`), and `canary-srv` creation/deletion (`0` evictions).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st90-deployment-t0-delete-and-recreate-lifecycle --no-skip-initial-offset --duration 38s --namespace kueue-optimizer-chamber-st90 --port 16490`
- **Cross-Examination Assertion**: `Sec 5.3` records **`4` preemptions (`Excluded=4`) in `st89`** vs. **`0` evictions (`Excluded=0`, `completed=6` cleanly retired serving replicas) in `st90`**, proving `saturation.py` separates preempted serving pods from cleanly deleted/retired serving pods.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `92%–100% (16/16 TPUs across t0, preemption & heal)` (`st89`) vs. `50% (t0..8s) -> 0% -> 100% (12..26s) -> 75%` (`st90`)
  - `Sec 5.2 Wait Time`: `p-high < 2.5s; 4 healed p-low admit post-18s` (`st89`) vs. `p-std < 2.5s (8 post-t0 replica Workloads)` (`st90`)
  - `Sec 5.3 Evictions`: `4 InClusterQueue preemptions (p-low)` (`st89`) vs. `0 evictions` (`st90`)
  - `Sec 5.4 Effective Sat`: `72%–86% (completed=1, active_at_end=8, Excluded=4)` (`st89`) vs. `62%–76% (completed=6 retired + active_at_end=6)` (`st90`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 4 nodes (`16` TPUs), `cq-dep-t0-preempt` (`16` TPUs, `withinClusterQueue: LowerPriority`), `--no-skip-initial-offset --duration 38s`. At `t0`: `steady-srv` (`p-std=100`, `2/2` = `4` TPUs), `bg-embed` (`p-low=50`, `2/2` = `4` TPUs), and partially admitted `partial-api` (`p-low=50`, `4/6` = `8` TPUs running + `2` pending = `16/16` TPUs full). At `t=+8s`: `urgent-eval` (`p-high=200`, `8` TPUs, `10s` `Job`) arrives.
- **Expected Runtime Behavior**: At `t0`, `8` replica `Workload`s are pre-admitted (`8/10` desired replicas) and `partial-api`'s remaining `2` replicas stay queued in `LocalQueue`. At `t=+8s`, `urgent-eval` (`p-high=200`) preempts `4` `p-low` serving replicas (`8` TPUs) while protecting `steady-srv` (`p-std=100`); `ReplicaSet` immediately creates `4` replacement gated pods. Once `urgent-eval` finishes at `~t=18.5s`, `4` queued `p-low` serving replicas re-admit (`admitted=8, pending=2` restored).

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `92%–100% (16/16 TPUs across t0, preemption & heal)` |
| **Section `5.2` Queue Wait Time** | `p-high < 2.5s; 4 healed p-low admit post-18s` |
| **Section `5.3` Evictions & Reclaims** | `4 InClusterQueue preemptions (p-low)` |
| **Section `5.4` Effective Saturation** | `72%–86% (completed=1, active_at_end=8, Excluded=4)` |

### Detailed Metric Checkpoints

- `Execution Summary`: `Initial Deployments: 3 Deployment(s) (8/10 replica(s) ready at t0)`, `Initial Infra Pods: 0`.
- `Section 5.2`: `p-high` (`urgent-eval`) waits **`< 2.5s`**; `4` `p-low` serving replicas admit after `t=18.5s`.
- `Section 5.3`: **`Total Evictions == 4`** (`InClusterQueue` preemptions of `p-low` serving replicas; `0` `p-std` evictions).
- `Section 5.4`: `completed=1` (`urgent-eval`), `active_at_end=8`, `Excluded: 4` (the `4` preempted `t0` serving replicas), `Unadmitted: 2` (`2` excess `p-low` replicas exceeding `16`-TPU quota).

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ Pre-admitting all `6/6` `partial-api` replicas at `t0` (`readyReplicas: 4` MUST keep `2` gated).
- ❌ Duplicating `t0` `Deployment` pods into `Initial Infra Pods` (`Initial Infra Pods` MUST be `0`).
- ❌ Evicting `p-std` `steady-srv` instead of `p-low` replicas.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
