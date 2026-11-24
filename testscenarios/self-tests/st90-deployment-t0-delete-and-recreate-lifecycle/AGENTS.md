# Agent Execution & Cross-Examination Guide — `st90-deployment-t0-delete-and-recreate-lifecycle`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st90-deployment-t0-delete-and-recreate-lifecycle`
- **Category**: 18. Deployment Pauses, `t0` Status & Lifecycle
- **Title**: `t0` `Deployment` Deletion, Same-Name Incarnation-`1` Re-Creation & Canary Creation/Deletion
- **Primary Verification Target**: Verifies full `Deployment` deletion & creation lifecycle: `t0` `ephemeral-srv` (`4/4` = `8` TPUs) is deleted at `t=+8s` (freeing `100%` of its ReplicaSet/Pods/Workloads), re-created with the same name at `t=+12s` (`replicas: 6` = `12` TPUs) alongside new `canary-srv` (`replicas: 2` = `4` TPUs -> `16/16` TPUs), and `canary-srv` is deleted at `t=+26s` without touching `ephemeral-srv`
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st90` (`--namespace kueue-optimizer-chamber-st90`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16490` | Prometheus `19137` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st89-deployment-t0-partial-status-and-preemption`](../st89-deployment-t0-partial-status-and-preemption/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST90 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st90' (API port 16490, Prom port 19137):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st90-deployment-t0-delete-and-recreate-lifecycle --no-skip-initial-offset --duration 38s --namespace kueue-optimizer-chamber-st90 --port 16490 > /tmp/st90.log 2>&1 &
PID_ST90=$!

# Simultaneously launch paired ST89 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st89' (API port 16489, Prom port 19136):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st89-deployment-t0-partial-status-and-preemption --no-skip-initial-offset --duration 38s --namespace kueue-optimizer-chamber-st89 --port 16489 > /tmp/st89.log 2>&1 &
PID_ST89=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST90" "$PID_ST89"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST90 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st90' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st90-deployment-t0-delete-and-recreate-lifecycle --no-skip-initial-offset --duration 38s --namespace kueue-optimizer-chamber-st90 --port 16490 | tee /tmp/st90.log

# 2. Run paired ST89 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st89':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st89-deployment-t0-partial-status-and-preemption --no-skip-initial-offset --duration 38s --namespace kueue-optimizer-chamber-st89 --port 16489 | tee /tmp/st89.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st90-deployment-t0-delete-and-recreate-lifecycle --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st90`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st90-deployment-t0-delete-and-recreate-lifecycle` (`kueue-optimizer-chamber-st90`) ↔ [`st89-deployment-t0-partial-status-and-preemption`](../st89-deployment-t0-partial-status-and-preemption/AGENTS.md) (`kueue-optimizer-chamber-st89`) — `t0` `Deployment` Deletion & Re-Creation (`st90`) vs. `t0` `Deployment` Preemption & Self-Healing (`st89`)

- **Controlled Variable Changed**: Explicit `Deleted` + `New` re-creation (`st90`, `0` evictions) vs. `LowerPriority` preemption by `urgent-eval` (`st89`, `4` evictions).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st89-deployment-t0-partial-status-and-preemption --no-skip-initial-offset --duration 38s --namespace kueue-optimizer-chamber-st89 --port 16489`
- **Cross-Examination Assertion**: `st90` retires `6` serving replicas cleanly (`completed=6, Excluded=0`) and ends with `6` `ephemeral-srv` inc-`1` replicas (`12/16` TPUs), whereas `st89` excludes `4` preempted serving replicas (`Excluded=4`) and heals back to `16/16` TPUs.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `50% (t0..8s) -> 0% -> 100% (12..26s) -> 75%` (`st90`) vs. `92%–100% (16/16 TPUs across t0, preemption & heal)` (`st89`)
  - `Sec 5.2 Wait Time`: `p-std < 2.5s (8 post-t0 replica Workloads)` (`st90`) vs. `p-high < 2.5s; 4 healed p-low admit post-18s` (`st89`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st90`) vs. `4 InClusterQueue preemptions (p-low)` (`st89`)
  - `Sec 5.4 Effective Sat`: `62%–76% (completed=6 retired + active_at_end=6)` (`st90`) vs. `72%–86% (completed=1, active_at_end=8, Excluded=4)` (`st89`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 4 nodes (`16` TPUs), `cq-dep-lifecycle` (`16` TPUs), `--no-skip-initial-offset --duration 38s`. `t0`: `ephemeral-srv` inc `0` (`4/4` = `8` TPUs); `t=+8s`: `Deleted` `ephemeral-srv` inc `0`; `t=+12s`: `New` `ephemeral-srv` inc `1` (`replicas: 6` = `12` TPUs) + `New` `canary-srv` (`replicas: 2` = `4` TPUs); `t=+26s`: `Deleted` `canary-srv`.
- **Expected Runtime Behavior**: At `t=+8s`, `_execute_workflow_deletion` deletes `ephemeral-srv` inc `0`, its `ReplicaSet`, and its `4` pods (`wait_gone=True`), dropping usage `8 -> 0` TPUs and crediting `4` cleanly retired serving replicas. At `t=+12s`, `ephemeral-srv` inc `1` (`6` pods) and `canary-srv` (`2` pods) admit cleanly (`16/16` TPUs = `100%`). At `t=+26s`, deleting `canary-srv` removes only `canary-srv`'s `ReplicaSet`/pods (`16 -> 12` TPUs = `75%`), leaving `ephemeral-srv` inc `1`'s `6` pods running (`completed=6`, `active_at_end=6`, `Excluded=0`).

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `50% (t0..8s) -> 0% -> 100% (12..26s) -> 75%` |
| **Section `5.2` Queue Wait Time** | `p-std < 2.5s (8 post-t0 replica Workloads)` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `62%–76% (completed=6 retired + active_at_end=6)` |

### Detailed Metric Checkpoints

- `Execution Summary`: `Initial Deployments: 1 Deployment(s) (4/4 replica(s) ready at t0)`, `Workflows Submitted: 2/2`, `admitted=6, pending=0`, `pods running=6` at `t=38s`.
- `Section 1 (kueue_admitted_workloads_total)`: **`8`** post-`t0` admissions (`6` `ephemeral-srv` inc `1` + `2` `canary-srv`; `t0` inc `0` adds `0` counter increments).
- `Section 5.3`: **`0`** evictions (graceful `Deployment` deletions are NOT Kueue evictions).
- `Section 5.4`: `Qualifying Workloads: 12 (completed=6, active_at_end=6)` (`4` inc-`0` + `2` `canary-srv` retired + `6` inc-`1` active), `Excluded: 0`.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ Stale `ReplicaSet` or pods from `ephemeral-srv` inc `0` surviving `t=+8s` deletion and blocking inc `1`'s `12` + `canary-srv`'s `4` = `16` TPUs.
- ❌ Deleting `canary-srv` at `t=+26s` accidentally killing any pod of `ephemeral-srv` inc `1`.
- ❌ Misclassifying deleted `t0` or `canary-srv` serving replicas as `Excluded (Interrupted)` (`Excluded` MUST be `0`).

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
