# Agent Execution & Cross-Examination Guide — `st85-deployment-rolling-upgrade-max-unavailable`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st85-deployment-rolling-upgrade-max-unavailable`
- **Category**: 17. Deployment Scaling & Rolling Upgrades
- **Title**: `Deployment` Rolling Upgrade (`templateHash` `v1 -> v2`) With `maxUnavailable: 1, maxSurge: 0`
- **Primary Verification Target**: Verifies privacy-preserving `kueue-optimizer.gke.io/template-hash` rolling `Upgrade` at `t=+12s` completes all `4` `v2` replicas inside a tight `8`-TPU `ClusterQueue` when `maxUnavailable: 1, maxSurge: 0` releases `1` `v1` pod before creating each `v2` pod
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st85` (`--namespace kueue-optimizer-chamber-st85`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16485` | Prometheus `19132` (collision-free across all 95 scenarios `st01`–`st95`)
- **Mandatory Paired Counterpart(s)**: [`st86-deployment-rolling-upgrade-surge-deadlock`](../st86-deployment-rolling-upgrade-surge-deadlock/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st95`) and non-overlapping port pair (`16401/19048` .. `16495/19142`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST85 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st85' (API port 16485, Prom port 19132):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st85-deployment-rolling-upgrade-max-unavailable --duration 40s --namespace kueue-optimizer-chamber-st85 --port 16485 > /tmp/st85.log 2>&1 &
PID_ST85=$!

# Simultaneously launch paired ST86 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st86' (API port 16486, Prom port 19133):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st86-deployment-rolling-upgrade-surge-deadlock --duration 40s --namespace kueue-optimizer-chamber-st86 --port 16486 > /tmp/st86.log 2>&1 &
PID_ST86=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST85" "$PID_ST86"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST85 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st85' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st85-deployment-rolling-upgrade-max-unavailable --duration 40s --namespace kueue-optimizer-chamber-st85 --port 16485 | tee /tmp/st85.log

# 2. Run paired ST86 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st86':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st86-deployment-rolling-upgrade-surge-deadlock --duration 40s --namespace kueue-optimizer-chamber-st86 --port 16486 | tee /tmp/st86.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st85-deployment-rolling-upgrade-max-unavailable --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st85`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st85-deployment-rolling-upgrade-max-unavailable` (`kueue-optimizer-chamber-st85`) ↔ [`st86-deployment-rolling-upgrade-surge-deadlock`](../st86-deployment-rolling-upgrade-surge-deadlock/AGENTS.md) (`kueue-optimizer-chamber-st86`) — Exact `Deployment` Rolling-Upgrade Strategy A/B Twin (`maxUnavailable: 1, maxSurge: 0` `st85` vs. `maxUnavailable: 0, maxSurge: 2` `st86`)

- **Controlled Variable Changed**: ONLY `spec.strategy.rollingUpdate` changes (`{maxUnavailable: 1, maxSurge: 0}` in `st85` vs. `{maxUnavailable: 0, maxSurge: 2}` in `st86`) on identical `4`-replica `v1 -> v2` `templateHash` upgrade inside a tight `8`-TPU `ClusterQueue`.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st86-deployment-rolling-upgrade-surge-deadlock --duration 40s --namespace kueue-optimizer-chamber-st86 --port 16486`
- **Cross-Examination Assertion**: `st85` completes all `4` `v2` replacements (`8` total admissions = `4` retired `v1` + `4` active `v2`, `pending=0`), whereas `st86` deadlocks with `2` surge `v2` pods gated (`4` total admissions, `pending=2`, `Unadmitted=2`).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `88%–100% (~8/8 CQ TPUs across rollout)` (`st85`) vs. `95%–100% (8/8 CQ TPUs held by v1)` (`st86`)
  - `Sec 5.2 Wait Time`: `p-std < 2.5s (8 total admitted: 4 v1 + 4 v2)` (`st85`) vs. `p-std < 2.0s (4 v1 admitted; 2 v2 pending)` (`st86`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st85`) vs. `0 evictions` (`st86`)
  - `Sec 5.4 Effective Sat`: `44%–50% (completed=4 v1 + active_at_end=4 v2)` (`st85`) vs. `45%–50% (active_at_end=4 v1, Unadmitted=2 v2)` (`st86`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 4 nodes (`16` physical TPUs), `cq-dep-upgrade-clean` (`8`-TPU quota = exact `4` pods × `2` TPUs), `--duration 40s`. `model-server` (`replicas: 4`, `maxUnavailable: 1, maxSurge: 0`) starts on `th-0000000000000001` at `t=0s` and upgrades to `th-0000000000000002` at `t=+12s`.
- **Expected Runtime Behavior**: At `t=0s`, `4` `v1` pods fill `8/8` CQ TPUs. At `t=+12s`, `template-hash` changes to `th-0000000000000002`: `deployment-controller` scales down `v1` by `1` -> freeing `2` TPUs -> `1` `v2` pod admits and becomes `Ready` -> repeating 4 times until all `4` `v2` pods are `Running` (`8` total Kueue admissions = `4` cleanly retired `v1` + `4` active `v2`, `pending=0`).

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `88%–100% (~8/8 CQ TPUs across rollout)` |
| **Section `5.2` Queue Wait Time** | `p-std < 2.5s (8 total admitted: 4 v1 + 4 v2)` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `44%–50% (completed=4 v1 + active_at_end=4 v2)` |

### Detailed Metric Checkpoints

- `Section 1 (kueue_admitted_workloads_total)` & `Section 5.2`: **`8`** total admitted replica `Workload`s (`4` `v1` + `4` `v2`), `pending=0` at test end.
- `Section 5.1`: `cq-dep-upgrade-clean` stays near **`88%–100%` (`~7.5–8 / 8` CQ TPUs)**.
- `Section 5.3`: **`0`** evictions.
- `Section 5.4`: `Qualifying Workloads: 8 (completed=4, active_at_end=4)`, `Excluded: 0`, `Unadmitted: 0`, physical `EFFECTIVE SAT ≈ 44%–50%` (`8/16` cluster TPUs).

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ Sanitization stripping the `v1 -> v2` template diff (`admitted_workloads_total` MUST be `8`, never `4`).
- ❌ Rollout stalling with `pending > 0` at `t=40s`.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
