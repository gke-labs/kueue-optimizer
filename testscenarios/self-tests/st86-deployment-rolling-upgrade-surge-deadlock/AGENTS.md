# Agent Execution & Cross-Examination Guide — `st86-deployment-rolling-upgrade-surge-deadlock`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st86-deployment-rolling-upgrade-surge-deadlock`
- **Category**: 18. Deployment Pauses, `t0` Status & Lifecycle
- **Title**: `Deployment` Rolling Upgrade Surge Quota Deadlock (`maxUnavailable: 0, maxSurge: 2` — Twin of st85)
- **Primary Verification Target**: Direct A/B twin of `st85` changing ONLY `rollingUpdate` to `{maxUnavailable: 0, maxSurge: 2}` in the same `8`-TPU `ClusterQueue`: `2` surge `v2` pods block on Kueue admission gate while `4` `v1` pods hold `8/8` TPUs, reproducing the classic Kueue surge rollout deadlock
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st86` (`--namespace kueue-optimizer-chamber-st86`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16486` | Prometheus `19133` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st85-deployment-rolling-upgrade-max-unavailable`](../st85-deployment-rolling-upgrade-max-unavailable/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST86 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st86' (API port 16486, Prom port 19133):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st86-deployment-rolling-upgrade-surge-deadlock --duration 40s --namespace kueue-optimizer-chamber-st86 --port 16486 > /tmp/st86.log 2>&1 &
PID_ST86=$!

# Simultaneously launch paired ST85 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st85' (API port 16485, Prom port 19132):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st85-deployment-rolling-upgrade-max-unavailable --duration 40s --namespace kueue-optimizer-chamber-st85 --port 16485 > /tmp/st85.log 2>&1 &
PID_ST85=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST86" "$PID_ST85"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST86 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st86' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st86-deployment-rolling-upgrade-surge-deadlock --duration 40s --namespace kueue-optimizer-chamber-st86 --port 16486 | tee /tmp/st86.log

# 2. Run paired ST85 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st85':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st85-deployment-rolling-upgrade-max-unavailable --duration 40s --namespace kueue-optimizer-chamber-st85 --port 16485 | tee /tmp/st85.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st86-deployment-rolling-upgrade-surge-deadlock --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st86`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st86-deployment-rolling-upgrade-surge-deadlock` (`kueue-optimizer-chamber-st86`) ↔ [`st85-deployment-rolling-upgrade-max-unavailable`](../st85-deployment-rolling-upgrade-max-unavailable/AGENTS.md) (`kueue-optimizer-chamber-st85`) — Exact `Deployment` Rolling-Upgrade Strategy A/B Twin (Surge Deadlock `st86` vs. Clean `maxUnavailable: 1` Rollout `st85`)

- **Controlled Variable Changed**: `{maxUnavailable: 0, maxSurge: 2}` (`st86`) vs. `{maxUnavailable: 1, maxSurge: 0}` (`st85`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st85-deployment-rolling-upgrade-max-unavailable --duration 40s --namespace kueue-optimizer-chamber-st85 --port 16485`
- **Cross-Examination Assertion**: Demonstrates why Kueue-managed `Deployment`s in tight `ClusterQueue`s need `maxUnavailable >= 1` (or spare surge quota): `admitted_workloads_total` is `4` (`pending=2`) in `st86` vs. `8` (`pending=0`) in `st85`.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `95%–100% (8/8 CQ TPUs held by v1)` (`st86`) vs. `88%–100% (~8/8 CQ TPUs across rollout)` (`st85`)
  - `Sec 5.2 Wait Time`: `p-std < 2.0s (4 v1 admitted; 2 v2 pending)` (`st86`) vs. `p-std < 2.5s (8 total admitted: 4 v1 + 4 v2)` (`st85`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st86`) vs. `0 evictions` (`st85`)
  - `Sec 5.4 Effective Sat`: `45%–50% (active_at_end=4 v1, Unadmitted=2 v2)` (`st86`) vs. `44%–50% (completed=4 v1 + active_at_end=4 v2)` (`st85`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: Identical 4 nodes (`16` physical TPUs), `8`-TPU `ClusterQueue`, and `model-server` `v1 -> v2` `Upgrade` at `t=+12s` as `st85`, changing ONLY `strategy.rollingUpdate` from `{maxUnavailable: 1, maxSurge: 0}` to `{maxUnavailable: 0, maxSurge: 2}` (`--duration 40s`).
- **Expected Runtime Behavior**: At `t=0s`, `4` `v1` pods fill `8/8` CQ TPUs. At `t=+12s`, `deployment-controller` creates `2` surge `v2` pods WITHOUT terminating any `v1` pod (`maxUnavailable: 0`). Because `8/8` CQ TPUs are already held by `v1`, both `v2` surge `Workload`s stay `QuotaReserved=False` (`pending=2`), freezing the rollout at `4` `v1` running + `2` `v2` gated (`4` total admissions vs. `8` in `st85`).

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `95%–100% (8/8 CQ TPUs held by v1)` |
| **Section `5.2` Queue Wait Time** | `p-std < 2.0s (4 v1 admitted; 2 v2 pending)` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `45%–50% (active_at_end=4 v1, Unadmitted=2 v2)` |

### Detailed Metric Checkpoints

- `Execution Summary`: `admitted=4, pending=2` (`4` total admissions vs. `8` in `st85`).
- `Section 5.1`: `cq-dep-upgrade-surge` holds **`95%–100%` (`8/8` CQ TPUs)**.
- `Section 5.3`: **`0`** evictions.
- `Section 5.4`: `Qualifying Workloads: 4 (completed=0, active_at_end=4)`, `Unadmitted: 2 [2 pods]`, physical `EFFECTIVE SAT ≈ 45%–50%`.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ Completing the `v2` rollout when `maxUnavailable: 0` and `0` spare CQ quota exists (`pending` MUST be `2` at `t=40s`).

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
