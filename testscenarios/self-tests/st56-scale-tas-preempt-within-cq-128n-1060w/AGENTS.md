# Agent Execution & Cross-Examination Guide — `st56-scale-tas-preempt-within-cq-128n-1060w`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st56-scale-tas-preempt-within-cq-128n-1060w`
- **Category**: 12. Large-Scale TAS Preemption, Reclaim, Fair Sharing & Hybrid (>100 Nodes, >1,000 Workloads)
- **Title**: 128-Node, 1,060-Workload TAS Intra-CQ Preemption (`2x2x2-id` Evicting `64` `t0` TAS Jobs)
- **Primary Verification Target**: Verifies TAS topology-aware preemption evicts `64` `t0` `p-low` TAS `PodGroup`s (`32` node-pairs = `512` TPUs) for `32` arriving `2`-node `p-high` TAS jobs and dynamically resubmits all `64` victims
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st56` (`--namespace kueue-optimizer-chamber-st56`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16456` | Prometheus `19103` (collision-free across all 95 scenarios `st01`–`st95`)
- **Mandatory Paired Counterpart(s)**: [`st47-scale-quota-preemption-128n-1080w`](../st47-scale-quota-preemption-128n-1080w/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st95`) and non-overlapping port pair (`16401/19048` .. `16495/19142`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST56 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st56' (API port 16456, Prom port 19103):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st56-scale-tas-preempt-within-cq-128n-1060w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st56 --port 16456 > /tmp/st56.log 2>&1 &
PID_ST56=$!

# Simultaneously launch paired ST47 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st47' (API port 16447, Prom port 19094):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st47-scale-quota-preemption-128n-1080w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st47 --port 16447 > /tmp/st47.log 2>&1 &
PID_ST47=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST56" "$PID_ST47"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST56 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st56' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st56-scale-tas-preempt-within-cq-128n-1060w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st56 --port 16456 | tee /tmp/st56.log

# 2. Run paired ST47 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st47':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st47-scale-quota-preemption-128n-1080w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st47 --port 16447 | tee /tmp/st47.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st56-scale-tas-preempt-within-cq-128n-1060w --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st56`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st56-scale-tas-preempt-within-cq-128n-1060w` (`kueue-optimizer-chamber-st56`) ↔ [`st47-scale-quota-preemption-128n-1080w`](../st47-scale-quota-preemption-128n-1080w/AGENTS.md) (`kueue-optimizer-chamber-st47`) — 128-Node TAS vs. Quota-Only Intra-CQ Preemption Pair

- **Controlled Variable Changed**: 7-level TAS `2x2x2-id` preemption (`st56`, `1,060` workloads) vs. quota-only preemption (`st47`, `1,080` workloads).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st47-scale-quota-preemption-128n-1080w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st47 --port 16447`
- **Cross-Examination Assertion**: Both evict `>= 64` `t0` `p-low` `PodGroup`s (`512` TPUs) at `t=3s` and dynamically resubmit all `64` victims.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `Peak 100% (1,024/1,024 TPUs), AVG ~68%–90%` (`st56`) vs. `Peak 100% (1,024/1,024 TPUs), AVG ~70%–92%` (`st47`)
  - `Sec 5.2 Wait Time`: `p-high 2-node TAS burst waits < 5s` (`st56`) vs. `p-high < p-low wait` (`st47`)
  - `Sec 5.3 Evictions`: `>= 64 InClusterQueue TAS evictions` (`st56`) vs. `>= 64 InClusterQueue evictions` (`st47`)
  - `Sec 5.4 Effective Sat`: `~60%–88% (1,060 completed)` (`st56`) vs. `~60%–90% (1,080 completed, 64 excluded)` (`st47`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 128 nodes (`32` racks, `1,024` TPUs) with TAS, `withinClusterQueue: LowerPriority`. At `t0`, `64` `p-low` `8`-TPU TAS jobs (`st-node-000..063`, `34s` remaining) + `64` `p-high` `8`-TPU TAS jobs (`st-node-064..127`, `20s` remaining) occupy `100%` of the cluster (`128` `t0` workloads). At `t=3s`, `32` `2`-node (`16`-TPU) `p-high` TAS jobs (`2x2x2-id`) arrive, followed by `900` `4`-TPU TAS jobs across `t=8s..18s` (**`1,060` workloads**).
- **Expected Runtime Behavior**: At `t=3s`, Kueue's TAS preemptor evicts `64` `t0` `p-low` TAS `PodGroup`s to free `32` contiguous `2x2x2-id` node pairs, admits all `32` `p-high` `2`-node TAS jobs immediately, dynamically resubmits the `64` `p-low` TAS `Job`s (`[+64 preempted-t0 resubmit]`), and completes all `1,060` workloads.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `Peak 100% (1,024/1,024 TPUs), AVG ~68%–90%` |
| **Section `5.2` Queue Wait Time** | `p-high 2-node TAS burst waits < 5s` |
| **Section `5.3` Evictions & Reclaims** | `>= 64 InClusterQueue TAS evictions` |
| **Section `5.4` Effective Saturation** | `~60%–88% (1,060 completed)` |

### Detailed Metric Checkpoints

- `Execution Summary`: `Initial Kueue Workloads: 128 PodGroup(s)`, `[+64 preempted-t0 resubmit]`.
- `Section 5.3`: **`>= 64`** `InClusterQueue` TAS evictions.
- `Section 5.4`: `Qualifying Workloads: 1060 (completed=1060)`, `Excluded: 64`.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ TAS preemption failing to free contiguous `2x2x2-id` node pairs occupied by `t0` `PodGroup`s.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
