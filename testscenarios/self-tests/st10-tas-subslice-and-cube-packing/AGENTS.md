# Agent Execution & Cross-Examination Guide — `st10-tas-subslice-and-cube-packing`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st10-tas-subslice-and-cube-packing`
- **Category**: 2. Queueing, Backfill & Topology
- **Title**: 7-Level Topology-Aware Scheduling (TAS) Exact Binary Tree Cube Packing
- **Primary Verification Target**: Kueue `Topology` + `ResourceFlavor.spec.topologyName` + `kueue.x-k8s.io/podset-required-topology` packing `8+4+2+1+1 = 16` nodes
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st10` (`--namespace kueue-optimizer-chamber-st10`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16410` | Prometheus `19057` (collision-free across all 95 scenarios `st01`–`st95`)
- **Mandatory Paired Counterpart(s)**: [`st40-t0-tas-topology-domain-occupancy`](../st40-t0-tas-topology-domain-occupancy/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st95`) and non-overlapping port pair (`16401/19048` .. `16495/19142`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST10 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st10' (API port 16410, Prom port 19057):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st10-tas-subslice-and-cube-packing --namespace kueue-optimizer-chamber-st10 --port 16410 > /tmp/st10.log 2>&1 &
PID_ST10=$!

# Simultaneously launch paired ST40 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st40' (API port 16440, Prom port 19087):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st40-t0-tas-topology-domain-occupancy --namespace kueue-optimizer-chamber-st40 --port 16440 > /tmp/st40.log 2>&1 &
PID_ST40=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST10" "$PID_ST40"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST10 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st10' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st10-tas-subslice-and-cube-packing --namespace kueue-optimizer-chamber-st10 --port 16410 | tee /tmp/st10.log

# 2. Run paired ST40 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st40':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st40-t0-tas-topology-domain-occupancy --namespace kueue-optimizer-chamber-st40 --port 16440 | tee /tmp/st40.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st10-tas-subslice-and-cube-packing --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st10`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st10-tas-subslice-and-cube-packing` (`kueue-optimizer-chamber-st10`) ↔ [`st40-t0-tas-topology-domain-occupancy`](../st40-t0-tas-topology-domain-occupancy/AGENTS.md) (`kueue-optimizer-chamber-st40`) — Greenfield TAS Cube Packing (`st10`) vs. Pre-Occupied `t0` TAS Cube Packing (`st40`) Pair

- **Controlled Variable Changed**: `st10` packs `8+4+2+1+1 = 16` fresh `t=0s` nodes, whereas `st40` starts with `p244-0` (`8` nodes) + `p224-2` (`4` nodes) already occupied by `t0` `PodGroup`s and schedules new `2x2x4` and `2x4x4` arrivals against `t0` TAS state.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st40-t0-tas-topology-domain-occupancy --namespace kueue-optimizer-chamber-st40 --port 16440`
- **Cross-Examination Assertion**: Both runs achieve `0` pod collisions; in `st40`, `arr-tas-4n-fit` lands immediately (`< 2s`) on disjoint `p224-3` while `arr-tas-8n-wait` queues `~26s` until `init-tas-8n` frees `p244-0`.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `93%–100% (64/64 TPUs)` (`st10`) vs. `~72%–82% (100% during 0s–20s)` (`st40`)
  - `Sec 5.2 Wait Time`: `< 1.5s` (`st10`) vs. `4n < 1.5s, 8n ~26s (AVG ~13s)` (`st40`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st10`) vs. `0 evictions` (`st40`)
  - `Sec 5.4 Effective Sat`: `92%–100%` (`st10`) vs. `~70%–82% (4 TAS workloads, 24 pods)` (`st40`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 16-node (`64`-TPU) v7x cube with full 7-level hierarchy (`block -> 4x4x4 -> 2x4x4 -> 2x2x4 -> 2x2x2 -> 2x2x1 -> hostname`). 5 workloads submitted at `t=0s` whose binary partition sizes sum to `8 + 4 + 2 + 1 + 1 = 16` nodes (`64/64` TPUs).
- **Expected Runtime Behavior**: Kueue TAS assigns each JobSet to a non-overlapping aligned sub-tree (`p244-0`, `p224-2`, `p222-6`, `p221-14`, `p221-15`) on the very first scheduling cycle with zero fragmentation.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `93%–100% (64/64 TPUs)` |
| **Section `5.2` Queue Wait Time** | `< 1.5s` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `92%–100%` |

### Detailed Metric Checkpoints

- `Execution Summary`: `Workflows Submitted: 5/5`, `Workloads Status: ... finished=5`, `KWOK Pods Status: ... succeeded=16, pending=0`.
- `Section 5.1` & `Section 5.4`: **`93%–100%`** (`64/64` TPUs packed across all 16 nodes).
- `Section 5.2`: `< 1.5s` (all 5 TAS shapes admit simultaneously).

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ Misaligned 1-node or 2-node placement splintering the `2x4x4` or `2x2x4` domain and forcing `js-tas-8n` to wait (`Wait > 5s`).
- ❌ Pods stuck in `Pending` due to missing node topology labels.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
