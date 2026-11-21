# Agent Execution & Cross-Examination Guide — `st40-t0-tas-topology-domain-occupancy`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and re-run:
> ```bash
> python3 testscenarios/self-tests/generate_self_test_suite.py
> python3 deploy/scripts/generate_scenario_readme.py --all
> python3 testscenarios/self-tests/generate_self_test_suite.py --check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st40-t0-tas-topology-domain-occupancy`
- **Category**: 8. Initial t0 Workloads — Preemption & TAS
- **Title**: 7-Level TAS `t0` Domain Occupancy (`p244-0` + `p224-2` Occupied at `t0`, Disjoint `p224-3` Fit + `p244` Queueing)
- **Primary Verification Target**: Verifies `build_topology_assignment` populates Kueue's TAS cache at `t0` so `arr-tas-4n-fit` lands on disjoint `st-node-12..15` (`<2s`) while `arr-tas-8n-wait` waits `~26s`
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st40` (`--namespace kueue-optimizer-chamber-st40`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16440` | Prometheus `19087` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st10-tas-subslice-and-cube-packing`](../st10-tas-subslice-and-cube-packing/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST40 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st40' (API port 16440, Prom port 19087):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st40-t0-tas-topology-domain-occupancy --namespace kueue-optimizer-chamber-st40 --port 16440 > /tmp/st40.log 2>&1 &
PID_ST40=$!

# Simultaneously launch paired ST10 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st10' (API port 16410, Prom port 19057):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st10-tas-subslice-and-cube-packing --namespace kueue-optimizer-chamber-st10 --port 16410 > /tmp/st10.log 2>&1 &
PID_ST10=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST40" "$PID_ST10"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST40 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st40' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st40-t0-tas-topology-domain-occupancy --namespace kueue-optimizer-chamber-st40 --port 16440 | tee /tmp/st40.log

# 2. Run paired ST10 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st10':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st10-tas-subslice-and-cube-packing --namespace kueue-optimizer-chamber-st10 --port 16410 | tee /tmp/st10.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st40-t0-tas-topology-domain-occupancy --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st40`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st40-t0-tas-topology-domain-occupancy` (`kueue-optimizer-chamber-st40`) ↔ [`st10-tas-subslice-and-cube-packing`](../st10-tas-subslice-and-cube-packing/AGENTS.md) (`kueue-optimizer-chamber-st10`) — `t0`-Occupied 7-Level TAS Cube (`st40`) vs. Greenfield 7-Level TAS Cube (`st10`) Pair

- **Controlled Variable Changed**: `st40` pre-occupies `p244-0` (`8` nodes) and `p224-2` (`4` nodes) with `t0` `PodGroup`s (`topologyAssignment` populated), leaving only `p224-3` (`st-node-12..15`) free at `t=0s`.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st10-tas-subslice-and-cube-packing --namespace kueue-optimizer-chamber-st10 --port 16410`
- **Cross-Examination Assertion**: `arr-tas-4n-fit` schedules immediately (`< 2s`) onto `st-node-12..15` while `arr-tas-8n-wait` waits `~26s` for `p244-0`, with `0` node collisions.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `~72%–82% (100% during 0s–20s)` (`st40`) vs. `93%–100% (64/64 TPUs)` (`st10`)
  - `Sec 5.2 Wait Time`: `4n < 1.5s, 8n ~26s (AVG ~13s)` (`st40`) vs. `< 1.5s` (`st10`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st40`) vs. `0 evictions` (`st10`)
  - `Sec 5.4 Effective Sat`: `~70%–82% (4 TAS workloads, 24 pods)` (`st40`) vs. `92%–100%` (`st10`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 16-node (`64`-TPU) v7x cube with 7-level `Topology`. At `t0`, `init-tas-8n` occupies `p244-0` (`st-node-00..07`, `26s`) and `init-tas-4n` occupies `p224-2` (`st-node-08..11`, `26s`), leaving ONLY `p224-3` (`st-node-12..15`, `16` TPUs) free. At `t=0s`, `arr-tas-4n-fit` (`2x2x4`, `20s`) and `arr-tas-8n-wait` (`2x4x4`, `18s`) arrive.
- **Expected Runtime Behavior**: `arr-tas-4n-fit` admits immediately (`< 2s`) onto `st-node-12..15` (`64/64` TPUs = `100%` cube pack during `0s–20s`) with zero TAS collision against `st-node-00..11`. `arr-tas-8n-wait` waits `~26s` until `init-tas-8n` finishes, then runs `26s–44s`.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `~72%–82% (100% during 0s–20s)` |
| **Section `5.2` Queue Wait Time** | `4n < 1.5s, 8n ~26s (AVG ~13s)` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `~70%–82% (4 TAS workloads, 24 pods)` |

### Detailed Metric Checkpoints

- `Execution Summary`: `Initial Kueue Workloads: 2 PodGroup(s) (12 pod(s))` (`[TAS]`), `KWOK Pods Status: ... succeeded=24, pending=0, failed=0`.
- `Section 5.2`: Bimodal 2-workload split (`arr-tas-4n-fit <= 1.5s`, `arr-tas-8n-wait ≈ 26s`, `AVG ≈ 12s–15s`).
- `Section 5.4`: `Qualifying Workloads: 4 (completed=4 [24 pods])`, **`EFFECTIVE SAT ≈ 70%–82%`** (`(48×26 + 16×20 + 32×18) / (64×44) = 2,144 / 2,816 = 76.1%`).

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ Kueue TAS assigning `arr-tas-4n-fit` onto `p224-0/1/2` (`st-node-00..11`, causing `Pending` pod collisions) because `topologyAssignment` was missing on `t0` `PodGroup`s.
- ❌ `arr-tas-8n-wait` admitting before `init-tas-8n` finishes.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
python3 testscenarios/self-tests/generate_self_test_suite.py
python3 deploy/scripts/generate_scenario_readme.py --all
python3 testscenarios/self-tests/generate_self_test_suite.py --check
```
