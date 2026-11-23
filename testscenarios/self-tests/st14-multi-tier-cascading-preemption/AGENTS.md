# Agent Execution & Cross-Examination Guide — `st14-multi-tier-cascading-preemption`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st14-multi-tier-cascading-preemption`
- **Category**: 3. Intra-CQ Preemption Matrix
- **Title**: 4-Tier Cascading Preemption Chain (`tier-1 -> tier-2 -> tier-3 -> tier-4`)
- **Primary Verification Target**: Multi-wave cascading preemptions (`12` total evictions across 3 distinct priority transitions) and time-to-eviction histogram accuracy
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st14` (`--namespace kueue-optimizer-chamber-st14`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16414` | Prometheus `19061` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st12-preempt-within-cq-lower-priority`](../st12-preempt-within-cq-lower-priority/AGENTS.md), [`st15-preemption-badput-collapse-stress`](../st15-preemption-badput-collapse-stress/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST14 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st14' (API port 16414, Prom port 19061):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st14-multi-tier-cascading-preemption --duration 42s --namespace kueue-optimizer-chamber-st14 --port 16414 > /tmp/st14.log 2>&1 &
PID_ST14=$!

# Simultaneously launch paired ST12 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st12' (API port 16412, Prom port 19059):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st12-preempt-within-cq-lower-priority --namespace kueue-optimizer-chamber-st12 --port 16412 > /tmp/st12.log 2>&1 &
PID_ST12=$!

# Simultaneously launch paired ST15 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st15' (API port 16415, Prom port 19062):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st15-preemption-badput-collapse-stress --duration 41s --namespace kueue-optimizer-chamber-st15 --port 16415 > /tmp/st15.log 2>&1 &
PID_ST15=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST14" "$PID_ST12" "$PID_ST15"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST14 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st14' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st14-multi-tier-cascading-preemption --duration 42s --namespace kueue-optimizer-chamber-st14 --port 16414 | tee /tmp/st14.log

# 2. Run paired ST12 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st12':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st12-preempt-within-cq-lower-priority --namespace kueue-optimizer-chamber-st12 --port 16412 | tee /tmp/st12.log

# 3. Run paired ST15 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st15':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st15-preemption-badput-collapse-stress --duration 41s --namespace kueue-optimizer-chamber-st15 --port 16415 | tee /tmp/st15.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st14-multi-tier-cascading-preemption --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st14`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st14-multi-tier-cascading-preemption` (`kueue-optimizer-chamber-st14`) ↔ [`st12-preempt-within-cq-lower-priority`](../st12-preempt-within-cq-lower-priority/AGENTS.md) (`kueue-optimizer-chamber-st12`) — Multi-Tier 3-Cascade Preemption Chain (`st14`) vs. Single-Step Preemption (`st12`) Pair

- **Controlled Variable Changed**: 4 ascending priority tiers arriving every `7s` (`tier-1 -> tier-2 -> tier-3 -> tier-4`, `st14`) vs. a single `p-low -> p-high` step at `t=8s` (`st12`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st12-preempt-within-cq-lower-priority --namespace kueue-optimizer-chamber-st12 --port 16412`
- **Cross-Examination Assertion**: `Sec 5.3 Total Evictions` scales `3x` from `4` (`st12`) to `~12` (`3` cascades × `4` jobs in `st14`), while `Sec 5.4 EFFECTIVE SAT` drops to `~48%` (`st14`) as aborted intermediate tiers are excluded.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `90%–99% (cluster busy all run)` (`st14`) vs. `80%–95%` (`st12`)
  - `Sec 5.2 Wait Time`: `tier-4 wait < 2.0s` (`st14`) vs. `p-high wait < 2.0s` (`st12`)
  - `Sec 5.3 Evictions`: `~12 evictions (3 cascades x 4)` (`st14`) vs. `4 evictions (4 unique)` (`st12`)
  - `Sec 5.4 Effective Sat`: `~44%–54% (only tier-4 + active t=42s)` (`st14`) vs. `~68%–88% (after requeue)` (`st12`)

### Pair: `st14-multi-tier-cascading-preemption` (`kueue-optimizer-chamber-st14`) ↔ [`st15-preemption-badput-collapse-stress`](../st15-preemption-badput-collapse-stress/AGENTS.md) (`kueue-optimizer-chamber-st15`) — Cascading Mid-Life Preemptions (`st14`) vs. Late 87%-Complete Preemption Collapse (`st15`) Pair

- **Controlled Variable Changed**: Preemptions at `7s` intervals (`st14`) vs. simultaneous preemption of all 8 jobs at `t=26s` out of `30s` (`st15`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st15-preemption-badput-collapse-stress --duration 41s --namespace kueue-optimizer-chamber-st15 --port 16415`
- **Cross-Examination Assertion**: Both scenarios keep raw `Sec 5.1` occupancy `>90%` while `Sec 5.4` accurately tracks uninterrupted goodput (`~48%` in `st14`, `~35%` in `st15`).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `90%–99% (cluster busy all run)` (`st14`) vs. `93%–99% (raw occupancy)` (`st15`)
  - `Sec 5.2 Wait Time`: `tier-4 wait < 2.0s` (`st14`) vs. `p-urgent wait < 1.5s` (`st15`)
  - `Sec 5.3 Evictions`: `~12 evictions (3 cascades x 4)` (`st14`) vs. `8 evictions` (`st15`)
  - `Sec 5.4 Effective Sat`: `~44%–54% (only tier-4 + active t=42s)` (`st14`) vs. `~32%–40% (~60pp badput gap)` (`st15`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 8-node (`32`-TPU) cluster, `withinClusterQueue: LowerPriority`. Four 4-job (`32`-TPU) waves arrive every `7s` with strictly ascending priority: `tier-1 (100)` at `t=0s`, `tier-2 (300)` at `t=7s`, `tier-3 (600)` at `t=14s`, `tier-4 (1000)` at `t=21s` (`18s` duration), evaluated over `--duration 42s`.
- **Expected Runtime Behavior**: `tier-1` runs `0s–7s` and gets evicted by `tier-2`; `tier-2` runs `7s–14s` and gets evicted by `tier-3`; `tier-3` runs `14s–21s` and gets evicted by `tier-4`; `tier-4` runs `21s–39s` to full completion.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `90%–99% (cluster busy all run)` |
| **Section `5.2` Queue Wait Time** | `tier-4 wait < 2.0s` |
| **Section `5.3` Evictions & Reclaims** | `~12 evictions (3 cascades x 4)` |
| **Section `5.4` Effective Saturation** | `~44%–54% (only tier-4 + active t=42s)` |

### Detailed Metric Checkpoints

- `Section 5.3 (Total Evictions)`: **`~12` evictions** across `tier-1` (`4`), `tier-2` (`4`), and `tier-3` (`4`).
- `Section 3 Histogram (kueue_pods_ready_to_evicted_time_seconds)`: Tightly clustered around **`~7s`** (every preempted tier ran for `7s` before the next tier arrived).
- `Section 5.2`: `tier-4 (1000)` wait time `< 2.0s`.
- `Section 5.4`: Only `tier-4` (plus whichever `tier-3` jobs re-admit at `t=39s–42s` and remain active at `t=42s`) contributes to `EFFECTIVE SAT` (`~44%–54%`), while the `21s` (`0s–21s`) of preempted `tier-1/2/3` compute is accurately excluded as wasted badput.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ `Section 5.4 EFFECTIVE SAT` reporting `95%–100%` (which would mean crediting the aborted `0s–21s` runs of `tier-1`, `tier-2`, and `tier-3` that were killed before completing).

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
