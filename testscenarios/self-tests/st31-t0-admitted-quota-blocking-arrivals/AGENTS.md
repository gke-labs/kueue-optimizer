# Agent Execution & Cross-Examination Guide — `st31-t0-admitted-quota-blocking-arrivals`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and re-run:
> ```bash
> python3 testscenarios/self-tests/generate_self_test_suite.py
> python3 deploy/scripts/generate_scenario_readme.py --all
> python3 testscenarios/self-tests/generate_self_test_suite.py --check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st31-t0-admitted-quota-blocking-arrivals`
- **Category**: 7. Initial t0 Workloads — Handling & Accounting
- **Title**: Pre-Admitted `t0` Kueue `PodGroup` Workloads Holding Quota & Blocking New Arrivals
- **Primary Verification Target**: Bootstraps 8 `t0`-admitted `PodGroup` `Workload`s (`32/32` TPUs) before CQ activation, jumps `+6s` to first arrival (`remaining = 20s`), and blocks 4 arriving jobs in Kueue until `t0` quota frees
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st31` (`--namespace kueue-optimizer-chamber-st31`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16431` | Prometheus `19078` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st32-t0-mixed-kueue-workloads-and-infra-pods`](../st32-t0-mixed-kueue-workloads-and-infra-pods/AGENTS.md), [`st01-50pct-single-cq-quota-cap`](../st01-50pct-single-cq-quota-cap/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST31 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st31' (API port 16431, Prom port 19078):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st31-t0-admitted-quota-blocking-arrivals --namespace kueue-optimizer-chamber-st31 --port 16431 > /tmp/st31.log 2>&1 &
PID_ST31=$!

# Simultaneously launch paired ST32 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st32' (API port 16432, Prom port 19079):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st32-t0-mixed-kueue-workloads-and-infra-pods --namespace kueue-optimizer-chamber-st32 --port 16432 > /tmp/st32.log 2>&1 &
PID_ST32=$!

# Simultaneously launch paired ST01 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st01' (API port 16401, Prom port 19048):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st01-50pct-single-cq-quota-cap --namespace kueue-optimizer-chamber-st01 --port 16401 > /tmp/st01.log 2>&1 &
PID_ST01=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST31" "$PID_ST32" "$PID_ST01"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST31 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st31' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st31-t0-admitted-quota-blocking-arrivals --namespace kueue-optimizer-chamber-st31 --port 16431 | tee /tmp/st31.log

# 2. Run paired ST32 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st32':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st32-t0-mixed-kueue-workloads-and-infra-pods --namespace kueue-optimizer-chamber-st32 --port 16432 | tee /tmp/st32.log

# 3. Run paired ST01 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st01':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st01-50pct-single-cq-quota-cap --namespace kueue-optimizer-chamber-st01 --port 16401 | tee /tmp/st01.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st31-t0-admitted-quota-blocking-arrivals --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st31`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st31-t0-admitted-quota-blocking-arrivals` (`kueue-optimizer-chamber-st31`) ↔ [`st32-t0-mixed-kueue-workloads-and-infra-pods`](../st32-t0-mixed-kueue-workloads-and-infra-pods/AGENTS.md) (`kueue-optimizer-chamber-st32`) — Full-Quota `t0` Kueue `PodGroup`s (`st31`) vs. Hybrid `t0` `PodGroup`s + `infra-pods` (`st32`) Pair

- **Controlled Variable Changed**: `st31` fills `32/32` CQ TPUs with 8 `t0` Kueue `PodGroup`s (blocking 4 arrivals for `~20s`), whereas `st32` puts 2 pods in `infra-pods` (`8` TPUs) and 4 in `cq-hybrid` (`16/24` TPUs, admitting 2 arrivals immediately).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st32-t0-mixed-kueue-workloads-and-infra-pods --namespace kueue-optimizer-chamber-st32 --port 16432`
- **Cross-Examination Assertion**: `Sec 5.2 AVG` drops from `~18s–20s` (`st31`, blocked behind full `t0` CQ quota) to `< 1.5s` (`st32`, `8` free CQ TPUs).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `~70%–78% (100% w1, 50% w2)` (`st31`) vs. `~92%–100% (24/24 CQ TPUs)` (`st32`)
  - `Sec 5.2 Wait Time`: `AVG ~18s–20s (blocked by t0 quota)` (`st31`) vs. `< 1.5s` (`st32`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st31`) vs. `0 evictions` (`st32`)
  - `Sec 5.4 Effective Sat`: `~70%–77% (12 completed workloads)` (`st31`) vs. `~90%–98% (6 Kueue + 2 infra = 32/32 TPUs)` (`st32`)

### Pair: `st31-t0-admitted-quota-blocking-arrivals` (`kueue-optimizer-chamber-st31`) ↔ [`st01-50pct-single-cq-quota-cap`](../st01-50pct-single-cq-quota-cap/AGENTS.md) (`kueue-optimizer-chamber-st01`) — Bootstrapped `t0` Wave-1 Occupancy (`st31`) vs. Quota-Capped Two-Wave Run (`st01`) Pair

- **Controlled Variable Changed**: `st31` blocks Wave 2 behind 8 pre-admitted `t0` `PodGroup`s (`32/32` TPUs), whereas `st01` blocks Wave 2 behind a `16`-TPU CQ ceiling.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st01-50pct-single-cq-quota-cap --namespace kueue-optimizer-chamber-st01 --port 16401`
- **Cross-Examination Assertion**: In `st31`, all 4 queued arrivals wait the full `~20s` Wave-1 remainder (`Sec 5.4 ≈ 75%` across 12 total workloads vs. `~50%` in `st01`).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `~70%–78% (100% w1, 50% w2)` (`st31`) vs. `95%–100% (16/16 TPUs)` (`st01`)
  - `Sec 5.2 Wait Time`: `AVG ~18s–20s (blocked by t0 quota)` (`st31`) vs. `P50 <=1s, AVG ~0.5*T_wave (~10s @1x)` (`st01`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st31`) vs. `0 evictions` (`st01`)
  - `Sec 5.4 Effective Sat`: `~70%–77% (12 completed workloads)` (`st31`) vs. `~50.0% (640 / 1,280 TPU-s)` (`st01`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 8-node (`32`-TPU) cluster, `cq-t0-cap` (`32` TPUs). At `t=0s`, 8 `Admitted` `JobSet`s (`init-js-00..07`, `deletionTime = 26s`) occupy all 8 nodes (`32/32` TPUs). At `t=6s`, 4 `NotAdmitted` `JobSet`s (`arr-js-00..03`, `16` TPUs, `20s`) arrive.
- **Expected Runtime Behavior**: `testrunner.py` jumps `+6s` to `t=6s` (adjusting `init-js-*` remaining runtime to `26 - 6 = 20s`), bootstraps all 8 as pre-admitted `PodGroup` `Workload`s (`Initial Kueue Workloads: 8 PodGroup(s) (8 pod(s))`, `Initial Infra Pods: 0`), and activates `cq-t0-cap` at `32/32` TPUs. All 4 `arr-js-*` arrivals are held `Pending` in Kueue for `~20s` (never admitted prematurely onto full nodes), then run from `wall=20s` to `wall=40s`.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `~70%–78% (100% w1, 50% w2)` |
| **Section `5.2` Queue Wait Time** | `AVG ~18s–20s (blocked by t0 quota)` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `~70%–77% (12 completed workloads)` |

### Detailed Metric Checkpoints

- `Execution Summary`: `Initial Kueue Workloads: 8 PodGroup(s) (8 pod(s))`, `Initial Infra Pods: 0`, `Workflows Submitted: 4/4`, `Workloads Status: ... finished=12`, `KWOK Pods Status: ... succeeded=12, pending=0, failed=0`.
- `Section 5.1 (cq-t0-cap)`: `AVG ≈ 70%–78%` (`32/32 = 100%` during `0s–20s`, `16/32 = 50%` during `20s–40s`, theoretical mean `75.0%`).
- `Section 5.2 (Wait Time)`: `AVG ≈ 17s–21s` across the 4 arriving workloads (proving `t0` `PodGroup`s genuinely held `32/32` CQ quota).
- `Section 5.3 (Evictions)`: `0`.
- `Section 5.4 (Effective Saturation)`: `Qualifying Workloads: 12 (completed=12 [12 pods])` with `Initial Infra Pods` absent (`0`) and **`EFFECTIVE SAT ≈ 70%–77%`** (`960 / 1,280 TPU-s = 75.0%`).

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ `Section 5.2 AVG < 3s` (the pre-Option-1 bug where `t0` pods went to `infra-pods` with zero `Workload`s, letting Kueue admit `arr-js-*` at `wall=0s` into node scheduling collisions).
- ❌ `Initial Infra Pods > 0` when all 8 `t0` pods belong to Kueue-admitted workloads.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
python3 testscenarios/self-tests/generate_self_test_suite.py
python3 deploy/scripts/generate_scenario_readme.py --all
python3 testscenarios/self-tests/generate_self_test_suite.py --check
```
