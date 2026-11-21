# Agent Execution & Cross-Examination Guide — `st26-t0-initial-running-pods-baseline`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and re-run:
> ```bash
> python3 testscenarios/self-tests/generate_self_test_suite.py
> python3 deploy/scripts/generate_scenario_readme.py --all
> python3 testscenarios/self-tests/generate_self_test_suite.py --check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st26-t0-initial-running-pods-baseline`
- **Category**: 6. Tricky Accounting & Edge Cases
- **Title**: Coexistence With Pre-Existing `t0` Unmanaged Pods (`infra-pods` Namespace)
- **Primary Verification Target**: Verifies `infra-pods` (`kueue.x-k8s.io/exclude=true`) placement on `st-node-00..03` alongside Kueue jobs on `st-node-04..07`
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st26` (`--namespace kueue-optimizer-chamber-st26`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16426` | Prometheus `19073` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st32-t0-mixed-kueue-workloads-and-infra-pods`](../st32-t0-mixed-kueue-workloads-and-infra-pods/AGENTS.md), [`st01-50pct-single-cq-quota-cap`](../st01-50pct-single-cq-quota-cap/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST26 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st26' (API port 16426, Prom port 19073):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st26-t0-initial-running-pods-baseline --namespace kueue-optimizer-chamber-st26 --port 16426 > /tmp/st26.log 2>&1 &
PID_ST26=$!

# Simultaneously launch paired ST32 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st32' (API port 16432, Prom port 19079):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st32-t0-mixed-kueue-workloads-and-infra-pods --namespace kueue-optimizer-chamber-st32 --port 16432 > /tmp/st32.log 2>&1 &
PID_ST32=$!

# Simultaneously launch paired ST01 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st01' (API port 16401, Prom port 19048):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st01-50pct-single-cq-quota-cap --namespace kueue-optimizer-chamber-st01 --port 16401 > /tmp/st01.log 2>&1 &
PID_ST01=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST26" "$PID_ST32" "$PID_ST01"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST26 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st26' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st26-t0-initial-running-pods-baseline --namespace kueue-optimizer-chamber-st26 --port 16426 | tee /tmp/st26.log

# 2. Run paired ST32 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st32':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st32-t0-mixed-kueue-workloads-and-infra-pods --namespace kueue-optimizer-chamber-st32 --port 16432 | tee /tmp/st32.log

# 3. Run paired ST01 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st01':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st01-50pct-single-cq-quota-cap --namespace kueue-optimizer-chamber-st01 --port 16401 | tee /tmp/st01.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st26-t0-initial-running-pods-baseline --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st26`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st26-t0-initial-running-pods-baseline` (`kueue-optimizer-chamber-st26`) ↔ [`st32-t0-mixed-kueue-workloads-and-infra-pods`](../st32-t0-mixed-kueue-workloads-and-infra-pods/AGENTS.md) (`kueue-optimizer-chamber-st32`) — Pure Non-Kueue `infra-pods` (`st26`) vs. Hybrid `t0` Kueue `PodGroup`s + `infra-pods` (`st32`) Pair

- **Controlled Variable Changed**: `st26` places 4 unmanaged `infra-pods` (`16` TPUs) alongside 4 `t=0s` Kueue jobs (`16` TPUs), whereas `st32` places 2 unmanaged `infra-pods` (`8` TPUs) + 4 `t0` Kueue `PodGroup`s (`16` TPUs) + 2 `t=0s` Kueue jobs (`8` TPUs).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st32-t0-mixed-kueue-workloads-and-infra-pods --namespace kueue-optimizer-chamber-st32 --port 16432`
- **Cross-Examination Assertion**: Both scenarios pack `32/32` physical TPUs (`Sec 5.4 ≈ 88%–98%`) with zero node collisions, while `Sec 5.1` reflects `16/16` CQ TPUs in `st26` vs. `24/24` CQ TPUs (`16` `t0` + `8` arriving) in `st32`.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `95%–100% (16/16 CQ TPUs)` (`st26`) vs. `~92%–100% (24/24 CQ TPUs)` (`st32`)
  - `Sec 5.2 Wait Time`: `< 1.0s` (`st26`) vs. `< 1.5s` (`st32`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st26`) vs. `0 evictions` (`st32`)
  - `Sec 5.4 Effective Sat`: `~88%–96% (4 Kueue + 4 infra-pods = 32/32 TPUs)` (`st26`) vs. `~90%–98% (6 Kueue + 2 infra = 32/32 TPUs)` (`st32`)

### Pair: `st26-t0-initial-running-pods-baseline` (`kueue-optimizer-chamber-st26`) ↔ [`st01-50pct-single-cq-quota-cap`](../st01-50pct-single-cq-quota-cap/AGENTS.md) (`kueue-optimizer-chamber-st01`) — Unmanaged `t0` `infra-pods` Coexistence (`st26`) vs. Half-Cluster CQ Cap (`st01`) Pair

- **Controlled Variable Changed**: `st26` occupies `st-node-00..03` with 4 `infra-pods` (`16` TPUs) while `cq-coexist` (`16` TPUs) runs 4 jobs on `st-node-04..07`; `st01` leaves 4 nodes idle.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st01-50pct-single-cq-quota-cap --namespace kueue-optimizer-chamber-st01 --port 16401`
- **Cross-Examination Assertion**: `Sec 5.1 AVG` is `~100%` (`16/16`) in both, while `Sec 5.4 EFFECTIVE SAT` jumps from `~50%` (`st01`) to `~88%–96%` (`st26`) because `saturation.py` accounts for `Initial Infra Pods: 4`.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `95%–100% (16/16 CQ TPUs)` (`st26`) vs. `95%–100% (16/16 TPUs)` (`st01`)
  - `Sec 5.2 Wait Time`: `< 1.0s` (`st26`) vs. `P50 <=1s, AVG ~0.5*T_wave (~10s @1x)` (`st01`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st26`) vs. `0 evictions` (`st01`)
  - `Sec 5.4 Effective Sat`: `~88%–96% (4 Kueue + 4 infra-pods = 32/32 TPUs)` (`st26`) vs. `~50.0% (640 / 1,280 TPU-s)` (`st01`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 8-node (`32`-TPU) cluster. At `t0`, 4 pre-existing unmanaged pods (`legacy-pod-0..3`, `4` TPUs each = `16` TPUs, `28s` remaining runtime) occupy `st-node-00..03`. `cq-coexist` (`16` TPUs) receives 4 Kueue jobs (`16` TPUs, `32s`) at `t=0s`.
- **Expected Runtime Behavior**: `testrunner.py` creates namespace `infra-pods` with label `kueue.x-k8s.io/exclude: 'true'`, places all 4 `legacy-pod-*` onto `st-node-00..03` (`Initial Infra Pods: 4`), and schedules all 4 Kueue `JobSet` pods onto `st-node-04..07` with zero node resource collisions. In `Section 5.4`, both the 4 Kueue-managed workloads (`Qualifying Workloads: 4`) and the 4 pre-existing `infra-pods` (`Initial Infra Pods: 4`) are accounted for (`32/32` TPUs = **`~88%–96%`** effective uninterrupted cluster saturation).

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `95%–100% (16/16 CQ TPUs)` |
| **Section `5.2` Queue Wait Time** | `< 1.0s` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `~88%–96% (4 Kueue + 4 infra-pods = 32/32 TPUs)` |

### Detailed Metric Checkpoints

- `Execution Summary`: `Initial Infra Pods: 4` and `KWOK Pods Status: ... succeeded=8` (`4` initial + `4` JobSet pods).
- `Section 5.1 (cq-coexist)`: **`95%–100%`** (`16/16` TPUs).
- `Section 5.4 (Effective Saturation)`: **`~88%–96%`** (`8` total pods × `4` TPUs / `32` cluster TPUs = `4` Kueue workloads + `4` initial `infra-pods`), with `Qualifying Workloads: 4` and `Initial Infra Pods: 4` confirming zero rogue Kueue `Workload` objects were created for `infra-pods` while full physical cluster occupancy is captured.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ `Initial Infra Pods: 0` or Kueue pods colliding onto `st-node-00..03` while `legacy-pod-*` hold all `4` TPUs on those nodes.
- ❌ Kueue creating rogue `Workload` objects for pods in `infra-pods`.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
python3 testscenarios/self-tests/generate_self_test_suite.py
python3 deploy/scripts/generate_scenario_readme.py --all
python3 testscenarios/self-tests/generate_self_test_suite.py --check
```
