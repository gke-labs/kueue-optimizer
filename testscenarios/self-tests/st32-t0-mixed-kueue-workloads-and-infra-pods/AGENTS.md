# Agent Execution & Cross-Examination Guide — `st32-t0-mixed-kueue-workloads-and-infra-pods`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st32-t0-mixed-kueue-workloads-and-infra-pods`
- **Category**: 7. Initial t0 Workloads — Handling & Accounting
- **Title**: Hybrid `t0` Partitioning: 4 Pre-Admitted Kueue `PodGroup`s + 2 Non-Kueue `infra-pods` + 2 New Arrivals
- **Primary Verification Target**: Simultaneous separation of Class 1 `t0` Kueue `PodGroup`s (`16` TPUs in `cq-hybrid`) and Class 2 `infra-pods` (`8` TPUs excluded from Kueue) alongside `8` TPUs of `t=0s` arrivals
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st32` (`--namespace kueue-optimizer-chamber-st32`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16432` | Prometheus `19079` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st26-t0-initial-running-pods-baseline`](../st26-t0-initial-running-pods-baseline/AGENTS.md), [`st33-t0-legacy-unlinked-and-synthesized-pods`](../st33-t0-legacy-unlinked-and-synthesized-pods/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST32 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st32' (API port 16432, Prom port 19079):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st32-t0-mixed-kueue-workloads-and-infra-pods --namespace kueue-optimizer-chamber-st32 --port 16432 > /tmp/st32.log 2>&1 &
PID_ST32=$!

# Simultaneously launch paired ST26 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st26' (API port 16426, Prom port 19073):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st26-t0-initial-running-pods-baseline --namespace kueue-optimizer-chamber-st26 --port 16426 > /tmp/st26.log 2>&1 &
PID_ST26=$!

# Simultaneously launch paired ST33 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st33' (API port 16433, Prom port 19080):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st33-t0-legacy-unlinked-and-synthesized-pods --namespace kueue-optimizer-chamber-st33 --port 16433 > /tmp/st33.log 2>&1 &
PID_ST33=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST32" "$PID_ST26" "$PID_ST33"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST32 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st32' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st32-t0-mixed-kueue-workloads-and-infra-pods --namespace kueue-optimizer-chamber-st32 --port 16432 | tee /tmp/st32.log

# 2. Run paired ST26 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st26':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st26-t0-initial-running-pods-baseline --namespace kueue-optimizer-chamber-st26 --port 16426 | tee /tmp/st26.log

# 3. Run paired ST33 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st33':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st33-t0-legacy-unlinked-and-synthesized-pods --namespace kueue-optimizer-chamber-st33 --port 16433 | tee /tmp/st33.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st32-t0-mixed-kueue-workloads-and-infra-pods --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st32`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st32-t0-mixed-kueue-workloads-and-infra-pods` (`kueue-optimizer-chamber-st32`) ↔ [`st26-t0-initial-running-pods-baseline`](../st26-t0-initial-running-pods-baseline/AGENTS.md) (`kueue-optimizer-chamber-st26`) — Hybrid `t0` Kueue + `infra-pods` (`st32`) vs. Pure `infra-pods` (`st26`) Pair

- **Controlled Variable Changed**: Replaces 2 of `st26`'s unmanaged `infra-pods` and 2 `t=0s` arrivals with 4 pre-admitted `t0` Kueue `PodGroup`s (`Initial Kueue Workloads: 4`, `Initial Infra Pods: 2`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st26-t0-initial-running-pods-baseline --namespace kueue-optimizer-chamber-st26 --port 16426`
- **Cross-Examination Assertion**: Both pack `32/32` physical TPUs (`Sec 5.4 ≈ 90%–98%`) with `< 1.5s` wait and `0` collisions, while `cq-hybrid` `Sec 5.1` accounts for all `24` Kueue TPUs (`16` `t0` + `8` arriving).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `~92%–100% (24/24 CQ TPUs)` (`st32`) vs. `95%–100% (16/16 CQ TPUs)` (`st26`)
  - `Sec 5.2 Wait Time`: `< 1.5s` (`st32`) vs. `< 1.0s` (`st26`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st32`) vs. `0 evictions` (`st26`)
  - `Sec 5.4 Effective Sat`: `~90%–98% (6 Kueue + 2 infra = 32/32 TPUs)` (`st32`) vs. `~88%–96% (4 Kueue + 4 infra-pods = 32/32 TPUs)` (`st26`)

### Pair: `st32-t0-mixed-kueue-workloads-and-infra-pods` (`kueue-optimizer-chamber-st32`) ↔ [`st33-t0-legacy-unlinked-and-synthesized-pods`](../st33-t0-legacy-unlinked-and-synthesized-pods/AGENTS.md) (`kueue-optimizer-chamber-st33`) — Tier-1 Enriched Pod Linkage (`st32`) vs. Tier-3/4 Legacy Unlinked & Synthesized Pods (`st33`) Pair

- **Controlled Variable Changed**: `st32` uses explicit `workloadName`/`ownerName` pod linkage plus true `infra-pods`, whereas `st33` exercises Tier-3 hashed unlinked pod matching and Tier-4 missing-pod synthesis.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st33-t0-legacy-unlinked-and-synthesized-pods --namespace kueue-optimizer-chamber-st33 --port 16433`
- **Cross-Examination Assertion**: `Initial Infra Pods` is `2` in `st32` vs. `0` in `st33` (confirming Tier 3 claimed all 4 unlinked hashed pods).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `~92%–100% (24/24 CQ TPUs)` (`st32`) vs. `~90%–100% (32/32 TPUs)` (`st33`)
  - `Sec 5.2 Wait Time`: `< 1.5s` (`st32`) vs. `< 1.5s` (`st33`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st32`) vs. `0 evictions` (`st33`)
  - `Sec 5.4 Effective Sat`: `~90%–98% (6 Kueue + 2 infra = 32/32 TPUs)` (`st32`) vs. `~88%–98% (8 Kueue workloads, 0 infra)` (`st33`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 8-node (`32`-TPU) cluster. At `t0`: `st-node-00..01` run 2 unmanaged `sys-daemon-*` pods (`8` TPUs, no `workloadName`); `st-node-02..05` run 4 `Admitted` `init-kueue-*` `JobSet`s (`16` TPUs in `cq-hybrid`, `nominalQuota = 24` TPUs). At `t=0s`, 2 `NotAdmitted` `arr-kueue-*` `JobSet`s (`8` TPUs, `30s`) arrive.
- **Expected Runtime Behavior**: `sys-daemon-0..1` are isolated in `infra-pods` (`0` CQ quota impact); `init-kueue-0..3` become pre-admitted `PodGroup` `Workload`s consuming `16/24` TPUs of `cq-hybrid`; `arr-kueue-0..1` admit immediately (`16 + 8 = 24/24` CQ TPUs) onto free `st-node-06..07` (`32/32` physical TPUs packed cleanly).

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `~92%–100% (24/24 CQ TPUs)` |
| **Section `5.2` Queue Wait Time** | `< 1.5s` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `~90%–98% (6 Kueue + 2 infra = 32/32 TPUs)` |

### Detailed Metric Checkpoints

- `Execution Summary`: `Initial Kueue Workloads: 4 PodGroup(s) (4 pod(s))`, `Initial Infra Pods: 2`, `Workflows Submitted: 2/2`, `KWOK Pods Status: ... succeeded=8`.
- `Section 5.1 (cq-hybrid)`: **`~92%–100%`** (`24 / 24` CQ TPUs).
- `Section 5.4 (Effective Saturation)`: `Qualifying Workloads: 6 (completed=6 [6 pods])` + `Initial Infra Pods: 2 (completed=2, active_at_end=0)` → **`EFFECTIVE SAT ≈ 90%–98%`** (`32 / 32` cluster TPUs).

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ `sys-daemon-*` consuming `cq-hybrid` quota (which would push demand to `32 > 24` and block `arr-kueue-*`).
- ❌ `init-kueue-*` leaking into `infra-pods` (which would drop `cq-hybrid` `Sec 5.1` from `100%` to `33.3%`).

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
