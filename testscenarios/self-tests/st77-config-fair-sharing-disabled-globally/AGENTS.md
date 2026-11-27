# Agent Execution & Cross-Examination Guide — `st77-config-fair-sharing-disabled-globally`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st77-config-fair-sharing-disabled-globally`
- **Category**: 16. Kueue Controller Configuration (`FairSharing` & `AdmissionFairSharing`)
- **Title**: Declarative Kueue `Configuration` Disabling Fair Sharing Globally (Classical Preemption — Twin of st76)
- **Primary Verification Target**: Declarative `kind: Configuration` (with `fairSharing` omitted) automatically deploying the `kueue-optimizer` chamber with global Fair Sharing disabled — without `--no-fair-sharing` CLI flags — switching Kueue to Classical Preemption (`0` evictions, `~19s–23s` wait)
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st77` (`--namespace kueue-optimizer-chamber-st77`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16477` | Prometheus `19124` (collision-free across all 95 scenarios `st01`–`st95`)
- **Mandatory Paired Counterpart(s)**: [`st76-config-fair-sharing-enabled-default`](../st76-config-fair-sharing-enabled-default/AGENTS.md), [`st78-config-fair-sharing-final-share-rule-only`](../st78-config-fair-sharing-final-share-rule-only/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st95`) and non-overlapping port pair (`16401/19048` .. `16495/19142`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST77 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st77' (API port 16477, Prom port 19124):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st77-config-fair-sharing-disabled-globally --namespace kueue-optimizer-chamber-st77 --port 16477 > /tmp/st77.log 2>&1 &
PID_ST77=$!

# Simultaneously launch paired ST76 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st76' (API port 16476, Prom port 19123):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st76-config-fair-sharing-enabled-default --namespace kueue-optimizer-chamber-st76 --port 16476 > /tmp/st76.log 2>&1 &
PID_ST76=$!

# Simultaneously launch paired ST78 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st78' (API port 16478, Prom port 19125):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st78-config-fair-sharing-final-share-rule-only --namespace kueue-optimizer-chamber-st78 --port 16478 > /tmp/st78.log 2>&1 &
PID_ST78=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST77" "$PID_ST76" "$PID_ST78"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST77 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st77' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st77-config-fair-sharing-disabled-globally --namespace kueue-optimizer-chamber-st77 --port 16477 | tee /tmp/st77.log

# 2. Run paired ST76 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st76':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st76-config-fair-sharing-enabled-default --namespace kueue-optimizer-chamber-st76 --port 16476 | tee /tmp/st76.log

# 3. Run paired ST78 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st78':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st78-config-fair-sharing-final-share-rule-only --namespace kueue-optimizer-chamber-st78 --port 16478 | tee /tmp/st78.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st77-config-fair-sharing-disabled-globally --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st77`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st77-config-fair-sharing-disabled-globally` (`kueue-optimizer-chamber-st77`) ↔ [`st76-config-fair-sharing-enabled-default`](../st76-config-fair-sharing-enabled-default/AGENTS.md) (`kueue-optimizer-chamber-st76`) — Exact Global Fair-Sharing A/B Twin (`fairSharing` Disabled Globally `st77` vs. Enabled `st76`)

- **Controlled Variable Changed**: `Configuration` omits `fairSharing` (`st77`) vs. enables `fairSharing` (`st76`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st76-config-fair-sharing-enabled-default --namespace kueue-optimizer-chamber-st76 --port 16476`
- **Cross-Examination Assertion**: `st77` proves the `Configuration` statement in `infra.json` disables Fair Sharing in the deployed chamber (`0` evictions, `p-alpha` wait `~19s`) even when `ClusterQueue`s specify `reclaimWithinCohort: Any` and `fairSharing.weight: 1`.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `Cohort ~75%–92%` (`st77`) vs. `Cohort 78%–100%` (`st76`)
  - `Sec 5.2 Wait Time`: `p-alpha AVG ~19s–23s (0/4 immediate)` (`st77`) vs. `p-alpha AVG < 2.5s (4/4 immediate)` (`st76`)
  - `Sec 5.3 Evictions`: `0 evictions (Classical Preemption)` (`st77`) vs. `3 evictions (FairSharing=3)` (`st76`)
  - `Sec 5.4 Effective Sat`: `~72%–85% (Excluded: 0)` (`st77`) vs. `~60%–78% (all 7 jobs finish)` (`st76`)

### Pair: `st77-config-fair-sharing-disabled-globally` (`kueue-optimizer-chamber-st77`) ↔ [`st78-config-fair-sharing-final-share-rule-only`](../st78-config-fair-sharing-final-share-rule-only/AGENTS.md) (`kueue-optimizer-chamber-st78`) — Global Fair-Sharing Disabled (`st77`) vs. Strict `LessThanOrEqualToFinalShare` Only (`st78`)

- **Controlled Variable Changed**: `fairSharing` omitted (`0` strategies, `st77`) vs. `preemptionStrategies: [LessThanOrEqualToFinalShare]` (`st78`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st78-config-fair-sharing-final-share-rule-only --namespace kueue-optimizer-chamber-st78 --port 16478`
- **Cross-Examination Assertion**: Across `st77 -> st78 -> st76`, `FairSharing` preemptions step monotonically **`0 -> 2 -> 3`** and immediate `p-alpha` admissions step **`0/4 -> 2/4 -> 4/4`**.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `Cohort ~75%–92%` (`st77`) vs. `Cohort 72%–95%` (`st78`)
  - `Sec 5.2 Wait Time`: `p-alpha AVG ~19s–23s (0/4 immediate)` (`st77`) vs. `p-alpha bimodal AVG ~9.5s–12s, P50 <= 2.5s` (`st78`)
  - `Sec 5.3 Evictions`: `0 evictions (Classical Preemption)` (`st77`) vs. `2 evictions (FairSharing=2, 24-TPU protected)` (`st78`)
  - `Sec 5.4 Effective Sat`: `~72%–85% (Excluded: 0)` (`st77`) vs. `~60%–78%` (`st78`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: Identical 8-node (`32`-TPU) cluster, `cohort-fs-cfg`, `ClusterQueue`s (`reclaimWithinCohort: Any`, `fairSharing.weight: 1`), and 7 workloads as `st76`, changing ONLY `kind: Configuration` in `infra.json` to omit `fairSharing` (`cfg.FairSharing == nil`).
- **Expected Runtime Behavior**: `testrunner.py` reads `Configuration` from `infra.json` and deploys `kueue-controller-manager` with `fairSharing` removed from `ConfigMap/kueue-manager-config`. Under Classical Preemption, `cq-alpha` (`nominalQuota: 0`, `borrowWithinCohort.policy: Never`) cannot preempt while borrowing even with `reclaimWithinCohort: Any` and `p-alpha=300`, so `0` jobs are evicted and all `4` `p-alpha` jobs wait `~19s` until `t=24s`.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `Cohort ~75%–92%` |
| **Section `5.2` Queue Wait Time** | `p-alpha AVG ~19s–23s (0/4 immediate)` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions (Classical Preemption)` |
| **Section `5.4` Effective Saturation** | `~72%–85% (Excluded: 0)` |

### Detailed Metric Checkpoints

- `Section 5.3 (Total Evictions & Preemptions)`: Strictly **`0`** (`Total Evictions = 0, FairSharing = 0`, vs. `3` in `st76`).
- `Section 5.2 (Wait Time)`: `p-alpha` wait jumps from `< 2.5s` (`st76`) to **`AVG ≈ 19s–23s`** (`P50 ≈ 30s` bucket interpolation over `[20s, 40s]`).
- `Section 4 (Not Emitted)`: `kueue_cluster_queue_weighted_share` is absent because global Fair Sharing is disabled.
- `Section 5.4 (Effective Saturation)`: **`~72%–85%`** (`Excluded: 0`, zero wasted compute).
- `cluster-scraper`: Records `kind: Configuration` without `fairSharing` in `infra.json`.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ Any `FairSharing` eviction (`Section 5.3 > 0`), which would prove the chamber deployed with default `fairSharing` instead of obeying the scenario's `Configuration` statement.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
