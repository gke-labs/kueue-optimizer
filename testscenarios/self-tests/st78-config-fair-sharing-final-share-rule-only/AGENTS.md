# Agent Execution & Cross-Examination Guide — `st78-config-fair-sharing-final-share-rule-only`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st78-config-fair-sharing-final-share-rule-only`
- **Category**: 16. Kueue Controller Configuration (`FairSharing` & `AdmissionFairSharing`)
- **Title**: Declarative Kueue `Configuration` Changing Fair Sharing Rules (`[LessThanOrEqualToFinalShare]` Only — Twin of st76)
- **Primary Verification Target**: Declarative `kind: Configuration` restricting `fairSharing.preemptionStrategies` to `[LessThanOrEqualToFinalShare]` (Rule S2-a only): evicts the `2` small `4`-TPU jobs (`2` evictions) while protecting the lumpy `24`-TPU job (`TargetNewShare=0.00 < PreemptorNewShare=0.375`)
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st78` (`--namespace kueue-optimizer-chamber-st78`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16478` | Prometheus `19125` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st76-config-fair-sharing-enabled-default`](../st76-config-fair-sharing-enabled-default/AGENTS.md), [`st77-config-fair-sharing-disabled-globally`](../st77-config-fair-sharing-disabled-globally/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST78 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st78' (API port 16478, Prom port 19125):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st78-config-fair-sharing-final-share-rule-only --namespace kueue-optimizer-chamber-st78 --port 16478 > /tmp/st78.log 2>&1 &
PID_ST78=$!

# Simultaneously launch paired ST76 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st76' (API port 16476, Prom port 19123):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st76-config-fair-sharing-enabled-default --namespace kueue-optimizer-chamber-st76 --port 16476 > /tmp/st76.log 2>&1 &
PID_ST76=$!

# Simultaneously launch paired ST77 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st77' (API port 16477, Prom port 19124):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st77-config-fair-sharing-disabled-globally --namespace kueue-optimizer-chamber-st77 --port 16477 > /tmp/st77.log 2>&1 &
PID_ST77=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST78" "$PID_ST76" "$PID_ST77"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST78 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st78' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st78-config-fair-sharing-final-share-rule-only --namespace kueue-optimizer-chamber-st78 --port 16478 | tee /tmp/st78.log

# 2. Run paired ST76 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st76':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st76-config-fair-sharing-enabled-default --namespace kueue-optimizer-chamber-st76 --port 16476 | tee /tmp/st76.log

# 3. Run paired ST77 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st77':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st77-config-fair-sharing-disabled-globally --namespace kueue-optimizer-chamber-st77 --port 16477 | tee /tmp/st77.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st78-config-fair-sharing-final-share-rule-only --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st78`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st78-config-fair-sharing-final-share-rule-only` (`kueue-optimizer-chamber-st78`) ↔ [`st76-config-fair-sharing-enabled-default`](../st76-config-fair-sharing-enabled-default/AGENTS.md) (`kueue-optimizer-chamber-st76`) — Exact Fair-Sharing Strategy Rule Twin (`[LessThanOrEqualToFinalShare]` Only `st78` vs. Default Dual-Rule `st76`)

- **Controlled Variable Changed**: `Configuration.fairSharing.preemptionStrategies` restricted to `['LessThanOrEqualToFinalShare']` (`st78`) vs. `['LessThanOrEqualToFinalShare', 'LessThanInitialShare']` (`st76`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st76-config-fair-sharing-enabled-default --namespace kueue-optimizer-chamber-st76 --port 16476`
- **Cross-Examination Assertion**: `st78` evicts only the `2` `4`-TPU jobs (`FairSharing=2`) and protects the `24`-TPU job, whereas `st76` additionally evicts the `24`-TPU job via `LessThanInitialShare` (`FairSharing=3`).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `Cohort 72%–95%` (`st78`) vs. `Cohort 78%–100%` (`st76`)
  - `Sec 5.2 Wait Time`: `p-alpha bimodal AVG ~9.5s–12s, P50 <= 2.5s` (`st78`) vs. `p-alpha AVG < 2.5s (4/4 immediate)` (`st76`)
  - `Sec 5.3 Evictions`: `2 evictions (FairSharing=2, 24-TPU protected)` (`st78`) vs. `3 evictions (FairSharing=3)` (`st76`)
  - `Sec 5.4 Effective Sat`: `~60%–78%` (`st78`) vs. `~60%–78% (all 7 jobs finish)` (`st76`)

### Pair: `st78-config-fair-sharing-final-share-rule-only` (`kueue-optimizer-chamber-st78`) ↔ [`st77-config-fair-sharing-disabled-globally`](../st77-config-fair-sharing-disabled-globally/AGENTS.md) (`kueue-optimizer-chamber-st77`) — Strict `LessThanOrEqualToFinalShare` Fair Sharing (`st78`) vs. Global Fair Sharing Disabled (`st77`)

- **Controlled Variable Changed**: `preemptionStrategies: [LessThanOrEqualToFinalShare]` (`st78`) vs. `fairSharing` disabled (`st77`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st77-config-fair-sharing-disabled-globally --namespace kueue-optimizer-chamber-st77 --port 16477`
- **Cross-Examination Assertion**: `st78` reclaims `8` TPUs (`2` evictions) immediately for `cq-alpha`, whereas `st77` reclaims `0` TPUs (`0` evictions).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `Cohort 72%–95%` (`st78`) vs. `Cohort ~75%–92%` (`st77`)
  - `Sec 5.2 Wait Time`: `p-alpha bimodal AVG ~9.5s–12s, P50 <= 2.5s` (`st78`) vs. `p-alpha AVG ~19s–23s (0/4 immediate)` (`st77`)
  - `Sec 5.3 Evictions`: `2 evictions (FairSharing=2, 24-TPU protected)` (`st78`) vs. `0 evictions (Classical Preemption)` (`st77`)
  - `Sec 5.4 Effective Sat`: `~60%–78%` (`st78`) vs. `~72%–85% (Excluded: 0)` (`st77`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: Identical 8-node (`32`-TPU) cluster, `cohort-fs-cfg`, `ClusterQueue`s, and 7 workloads as `st76` and `st77`, changing ONLY `Configuration.fairSharing.preemptionStrategies` in `infra.json` to `['LessThanOrEqualToFinalShare']`.
- **Expected Runtime Behavior**: At `t=5s`, `js-alpha-0` (`0.125 <= 0.875`) and `js-alpha-1` (`0.25 <= 0.75`) satisfy `LessThanOrEqualToFinalShare` and evict `js-beta-small-0/1` (`2` evictions, `2` `p-alpha` jobs start in `< 2s` and hold `8` TPUs until `t=25.5s`). `js-alpha-2/3` (`0.375 > TargetNewShare=0.00`) fail `LessThanOrEqualToFinalShare`, and because `LessThanInitialShare` is disabled in `Configuration`, `js-beta-large-0` (`24` TPUs) runs uninterrupted to `t=24s` (`0` `p-beta-large` evictions) while `js-alpha-2/3` wait `~19s`.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `Cohort 72%–95%` |
| **Section `5.2` Queue Wait Time** | `p-alpha bimodal AVG ~9.5s–12s, P50 <= 2.5s` |
| **Section `5.3` Evictions & Reclaims** | `2 evictions (FairSharing=2, 24-TPU protected)` |
| **Section `5.4` Effective Saturation** | `~60%–78%` |

### Detailed Metric Checkpoints

- `Section 5.3 (Total Evictions & Preemptions)`: Exact **`2` `FairSharing` preemptions** (`Total Evictions = 2`, `Unique workloads evicted once = 2`, `0` `p-beta-large` evictions — strictly between `0` in `st77` and `3` in `st76`!).
- `Section 5.2 (Wait Time)`: `p-alpha` shows bimodal wait — `2` jobs `< 2.5s` and `2` jobs `~19s` (**`AVG ≈ 9s–12s`, `P50 <= 2.5s`, `P90 ≈ 18s–25s`**); `p-beta-large` has `WORKLOADS = 1` (`AVG < 1.5s`).
- `cluster-scraper`: Preserves `preemptionStrategies: ['LessThanOrEqualToFinalShare']` in `infra.json`.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ Any eviction of `p-beta-large` (`js-beta-large-0` evicted would mean `LessThanInitialShare` was still active despite `Configuration` removing it).
- ❌ `0` evictions (would mean `LessThanOrEqualToFinalShare` failed to run).

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
