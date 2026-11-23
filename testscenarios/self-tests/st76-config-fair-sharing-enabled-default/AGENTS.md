# Agent Execution & Cross-Examination Guide — `st76-config-fair-sharing-enabled-default`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st76-config-fair-sharing-enabled-default`
- **Category**: 16. Kueue Controller Configuration (`FairSharing` & `AdmissionFairSharing`)
- **Title**: Declarative Kueue `Configuration` Enabling Global Fair Sharing (`[LessThanOrEqualToFinalShare, LessThanInitialShare]`)
- **Primary Verification Target**: Declarative `kind: Configuration` in `infra.json` enabling global Fair Sharing with both `LessThanOrEqualToFinalShare` (S2-a) and `LessThanInitialShare` (S2-b), evicting all `3` `cq-beta` incumbents (`2×4` TPUs + `1×24` TPUs) so all `4` `cq-alpha` challengers start immediately
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st76` (`--namespace kueue-optimizer-chamber-st76`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16476` | Prometheus `19123` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st77-config-fair-sharing-disabled-globally`](../st77-config-fair-sharing-disabled-globally/AGENTS.md), [`st78-config-fair-sharing-final-share-rule-only`](../st78-config-fair-sharing-final-share-rule-only/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST76 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st76' (API port 16476, Prom port 19123):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st76-config-fair-sharing-enabled-default --namespace kueue-optimizer-chamber-st76 --port 16476 > /tmp/st76.log 2>&1 &
PID_ST76=$!

# Simultaneously launch paired ST77 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st77' (API port 16477, Prom port 19124):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st77-config-fair-sharing-disabled-globally --namespace kueue-optimizer-chamber-st77 --port 16477 > /tmp/st77.log 2>&1 &
PID_ST77=$!

# Simultaneously launch paired ST78 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st78' (API port 16478, Prom port 19125):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st78-config-fair-sharing-final-share-rule-only --namespace kueue-optimizer-chamber-st78 --port 16478 > /tmp/st78.log 2>&1 &
PID_ST78=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST76" "$PID_ST77" "$PID_ST78"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST76 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st76' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st76-config-fair-sharing-enabled-default --namespace kueue-optimizer-chamber-st76 --port 16476 | tee /tmp/st76.log

# 2. Run paired ST77 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st77':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st77-config-fair-sharing-disabled-globally --namespace kueue-optimizer-chamber-st77 --port 16477 | tee /tmp/st77.log

# 3. Run paired ST78 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st78':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st78-config-fair-sharing-final-share-rule-only --namespace kueue-optimizer-chamber-st78 --port 16478 | tee /tmp/st78.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st76-config-fair-sharing-enabled-default --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st76`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st76-config-fair-sharing-enabled-default` (`kueue-optimizer-chamber-st76`) ↔ [`st77-config-fair-sharing-disabled-globally`](../st77-config-fair-sharing-disabled-globally/AGENTS.md) (`kueue-optimizer-chamber-st77`) — Exact Global Fair-Sharing A/B Twin (`fairSharing` Enabled `st76` vs. Disabled Globally `st77` via `Configuration`)

- **Controlled Variable Changed**: `Configuration.fairSharing` enabled (`[LessThanOrEqualToFinalShare, LessThanInitialShare]`, `st76`) vs. omitted (`cfg.FairSharing == nil`, `st77`) in `infra.json` on identical queues (`reclaimWithinCohort: Any`) and workloads.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st77-config-fair-sharing-disabled-globally --namespace kueue-optimizer-chamber-st77 --port 16477`
- **Cross-Examination Assertion**: `Sec 5.3` flips from **`3` `FairSharing` preemptions (`st76`)** to **`0` evictions (`st77`)**, and `Sec 5.2` `p-alpha` wait jumps from **`< 2.5s` (`st76`)** to **`~19s` (`st77`)** without any CLI flags.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `Cohort 78%–100%` (`st76`) vs. `Cohort ~75%–92%` (`st77`)
  - `Sec 5.2 Wait Time`: `p-alpha AVG < 2.5s (4/4 immediate)` (`st76`) vs. `p-alpha AVG ~19s–23s (0/4 immediate)` (`st77`)
  - `Sec 5.3 Evictions`: `3 evictions (FairSharing=3)` (`st76`) vs. `0 evictions (Classical Preemption)` (`st77`)
  - `Sec 5.4 Effective Sat`: `~60%–78% (all 7 jobs finish)` (`st76`) vs. `~72%–85% (Excluded: 0)` (`st77`)

### Pair: `st76-config-fair-sharing-enabled-default` (`kueue-optimizer-chamber-st76`) ↔ [`st78-config-fair-sharing-final-share-rule-only`](../st78-config-fair-sharing-final-share-rule-only/AGENTS.md) (`kueue-optimizer-chamber-st78`) — Exact Fair-Sharing Strategy Rule Twin (`[LessThanOrEqualToFinalShare, LessThanInitialShare]` `st76` vs. `[LessThanOrEqualToFinalShare]` Only `st78`)

- **Controlled Variable Changed**: Removing `LessThanInitialShare` from `Configuration.fairSharing.preemptionStrategies` in `st78`.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st78-config-fair-sharing-final-share-rule-only --namespace kueue-optimizer-chamber-st78 --port 16478`
- **Cross-Examination Assertion**: `Sec 5.3` drops from **`3` preemptions (`st76`)** to **`2` preemptions (`st78`)**, protecting `cq-beta`'s `24`-TPU job (`TargetNewShare=0.00 < PreemptorNewShare=0.375`) while shifting `p-alpha` wait from `< 2.5s` (`4/4` immediate) to bimodal `AVG ~10.5s` (`2/4` immediate, `2/4` `~19s`).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `Cohort 78%–100%` (`st76`) vs. `Cohort 72%–95%` (`st78`)
  - `Sec 5.2 Wait Time`: `p-alpha AVG < 2.5s (4/4 immediate)` (`st76`) vs. `p-alpha bimodal AVG ~9.5s–12s, P50 <= 2.5s` (`st78`)
  - `Sec 5.3 Evictions`: `3 evictions (FairSharing=3)` (`st76`) vs. `2 evictions (FairSharing=2, 24-TPU protected)` (`st78`)
  - `Sec 5.4 Effective Sat`: `~60%–78% (all 7 jobs finish)` (`st76`) vs. `~60%–78%` (`st78`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 8-node (`32`-TPU) cluster with `kind: Configuration` (`fairSharing.preemptionStrategies: [LessThanOrEqualToFinalShare, LessThanInitialShare]`). `cohort-fs-cfg` holds `32` TPUs; `cq-beta` and `cq-alpha` both have `nominalQuota: 0`, `fairSharing.weight: 1`, `reclaimWithinCohort: Any`, `borrowWithinCohort.policy: Never`. At `t=0s`, `cq-beta` starts `2` `4`-TPU jobs (`p-beta-small=100`, `24s`) + `1` `24`-TPU job (`p-beta-large=200`, `24s`). At `t=5s`, `cq-alpha` submits `4` `4`-TPU jobs (`p-alpha=300`, `20s`).
- **Expected Runtime Behavior**: `testrunner.py` reads `Configuration` from `infra.json` and deploys `kueue-controller-manager` with both S2-a and S2-b active. At `t=5s`, `js-alpha-0` and `js-alpha-1` evict `js-beta-small-0/1` via `LessThanOrEqualToFinalShare`, while `js-alpha-2` falls back to `LessThanInitialShare` (`0.375 < 0.75`) to evict `js-beta-large-0`, admitting all `4/4` `p-alpha` jobs in `< 2.5s`.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `Cohort 78%–100%` |
| **Section `5.2` Queue Wait Time** | `p-alpha AVG < 2.5s (4/4 immediate)` |
| **Section `5.3` Evictions & Reclaims** | `3 evictions (FairSharing=3)` |
| **Section `5.4` Effective Saturation** | `~60%–78% (all 7 jobs finish)` |

### Detailed Metric Checkpoints

- `Section 5.3 (Total Evictions & Preemptions)`: **`3` `FairSharing` preemptions** (`Total Evictions = 3`, `Unique workloads evicted once = 3`, `FairSharing = 3`: `2` `p-beta-small` + `1` `p-beta-large`).
- `Section 5.2 (Wait Time)`: `p-alpha` admits immediately (**`P50 < 2.5s, P90 < 3.5s, AVG < 2.5s`**).
- `Section 1 Gauge`: `kueue_cluster_queue_weighted_share` actively emitted for `cq-alpha` and `cq-beta`.
- `cluster-scraper`: Records sanitized `kind: Configuration` (`fairSharing.preemptionStrategies: [LessThanOrEqualToFinalShare, LessThanInitialShare]`) in `infra.json`.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ `p-alpha` waiting `~19s–23s` (would mean `Configuration` failed to enable Fair Sharing).
- ❌ Fewer than `3` `FairSharing` preemptions (would mean `LessThanInitialShare` fallback was not configured).

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
