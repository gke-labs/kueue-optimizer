# Agent Execution & Cross-Examination Guide — `st23-heavy-asymmetric-fair-sharing-7-to-1`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st23-heavy-asymmetric-fair-sharing-7-to-1`
- **Category**: 5. Fair Sharing & Priority Thresholds
- **Title**: Heavy Asymmetric `7 : 1` Fair Sharing (`cq-gold = 28 TPUs [87.5%]` vs `cq-bronze = 4 TPUs [12.5%]`)
- **Primary Verification Target**: Exact `7 : 1` weight ratio mapping onto 8 physical nodes (`7` nodes for `cq-gold`, `1` node for `cq-bronze`)
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st23` (`--namespace kueue-optimizer-chamber-st23`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16423` | Prometheus `19070` (collision-free across all 95 scenarios `st01`–`st95`)
- **Mandatory Paired Counterpart(s)**: [`st22-equal-fair-sharing-50-50-split`](../st22-equal-fair-sharing-50-50-split/AGENTS.md), [`st24-geometric-6cq-fair-sharing-hierarchy`](../st24-geometric-6cq-fair-sharing-hierarchy/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st95`) and non-overlapping port pair (`16401/19048` .. `16495/19142`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST23 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st23' (API port 16423, Prom port 19070):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st23-heavy-asymmetric-fair-sharing-7-to-1 --namespace kueue-optimizer-chamber-st23 --port 16423 > /tmp/st23.log 2>&1 &
PID_ST23=$!

# Simultaneously launch paired ST22 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st22' (API port 16422, Prom port 19069):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st22-equal-fair-sharing-50-50-split --namespace kueue-optimizer-chamber-st22 --port 16422 > /tmp/st22.log 2>&1 &
PID_ST22=$!

# Simultaneously launch paired ST24 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st24' (API port 16424, Prom port 19071):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st24-geometric-6cq-fair-sharing-hierarchy --namespace kueue-optimizer-chamber-st24 --port 16424 > /tmp/st24.log 2>&1 &
PID_ST24=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST23" "$PID_ST22" "$PID_ST24"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST23 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st23' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st23-heavy-asymmetric-fair-sharing-7-to-1 --namespace kueue-optimizer-chamber-st23 --port 16423 | tee /tmp/st23.log

# 2. Run paired ST22 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st22':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st22-equal-fair-sharing-50-50-split --namespace kueue-optimizer-chamber-st22 --port 16422 | tee /tmp/st22.log

# 3. Run paired ST24 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st24':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st24-geometric-6cq-fair-sharing-hierarchy --namespace kueue-optimizer-chamber-st24 --port 16424 | tee /tmp/st24.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st23-heavy-asymmetric-fair-sharing-7-to-1 --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st23`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st23-heavy-asymmetric-fair-sharing-7-to-1` (`kueue-optimizer-chamber-st23`) ↔ [`st22-equal-fair-sharing-50-50-split`](../st22-equal-fair-sharing-50-50-split/AGENTS.md) (`kueue-optimizer-chamber-st22`) — Skewed `7:1` (`st23`) vs. Equal `1:1` (`st22`) Fair-Sharing Pair

- **Controlled Variable Changed**: CQ weights `7 : 1` (`st23`) vs. `1 : 1` (`st22`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st22-equal-fair-sharing-50-50-split --namespace kueue-optimizer-chamber-st22 --port 16422`
- **Cross-Examination Assertion**: `cq-gold` (`weight 7`) runs `7x` as many concurrent jobs as `cq-bronze` (`weight 1`) during `0s–32s`, then `cq-bronze` work-conserves all 8 nodes from `32s–48s`.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `Cohort ~80%–92% (87.5% vs 12.5% in 0s–32s)` (`st23`) vs. `Cohort 92%–100% (50/50 split)` (`st22`)
  - `Sec 5.2 Wait Time`: `cq-gold finishes all 14 in 2 waves (~32s)` (`st23`) vs. `Symmetric across both CQs (AVG ~16s)` (`st22`)
  - `Sec 5.3 Evictions`: `0 steady-state (<= 6 t=0s convergence FairSharing)` (`st23`) vs. `0 evictions` (`st22`)
  - `Sec 5.4 Effective Sat`: `~76%–85%` (`st23`) vs. `90%–99%` (`st22`)

### Pair: `st23-heavy-asymmetric-fair-sharing-7-to-1` (`kueue-optimizer-chamber-st23`) ↔ [`st24-geometric-6cq-fair-sharing-hierarchy`](../st24-geometric-6cq-fair-sharing-hierarchy/AGENTS.md) (`kueue-optimizer-chamber-st24`) — 2-CQ `7:1` Skew (`st23`) vs. 6-CQ Geometric `1..32` Hierarchy (`st24`) Pair

- **Controlled Variable Changed**: Scales from 2 weighted CQs on 8 nodes (`32` TPUs, `st23`) to 6 geometrically weighted CQs (`1, 2, 4, 8, 16, 32`) on 16 nodes (`64` TPUs, `st24`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st24-geometric-6cq-fair-sharing-hierarchy --namespace kueue-optimizer-chamber-st24 --port 16424`
- **Cross-Examination Assertion**: Both scenarios preserve inverse correlation between `fairSharing.weight` and mean queue wait time without starving the cluster.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `Cohort ~80%–92% (87.5% vs 12.5% in 0s–32s)` (`st23`) vs. `Cohort P50=100%, AVG ~70%–86%` (`st24`)
  - `Sec 5.2 Wait Time`: `cq-gold finishes all 14 in 2 waves (~32s)` (`st23`) vs. `Wait inversely proportional to weight` (`st24`)
  - `Sec 5.3 Evictions`: `0 steady-state (<= 6 t=0s convergence FairSharing)` (`st23`) vs. `0 steady-state (<= 6 t=0s convergence FairSharing)` (`st24`)
  - `Sec 5.4 Effective Sat`: `~76%–85%` (`st23`) vs. `~68%–80% (2,160 TPU-s work)` (`st24`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 8-node (`32`-TPU) cluster, `cohort-fs-skew` (`32` Cohort TPUs). `cq-gold` has `fairSharing.weight: 7` (exact integer `7/8 = 28` TPUs = `7` nodes); `cq-bronze` has `fairSharing.weight: 1` (`1/8 = 4` TPUs = `1` node). At `t=0s`, `cq-gold` submits 14 jobs (`2` waves of `7`) and `cq-bronze` submits 6 jobs (`6` waves; `1` active during `0s–32s`, then `4` concurrent in `32s–48s`).
- **Expected Runtime Behavior**: Because `7 + 1 = 8` matches the 8-node cluster with zero rounding error, Kueue converges immediately (`< 1s`, using at most a few sub-second `InCohortFairSharing` convergence preemptions if parallel `t=0s` `JobSet` creation reconciles a `cq-bronze` job ahead of `cq-gold`) so `cq-gold` holds `7` nodes (`28` TPUs, `87.5%`) and finishes all 14 of its jobs in two clean waves (`~32s`). Meanwhile, `cq-bronze` holds `1` node (`4` TPUs, `12.5%`) during `0s–32s` and then inherits the idle cohort from `t=32s` onward to drain its remaining 4 jobs concurrently (`32s–48s`).

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `Cohort ~80%–92% (87.5% vs 12.5% in 0s–32s)` |
| **Section `5.2` Queue Wait Time** | `cq-gold finishes all 14 in 2 waves (~32s)` |
| **Section `5.3` Evictions & Reclaims** | `0 steady-state (<= 6 t=0s convergence FairSharing)` |
| **Section `5.4` Effective Saturation** | `~76%–85%` |

### Detailed Metric Checkpoints

- `Section 1 Gauge (kueue_admitted_active_workloads)` & `Section 5.1`: During `0s–32s`, `cq-gold` holds `P50 = 87.50% (cohort)` (`7` jobs) while `cq-bronze` holds `P50 = 12.50% (cohort)` (`1` job).
- `Section 5.1 (Cohort: cohort-fs-skew)`: `P50 = 100.00%`, `AVG ≈ 80%–92%`.
- `Section 5.4 (Effective Saturation)`: `20 × 4 × 16 = 1,280 TPU-s` over `~49s–52s` → **`EFFECTIVE SAT ≈ 76%–85%`**.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ `cq-bronze` holding `4` nodes (`50%`) throughout `0s–32s` despite having `1/7th` the weight of `cq-gold`.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
