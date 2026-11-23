# Agent Execution & Cross-Examination Guide — `st24-geometric-6cq-fair-sharing-hierarchy`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st24-geometric-6cq-fair-sharing-hierarchy`
- **Category**: 5. Fair Sharing & Priority Thresholds
- **Title**: Heavy 6-ClusterQueue Geometric Fair-Sharing Hierarchy (`Weights 1, 2, 4, 8, 16, 32`)
- **Primary Verification Target**: Multi-queue DRF stress test across 6 simultaneously backlogged ClusterQueues spanning a 32x weight range
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st24` (`--namespace kueue-optimizer-chamber-st24`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16424` | Prometheus `19071` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st23-heavy-asymmetric-fair-sharing-7-to-1`](../st23-heavy-asymmetric-fair-sharing-7-to-1/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST24 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st24' (API port 16424, Prom port 19071):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st24-geometric-6cq-fair-sharing-hierarchy --namespace kueue-optimizer-chamber-st24 --port 16424 > /tmp/st24.log 2>&1 &
PID_ST24=$!

# Simultaneously launch paired ST23 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st23' (API port 16423, Prom port 19070):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st23-heavy-asymmetric-fair-sharing-7-to-1 --namespace kueue-optimizer-chamber-st23 --port 16423 > /tmp/st23.log 2>&1 &
PID_ST23=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST24" "$PID_ST23"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST24 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st24' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st24-geometric-6cq-fair-sharing-hierarchy --namespace kueue-optimizer-chamber-st24 --port 16424 | tee /tmp/st24.log

# 2. Run paired ST23 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st23':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st23-heavy-asymmetric-fair-sharing-7-to-1 --namespace kueue-optimizer-chamber-st23 --port 16423 | tee /tmp/st23.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st24-geometric-6cq-fair-sharing-hierarchy --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st24`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st24-geometric-6cq-fair-sharing-hierarchy` (`kueue-optimizer-chamber-st24`) ↔ [`st23-heavy-asymmetric-fair-sharing-7-to-1`](../st23-heavy-asymmetric-fair-sharing-7-to-1/AGENTS.md) (`kueue-optimizer-chamber-st23`) — 6-CQ Geometric Hierarchy (`st24`) vs. 2-CQ `7:1` Baseline (`st23`) Pair

- **Controlled Variable Changed**: 6 CQs (`cq-geo-0..5`, weights `1, 2, 4, 8, 16, 32`, `36` jobs on `16` nodes) vs. 2 CQs (`weights 7, 1`, `20` jobs on `8` nodes).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st23-heavy-asymmetric-fair-sharing-7-to-1 --namespace kueue-optimizer-chamber-st23 --port 16423`
- **Cross-Examination Assertion**: All 6 `cq-geo-*` queues appear in `Sec 1` & `Sec 5.1`, completion order follows descending weight (`32 -> 16 -> 8 -> 4 -> 2 -> 1`), and `Sec 5.4 EFFECTIVE SAT` lands at `~75%` (`2,160 / 2,880 TPU-s`).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `Cohort P50=100%, AVG ~70%–86%` (`st24`) vs. `Cohort ~80%–92% (87.5% vs 12.5% in 0s–32s)` (`st23`)
  - `Sec 5.2 Wait Time`: `Wait inversely proportional to weight` (`st24`) vs. `cq-gold finishes all 14 in 2 waves (~32s)` (`st23`)
  - `Sec 5.3 Evictions`: `0 steady-state (<= 6 t=0s convergence FairSharing)` (`st24`) vs. `0 steady-state (<= 6 t=0s convergence FairSharing)` (`st23`)
  - `Sec 5.4 Effective Sat`: `~68%–80% (2,160 TPU-s work)` (`st24`) vs. `~76%–85%` (`st23`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 16-node (`64`-TPU) cluster, `cohort-geo6` (`64` Cohort TPUs). Six ClusterQueues `cq-geo-0` .. `cq-geo-5` with geometric weights `1, 2, 4, 8, 16, 32` each submit 6 single-node (`4`-TPU, `15s`) jobs simultaneously at `t=0s` (`36` jobs = `144` TPUs = `2.25` cluster waves).
- **Expected Runtime Behavior**: Highest-weight queues (`cq-geo-5` weight `32`: `6` jobs, `cq-geo-4` weight `16`: `6` jobs, `cq-geo-3` weight `8`: `3` jobs, `cq-geo-2` weight `4`: `1` job = `16` nodes) saturate Wave 1 (`0s–15s`), medium/lower weights (`cq-geo-3`, `cq-geo-2`, `cq-geo-1`, `cq-geo-0`) fill all 16 nodes in Wave 2 (`15s–30s`), and the remaining 4 lowest-weight jobs (`cq-geo-0`, `cq-geo-1`) drain in Wave 3 (`30s–45s`).

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `Cohort P50=100%, AVG ~70%–86%` |
| **Section `5.2` Queue Wait Time** | `Wait inversely proportional to weight` |
| **Section `5.3` Evictions & Reclaims** | `0 steady-state (<= 6 t=0s convergence FairSharing)` |
| **Section `5.4` Effective Saturation** | `~68%–80% (2,160 TPU-s work)` |

### Detailed Metric Checkpoints

- `Section 1 Gauge`: All 6 `cq-geo-*` queues appear in `kueue_cluster_queue_status{status='active'} = 1` and `kueue_cluster_queue_weighted_share`.
- `Section 5.1 (Cohort: cohort-geo6)`: `P50 = 100.00%`, `AVG ≈ 70%–86%` across the 3-wave drain (`100%` in Waves 1 & 2, `25%` in final 4-job tail Wave 3).
- `Section 5.4 (Effective Saturation)`: **`~68%–80%`** (`36 × 4 × 15 = 2,160 TPU-s` over `~46s–49s × 64 TPUs`; `75.0%` ideal zero-overhead theoretical).

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ `cq-geo-0` (`weight 1`) completing all 6 jobs ahead of `cq-geo-5` (`weight 32`).
- ❌ Any of the 6 CQs missing from `Section 1` or `Section 5.1` tables.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
