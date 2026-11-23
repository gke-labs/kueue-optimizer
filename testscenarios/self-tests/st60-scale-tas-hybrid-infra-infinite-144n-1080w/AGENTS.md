# Agent Execution & Cross-Examination Guide — `st60-scale-tas-hybrid-infra-infinite-144n-1080w`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st60-scale-tas-hybrid-infra-infinite-144n-1080w`
- **Category**: 12. Large-Scale TAS Preemption, Reclaim, Fair Sharing & Hybrid (>100 Nodes, >1,000 Workloads)
- **Title**: 144-Node, 1,080-Workload Hybrid Multi-API TAS + `infra-pods` + Infinite `Deployment` Scraper Stress
- **Primary Verification Target**: End-to-end TAS + `cluster-scraper` (`--hash-names` & plain) capstone combining `144` TAS nodes, `16` `infra-pods`, `64` multi-API `t0` TAS workloads (`JobSet`, `Job`, `PodGroup`, infinite `Deployment`), and `1,016` multi-API TAS arrivals
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st60` (`--namespace kueue-optimizer-chamber-st60`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16460` | Prometheus `19107` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st50-scale-quota-mixed-apis-infra-128n-1064w`](../st50-scale-quota-mixed-apis-infra-128n-1064w/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST60 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st60' (API port 16460, Prom port 19107):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st60-scale-tas-hybrid-infra-infinite-144n-1080w --no-skip-initial-offset --duration 85s --namespace kueue-optimizer-chamber-st60 --port 16460 > /tmp/st60.log 2>&1 &
PID_ST60=$!

# Simultaneously launch paired ST50 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st50' (API port 16450, Prom port 19097):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st50-scale-quota-mixed-apis-infra-128n-1064w --no-skip-initial-offset --duration 85s --namespace kueue-optimizer-chamber-st50 --port 16450 > /tmp/st50.log 2>&1 &
PID_ST50=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST60" "$PID_ST50"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST60 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st60' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st60-scale-tas-hybrid-infra-infinite-144n-1080w --no-skip-initial-offset --duration 85s --namespace kueue-optimizer-chamber-st60 --port 16460 | tee /tmp/st60.log

# 2. Run paired ST50 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st50':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st50-scale-quota-mixed-apis-infra-128n-1064w --no-skip-initial-offset --duration 85s --namespace kueue-optimizer-chamber-st50 --port 16450 | tee /tmp/st50.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st60-scale-tas-hybrid-infra-infinite-144n-1080w --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st60`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st60-scale-tas-hybrid-infra-infinite-144n-1080w` (`kueue-optimizer-chamber-st60`) ↔ [`st50-scale-quota-mixed-apis-infra-128n-1064w`](../st50-scale-quota-mixed-apis-infra-128n-1064w/AGENTS.md) (`kueue-optimizer-chamber-st50`) — 144-Node TAS Hybrid Multi-API (`st60`) vs. 128-Node Quota-Only Hybrid Multi-API (`st50`) Pair

- **Controlled Variable Changed**: 7-level TAS on `144` nodes (`st60`, `1,080` Kueue workloads) vs. quota-only on `128` nodes (`st50`, `1,064` Kueue workloads).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st50-scale-quota-mixed-apis-infra-128n-1064w --no-skip-initial-offset --duration 85s --namespace kueue-optimizer-chamber-st50 --port 16450`
- **Cross-Examination Assertion**: Both cleanly separate `16` `infra-pods` from `64` multi-API `t0` `PodGroup`s (`80` pods), complete all finite arrivals, keep `16` infinite `Deployment`s `active_at_end`, and round-trip through `cluster-scraper` without phantom pod duplication.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `Peak ~100% (1,152/1,152 TPUs), floor 5.56% (64/1,152)` (`st60`) vs. `Peak 100% (1,024/1,024 TPUs), steady floor 6.25% (64/1,024)` (`st50`)
  - `Sec 5.2 Wait Time`: `Multi-API TAS drain < 70s` (`st60`) vs. `Multi-wave drain < 70s` (`st50`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st60`) vs. `0 evictions` (`st50`)
  - `Sec 5.4 Effective Sat`: `1,064 completed + 16 active_at_end (FULL-DUR <= 100%)` (`st60`) vs. `1,048 completed + 16 active_at_end (FULL-DUR <= 100%)` (`st50`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 144 nodes (`36` racks, `1,152` TPUs) with TAS, `cq-tas-hybrid` (`1,152` TPUs), `--duration 85s`. At `t0`: `16` unmanaged `infra-pods` (`64` TPUs, `14s`) + `64` TAS `t0` workloads (`16` `JobSet`s, `16` `Job`s, `16` `2`-pod vanilla `PodGroup`s, `16` infinite `Deployment`s = `256` TPUs, `80` `t0` Kueue pods). Arrivals: `816` TAS `Job`s + `100` TAS `JobSet`s + `100` standalone TAS `Pod`s (**`1,080` Kueue workloads** + `16` `infra-pods`).
- **Expected Runtime Behavior**: Separates `16` `infra-pods` (`infra-pods` namespace) from `64` TAS `InitialWorkloadPodGroup`s (`80` Kueue `t0` pods with TAS `topologyAssignment`), completes all `1,064` finite TAS workloads within `85s`, retains `16` infinite `t0` TAS `PodGroup`s as `ACTIVE AT END`, and round-trips cleanly through `cluster-scraper` (`--hash-names` & plain).

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `Peak ~100% (1,152/1,152 TPUs), floor 5.56% (64/1,152)` |
| **Section `5.2` Queue Wait Time** | `Multi-API TAS drain < 70s` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `1,064 completed + 16 active_at_end (FULL-DUR <= 100%)` |

### Detailed Metric Checkpoints

- `Execution Summary`: `Initial Kueue Workloads: 64 PodGroup(s) (80 pod(s))`, `Initial Infra Pods: 16`, `Workflows Submitted: 1016/1016`.
- `Section 5.4`: `Qualifying Workloads: 1080 (completed=1064, active_at_end=16)` and `FULL-DUR SAT <= 100%`.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ Any `infra-pod` assigned a Kueue `PodGroup` or any `PodGroup` doubled on `cluster-scraper` round-trip.
- ❌ `FULL-DUR SAT` exceeding `100%` due to the `16` infinite `t0` TAS `Deployment`s.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
