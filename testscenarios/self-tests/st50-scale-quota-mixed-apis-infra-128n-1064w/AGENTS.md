# Agent Execution & Cross-Examination Guide — `st50-scale-quota-mixed-apis-infra-128n-1064w`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st50-scale-quota-mixed-apis-infra-128n-1064w`
- **Category**: 10. Large-Scale Quota-Only Admission (>100 Nodes, >1,000 Workloads — No TAS)
- **Title**: 128-Node, 1,064-Workload Multi-API Quota-Only (`JobSet` + `Job` + `PodGroup` + `Deployment` + `infra-pods`)
- **Primary Verification Target**: Validates vanilla K8s & chamber `t0` parity across 4 `t0` API kinds (`JobSet`, `Job`, native `PodGroup`, infinite `Deployment`) + `16` unmanaged `infra-pods` + `1,000` multi-API arrivals
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st50` (`--namespace kueue-optimizer-chamber-st50`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16450` | Prometheus `19097` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st60-scale-tas-hybrid-infra-infinite-144n-1080w`](../st60-scale-tas-hybrid-infra-infinite-144n-1080w/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST50 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st50' (API port 16450, Prom port 19097):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st50-scale-quota-mixed-apis-infra-128n-1064w --no-skip-initial-offset --duration 85s --namespace kueue-optimizer-chamber-st50 --port 16450 > /tmp/st50.log 2>&1 &
PID_ST50=$!

# Simultaneously launch paired ST60 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st60' (API port 16460, Prom port 19107):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st60-scale-tas-hybrid-infra-infinite-144n-1080w --no-skip-initial-offset --duration 85s --namespace kueue-optimizer-chamber-st60 --port 16460 > /tmp/st60.log 2>&1 &
PID_ST60=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST50" "$PID_ST60"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST50 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st50' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st50-scale-quota-mixed-apis-infra-128n-1064w --no-skip-initial-offset --duration 85s --namespace kueue-optimizer-chamber-st50 --port 16450 | tee /tmp/st50.log

# 2. Run paired ST60 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st60':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st60-scale-tas-hybrid-infra-infinite-144n-1080w --no-skip-initial-offset --duration 85s --namespace kueue-optimizer-chamber-st60 --port 16460 | tee /tmp/st60.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st50-scale-quota-mixed-apis-infra-128n-1064w --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st50`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st50-scale-quota-mixed-apis-infra-128n-1064w` (`kueue-optimizer-chamber-st50`) ↔ [`st60-scale-tas-hybrid-infra-infinite-144n-1080w`](../st60-scale-tas-hybrid-infra-infinite-144n-1080w/AGENTS.md) (`kueue-optimizer-chamber-st60`) — Multi-API + `infra-pods` + Infinite `Deployment` Quota-Only (`st50`) vs. TAS (`st60`) Pair

- **Controlled Variable Changed**: 128-node quota-only (`st50`, `1,064` Kueue workloads) vs. 144-node TAS (`st60`, `1,080` Kueue workloads) with identical `64` `t0` multi-API workloads (`JobSet`, `Job`, `PodGroup`, infinite `Deployment`) + `16` `infra-pods`.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st60-scale-tas-hybrid-infra-infinite-144n-1080w --no-skip-initial-offset --duration 85s --namespace kueue-optimizer-chamber-st60 --port 16460`
- **Cross-Examination Assertion**: Both report exact `Initial Kueue Workloads: 64 PodGroup(s) (80 pod(s))`, `Initial Infra Pods: 16`, `active_at_end=16`, and `FULL-DUR SAT <= 100%`.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `Peak 100% (1,024/1,024 TPUs), steady floor 6.25% (64/1,024)` (`st50`) vs. `Peak ~100% (1,152/1,152 TPUs), floor 5.56% (64/1,152)` (`st60`)
  - `Sec 5.2 Wait Time`: `Multi-wave drain < 70s` (`st50`) vs. `Multi-API TAS drain < 70s` (`st60`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st50`) vs. `0 evictions` (`st60`)
  - `Sec 5.4 Effective Sat`: `1,048 completed + 16 active_at_end (FULL-DUR <= 100%)` (`st50`) vs. `1,064 completed + 16 active_at_end (FULL-DUR <= 100%)` (`st60`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 128 nodes (`1,024` TPUs, no TAS), `cq-scale-mixed` (`1,024` TPUs), `--duration 85s`. At `t0`: `16` unmanaged `infra-pods` (`64` TPUs, `14s`) + `64` Kueue `t0` workloads (`16` `JobSet`s, `16` `Job`s, `16` `2`-pod vanilla `PodGroup`s, and `16` infinite `Deployment`s = `256` TPUs, `80` Kueue `t0` pods). Arrivals: `800` `Job`s + `100` `JobSet`s + `100` standalone `Pod`s (**`1,064` Kueue workloads** + `16` `infra-pods`).
- **Expected Runtime Behavior**: Loader coalesces the `16` `2`-pod vanilla `PodGroup`s without phantom pod duplication (`64` `InitialWorkloadPodGroup`s = `80` Kueue `t0` pods + `16` `infra-pods`), drains all `1,048` finite Kueue workloads well before `85s`, and keeps the `16` infinite `t0` `Deployment` `PodGroup`s active at `85s` (`ACTIVE AT END` without `864,000s` `FULL-DUR SAT` inflation).

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `Peak 100% (1,024/1,024 TPUs), steady floor 6.25% (64/1,024)` |
| **Section `5.2` Queue Wait Time** | `Multi-wave drain < 70s` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `1,048 completed + 16 active_at_end (FULL-DUR <= 100%)` |

### Detailed Metric Checkpoints

- `Execution Summary`: `Initial Kueue Workloads: 64 PodGroup(s) (80 pod(s))`, `Initial Infra Pods: 16`, `Workflows Submitted: 1000/1000`.
- `Section 5.4`: `Qualifying Workloads: 1064 (completed=1048, active_at_end=16)` and `FULL-DUR SAT <= 100%`.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ Doubling of the `16` `2`-pod `PodGroup`s into `16 + 32` candidates or phantom Tier 4 pods.
- ❌ `FULL-DUR SAT` exploding `> 1,000%` from the `16` infinite `t0` `Deployment`s.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
