# Agent Execution & Cross-Examination Guide — `st34-t0-multi-role-heterogeneous-podgroup`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st34-t0-multi-role-heterogeneous-podgroup`
- **Category**: 7. Initial t0 Workloads — Handling & Accounting
- **Title**: Heterogeneous Multi-PodSet `t0` `PodGroup` (`1` CPU Coordinator + `4` TPU Workers per Workload)
- **Primary Verification Target**: Homogeneous-per-PodSet grouping (`resourceUsage % count == 0` Kueue `WorkloadWebhook` invariant) and multi-role `t0` quota/saturation math
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st34` (`--namespace kueue-optimizer-chamber-st34`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16434` | Prometheus `19081` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st29-multi-role-heterogeneous-jobset`](../st29-multi-role-heterogeneous-jobset/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST34 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st34' (API port 16434, Prom port 19081):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st34-t0-multi-role-heterogeneous-podgroup --namespace kueue-optimizer-chamber-st34 --port 16434 > /tmp/st34.log 2>&1 &
PID_ST34=$!

# Simultaneously launch paired ST29 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st29' (API port 16429, Prom port 19076):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st29-multi-role-heterogeneous-jobset --namespace kueue-optimizer-chamber-st29 --port 16429 > /tmp/st29.log 2>&1 &
PID_ST29=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST34" "$PID_ST29"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST34 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st34' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st34-t0-multi-role-heterogeneous-podgroup --namespace kueue-optimizer-chamber-st34 --port 16434 | tee /tmp/st34.log

# 2. Run paired ST29 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st29':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st29-multi-role-heterogeneous-jobset --namespace kueue-optimizer-chamber-st29 --port 16429 | tee /tmp/st29.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st34-t0-multi-role-heterogeneous-podgroup --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st34`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st34-t0-multi-role-heterogeneous-podgroup` (`kueue-optimizer-chamber-st34`) ↔ [`st29-multi-role-heterogeneous-jobset`](../st29-multi-role-heterogeneous-jobset/AGENTS.md) (`kueue-optimizer-chamber-st29`) — Bootstrapped `t0` Multi-Role `PodGroup` (`st34`) vs. Scheduled Multi-Role `JobSet` (`st29`) Pair

- **Controlled Variable Changed**: `st34` bootstraps the two `1 coord + 4 worker` JobSets at `t0` (`24s`) and queues `arr-mrole-after` (`16s`) behind them.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st29-multi-role-heterogeneous-jobset --namespace kueue-optimizer-chamber-st29 --port 16429`
- **Cross-Examination Assertion**: `st34` passes `WorkloadWebhook` validation (`2` PodSets per `t0` `PodGroup`), holds `arr-mrole-after` for `~24s`, and finishes `15` total pods (`3` workloads) cleanly.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `tpu ~75%–82% (100% w1, 50% w2)` (`st34`) vs. `tpu ~100%, cpu ~31.25%` (`st29`)
  - `Sec 5.2 Wait Time`: `arr-mrole-after waits ~24s` (`st34`) vs. `< 1.0s` (`st29`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st34`) vs. `0 evictions` (`st29`)
  - `Sec 5.4 Effective Sat`: `tpu ~75%–82% (3 workloads, 15 pods)` (`st34`) vs. `tpu ~96% (2 workloads, 10 pods)` (`st29`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 8-node (`32`-TPU, `128`-CPU) cluster, `cq-t0-mrole` (`32` TPUs, `128` CPUs). At `t0`, 2 multi-role `Admitted` `JobSet`s (`init-mrole-0..1`, `24s` remaining) run `1` CPU `coord` pod (`0` TPUs, `4` CPUs) + `4` TPU `w` pods (`16` TPUs, `16` CPUs) each = `10` initial pods (`32/32` TPUs, `40/128` CPUs). At `t=0s`, 1 `NotAdmitted` multi-role `JobSet` (`arr-mrole-after`, `5` pods = `16` TPUs, `16s`) arrives.
- **Expected Runtime Behavior**: Both `init-mrole-*` `PodGroup` `Workload`s pass Kueue's `WorkloadWebhook` with 2 distinct `podSets` (`count=1` and `count=4`), hold `32/32` TPUs + `40/128` CPUs, block `arr-mrole-after` for `~24s`, and finish cleanly before `arr-mrole-after` runs `24s–40s`.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `tpu ~75%–82% (100% w1, 50% w2)` |
| **Section `5.2` Queue Wait Time** | `arr-mrole-after waits ~24s` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `tpu ~75%–82% (3 workloads, 15 pods)` |

### Detailed Metric Checkpoints

- `Execution Summary`: `Initial Kueue Workloads: 2 PodGroup(s) (10 pod(s))`, `Workflows Submitted: 1/1`, `KWOK Pods Status: ... succeeded=15, failed=0`.
- `Section 5.2`: `arr-mrole-after` waits **`~22s–26s`** behind the `32/32` TPU `t0` multi-role workloads.
- `Section 5.4`: `Qualifying Workloads: 3 (completed=3 [15 pods])`, `google.com/tpu` **`EFFECTIVE SAT ≈ 72%–82%`** (`(32×24 + 16×16) / (32×40) = 1,024 / 1,280 = 80.0%`).

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ Kueue `WorkloadWebhook` rejecting `init-mrole-*` with `resourceUsage is not a multiple of count` (would happen if `0`-TPU `coord` and `4`-TPU `w` pods were merged into one `count=5` PodSet with `16` TPUs).

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
