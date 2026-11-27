# Agent Execution & Cross-Examination Guide — `st88-deployment-unpaused-immediate-rollout-control`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st88-deployment-unpaused-immediate-rollout-control`
- **Category**: 18. Deployment Pauses, `t0` Status & Lifecycle
- **Title**: Unpaused `Deployment` Immediate `1 -> 4` TPU Upgrade at `t=+11s` (Control Twin of st87)
- **Primary Verification Target**: Direct A/B control twin of `st87` omitting `Paused`/`Resumed`: the `t=+11s` `1 -> 4` TPU/pod `Upgrade` rolls out `13s` earlier (`t=11s` vs. `t=24s`), lifting time-weighted TPU utilization and `Section 5.4` saturation by `~16–20pp`
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st88` (`--namespace kueue-optimizer-chamber-st88`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16488` | Prometheus `19135` (collision-free across all 95 scenarios `st01`–`st95`)
- **Mandatory Paired Counterpart(s)**: [`st87-deployment-pause-defers-rollout-and-resume`](../st87-deployment-pause-defers-rollout-and-resume/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st95`) and non-overlapping port pair (`16401/19048` .. `16495/19142`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST88 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st88' (API port 16488, Prom port 19135):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st88-deployment-unpaused-immediate-rollout-control --duration 40s --namespace kueue-optimizer-chamber-st88 --port 16488 > /tmp/st88.log 2>&1 &
PID_ST88=$!

# Simultaneously launch paired ST87 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st87' (API port 16487, Prom port 19134):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st87-deployment-pause-defers-rollout-and-resume --duration 40s --namespace kueue-optimizer-chamber-st87 --port 16487 > /tmp/st87.log 2>&1 &
PID_ST87=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST88" "$PID_ST87"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST88 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st88' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st88-deployment-unpaused-immediate-rollout-control --duration 40s --namespace kueue-optimizer-chamber-st88 --port 16488 | tee /tmp/st88.log

# 2. Run paired ST87 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st87':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st87-deployment-pause-defers-rollout-and-resume --duration 40s --namespace kueue-optimizer-chamber-st87 --port 16487 | tee /tmp/st87.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st88-deployment-unpaused-immediate-rollout-control --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st88`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st88-deployment-unpaused-immediate-rollout-control` (`kueue-optimizer-chamber-st88`) ↔ [`st87-deployment-pause-defers-rollout-and-resume`](../st87-deployment-pause-defers-rollout-and-resume/AGENTS.md) (`kueue-optimizer-chamber-st87`) — Exact `Deployment` Pause/Resume A/B Twin (Immediate Unpaused Upgrade `st88` vs. Paused/Resumed Rollout `st87`)

- **Controlled Variable Changed**: Immediate rollout at `t=+11s` (`st88`) vs. paused rollout until `t=+24s` (`st87`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st87-deployment-pause-defers-rollout-and-resume --duration 40s --namespace kueue-optimizer-chamber-st87 --port 16487`
- **Cross-Examination Assertion**: `Sec 5.4 EFFECTIVE SAT` rises by `~16–20pp` in `st88` because `3 x 4` TPU `v2` pods run for `~29s` (`t=11..40s`) instead of `~16s` (`t=24..40s`).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `18.75% (t=0..11s) -> 75% (t=11..40s, AVG ~59%)` (`st88`) vs. `18.75% (t=0..24s) -> 75% (t=24..40s, AVG ~41%)` (`st87`)
  - `Sec 5.2 Wait Time`: `p-std < 2.5s (6 total admitted: 3 v1 + 3 v2)` (`st88`) vs. `p-std < 2.5s (6 total admitted: 3 v1 + 3 v2)` (`st87`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st88`) vs. `0 evictions` (`st87`)
  - `Sec 5.4 Effective Sat`: `54%–63% (completed=3 v1 + active_at_end=3 v2)` (`st88`) vs. `36%–45% (completed=3 v1 + active_at_end=3 v2)` (`st87`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: Identical 4 nodes (`16` TPUs), `cq-dep-unpaused` (`16` TPUs), `embed-svc` (`replicas: 3`, `1` TPU/pod at `t=0s`), and `4`-TPU/pod `Upgrade` at `t=+11s` as `st87`, omitting the `t=+8s` `Paused` and `t=+24s` `Resumed` events (`--duration 40s`).
- **Expected Runtime Behavior**: At `t=+11s`, `embed-svc` immediately rolls from `3 x 1` TPU (`18.75%`) to `3 x 4` TPUs (`12/16 = 75%`) and runs at `12` TPUs for `~29s` (`t=11..40s`) instead of `~16s` (`t=24..40s` in `st87`).

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `18.75% (t=0..11s) -> 75% (t=11..40s, AVG ~59%)` |
| **Section `5.2` Queue Wait Time** | `p-std < 2.5s (6 total admitted: 3 v1 + 3 v2)` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `54%–63% (completed=3 v1 + active_at_end=3 v2)` |

### Detailed Metric Checkpoints

- `Section 1 (kueue_admitted_workloads_total)`: **`6`** admitted replica `Workload`s (`3` `v1` + `3` `v2`).
- `Section 5.1`: `cq-dep-unpaused` `google.com/tpu` jumps to `75%` (`12/16`) at `t=+11s` (**`AVG ≈ 54%–65%`** > `st87` `~35%–46%`).
- `Section 5.3`: **`0`** evictions.
- `Section 5.4`: `Qualifying Workloads: 6 (completed=3, active_at_end=3)`, `EFFECTIVE SAT ≈ 54%–63%` (`+16–20pp` above `st87`).

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ `Section 5.4 EFFECTIVE SAT` in `st88` being lower than or equal to paused `st87`.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
