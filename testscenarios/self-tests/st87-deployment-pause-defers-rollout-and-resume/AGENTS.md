# Agent Execution & Cross-Examination Guide — `st87-deployment-pause-defers-rollout-and-resume`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st87-deployment-pause-defers-rollout-and-resume`
- **Category**: 18. Deployment Pauses, `t0` Status & Lifecycle
- **Title**: `Deployment` Pause (`spec.paused: true`) Deferring `1 -> 4` TPU Upgrade Until Resume
- **Primary Verification Target**: Verifies `Paused` (`t=+8s`) freezes `Deployment` rollout when a `1 -> 4` TPU/pod `Upgrade` arrives at `t=+11s` (keeping usage at `3/16` TPUs across `t=0..24s`), and `Resumed` (`spec.paused: false` via Server-Side Apply at `t=+24s`) triggers the `12`-TPU rollout
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st87` (`--namespace kueue-optimizer-chamber-st87`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16487` | Prometheus `19134` (collision-free across all 95 scenarios `st01`–`st95`)
- **Mandatory Paired Counterpart(s)**: [`st88-deployment-unpaused-immediate-rollout-control`](../st88-deployment-unpaused-immediate-rollout-control/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st95`) and non-overlapping port pair (`16401/19048` .. `16495/19142`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST87 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st87' (API port 16487, Prom port 19134):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st87-deployment-pause-defers-rollout-and-resume --duration 40s --namespace kueue-optimizer-chamber-st87 --port 16487 > /tmp/st87.log 2>&1 &
PID_ST87=$!

# Simultaneously launch paired ST88 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st88' (API port 16488, Prom port 19135):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st88-deployment-unpaused-immediate-rollout-control --duration 40s --namespace kueue-optimizer-chamber-st88 --port 16488 > /tmp/st88.log 2>&1 &
PID_ST88=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST87" "$PID_ST88"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST87 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st87' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st87-deployment-pause-defers-rollout-and-resume --duration 40s --namespace kueue-optimizer-chamber-st87 --port 16487 | tee /tmp/st87.log

# 2. Run paired ST88 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st88':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st88-deployment-unpaused-immediate-rollout-control --duration 40s --namespace kueue-optimizer-chamber-st88 --port 16488 | tee /tmp/st88.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st87-deployment-pause-defers-rollout-and-resume --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st87`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st87-deployment-pause-defers-rollout-and-resume` (`kueue-optimizer-chamber-st87`) ↔ [`st88-deployment-unpaused-immediate-rollout-control`](../st88-deployment-unpaused-immediate-rollout-control/AGENTS.md) (`kueue-optimizer-chamber-st88`) — Exact `Deployment` Pause/Resume A/B Twin (`Paused` at `t=+8s` + `Resumed` at `t=+24s` `st87` vs. Immediate Unpaused Upgrade `st88`)

- **Controlled Variable Changed**: Wrapping the `t=+11s` `1 -> 4` TPU/pod `Upgrade` in `Paused` (`t=+8s`) and `Resumed` (`t=+24s`) in `st87` vs. unpaused rollout at `t=+11s` in `st88`.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st88-deployment-unpaused-immediate-rollout-control --duration 40s --namespace kueue-optimizer-chamber-st88 --port 16488`
- **Cross-Examination Assertion**: Both admit `6` total replica `Workload`s (`3` `v1` + `3` `v2`), but deferring rollout by `13s` in `st87` keeps `Sec 5.1 AVG` (`~41%` vs. `~59%`) and `Sec 5.4 EFFECTIVE SAT` (`~36%–45%` vs. `~54%–63%`) strictly lower than `st88`.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `18.75% (t=0..24s) -> 75% (t=24..40s, AVG ~41%)` (`st87`) vs. `18.75% (t=0..11s) -> 75% (t=11..40s, AVG ~59%)` (`st88`)
  - `Sec 5.2 Wait Time`: `p-std < 2.5s (6 total admitted: 3 v1 + 3 v2)` (`st87`) vs. `p-std < 2.5s (6 total admitted: 3 v1 + 3 v2)` (`st88`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st87`) vs. `0 evictions` (`st88`)
  - `Sec 5.4 Effective Sat`: `36%–45% (completed=3 v1 + active_at_end=3 v2)` (`st87`) vs. `54%–63% (completed=3 v1 + active_at_end=3 v2)` (`st88`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 4 nodes (`16` TPUs), `cq-dep-paused` (`16` TPUs), `--duration 40s`. `embed-svc` (`replicas: 3`, `1` TPU/pod = `3` TPUs) starts at `t=0s`, pauses at `t=+8s`, upgrades template to `4` TPUs/pod (`12` TPUs) at `t=+11s` while paused, and resumes at `t=+24s`.
- **Expected Runtime Behavior**: During `t=0..24s`, `embed-svc` consumes only `3/16` TPUs (`18.75%`) because `spec.paused: true` blocks `deployment-controller` from rolling out the `t=+11s` `4`-TPU template. At `t=+24s`, `spec.paused: false` unblocks the rollout, jumping quota usage to `12/16` TPUs (`75%`) for `t=24..40s` (`AVG ≈ 35%–46%`).

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `18.75% (t=0..24s) -> 75% (t=24..40s, AVG ~41%)` |
| **Section `5.2` Queue Wait Time** | `p-std < 2.5s (6 total admitted: 3 v1 + 3 v2)` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `36%–45% (completed=3 v1 + active_at_end=3 v2)` |

### Detailed Metric Checkpoints

- `Section 1 (kueue_admitted_workloads_total)`: **`6`** admitted replica `Workload`s (`3` `1`-TPU `v1` + `3` `4`-TPU `v2`).
- `Section 5.1`: `cq-dep-paused` `google.com/tpu` stays at `18.75%` (`3/16`) until `t=24s`, then jumps to `75%` (`12/16`, **`AVG ≈ 35%–46%`**).
- `Section 5.3`: **`0`** evictions.
- `Section 5.4`: `Qualifying Workloads: 6 (completed=3, active_at_end=3)`, `EFFECTIVE SAT ≈ 36%–45%` (strictly lower than `st88`'s `~54%–63%`).

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ Rolling out `4`-TPU pods at `t=+11s` while `spec.paused: true`.
- ❌ Server-Side Apply failing to clear `spec.paused` back to `false` at `t=+24s` (leaving usage stuck at `3` TPUs).

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
