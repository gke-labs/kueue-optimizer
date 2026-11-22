# Agent Execution & Cross-Examination Guide — `st72-partial-exec-preempted-true-length-override`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and re-run:
> ```bash
> python3 testscenarios/self-tests/generate_self_test_suite.py
> python3 deploy/scripts/generate_scenario_readme.py --all
> python3 testscenarios/self-tests/generate_self_test_suite.py --check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st72-partial-exec-preempted-true-length-override`
- **Category**: 15. Interrupted-Workload Recording, Multiplier & True-Length Replay
- **Title**: Preemption-Interrupted Workloads Replayed With Schema `trueDurationSeconds` & CLI `--interrupted-duration-override` (Twin of st71)
- **Primary Verification Target**: Direct A/B twin of `st71`: explicit true lengths via schema `trueDurationSeconds` (`16s` on `js-0..1`) and CLI `--interrupted-duration-override js-preempted-rec-2=24s,js-preempted-rec-3=24s` (`> 8s` interrupted recording)
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st72` (`--namespace kueue-optimizer-chamber-st72`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16472` | Prometheus `19119` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st71-partial-exec-preempted-multiplier-replay`](../st71-partial-exec-preempted-multiplier-replay/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST72 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st72' (API port 16472, Prom port 19119):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st72-partial-exec-preempted-true-length-override --interrupted-duration-override js-preempted-rec-2=24s,js-preempted-rec-3=24s --namespace kueue-optimizer-chamber-st72 --port 16472 > /tmp/st72.log 2>&1 &
PID_ST72=$!

# Simultaneously launch paired ST71 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st71' (API port 16471, Prom port 19118):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st71-partial-exec-preempted-multiplier-replay --interrupted-duration-multiplier 2.5 --namespace kueue-optimizer-chamber-st71 --port 16471 > /tmp/st71.log 2>&1 &
PID_ST71=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST72" "$PID_ST71"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST72 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st72' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st72-partial-exec-preempted-true-length-override --interrupted-duration-override js-preempted-rec-2=24s,js-preempted-rec-3=24s --namespace kueue-optimizer-chamber-st72 --port 16472 | tee /tmp/st72.log

# 2. Run paired ST71 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st71':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st71-partial-exec-preempted-multiplier-replay --interrupted-duration-multiplier 2.5 --namespace kueue-optimizer-chamber-st71 --port 16471 | tee /tmp/st71.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st72-partial-exec-preempted-true-length-override --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st72`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st72-partial-exec-preempted-true-length-override` (`kueue-optimizer-chamber-st72`) ↔ [`st71-partial-exec-preempted-multiplier-replay`](../st71-partial-exec-preempted-multiplier-replay/AGENTS.md) (`kueue-optimizer-chamber-st71`) — Exact Interrupted-Workload Replay A/B Twin (True-Length Override `st72` vs. Multiplier `st71`)

- **Controlled Variable Changed**: Per-workload `trueDurationSeconds` (`16s`) + CLI `--interrupted-duration-override` (`24s`) in `st72` vs. `--interrupted-duration-multiplier 2.5` (`20s`) in `st71`.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st71-partial-exec-preempted-multiplier-replay --interrupted-duration-multiplier 2.5 --namespace kueue-optimizer-chamber-st71 --port 16471`
- **Cross-Examination Assertion**: Outline & live `cluster-scraper` confirm `min=16s, max=24s` in `st72` vs. `min=20s, max=20s` in `st71`.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `~80%–90% (16 TPUs 0-16s, 8 TPUs 16-24s)` (`st72`) vs. `~92%–100% (16/16 TPUs for 20s)` (`st71`)
  - `Sec 5.2 Wait Time`: `< 1.5s` (`st72`) vs. `< 1.5s` (`st71`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st72`) vs. `0 evictions` (`st71`)
  - `Sec 5.4 Effective Sat`: `~78%–88% (83.3% theoretical)` (`st72`) vs. `~90%–100% (20s > 8s partial)` (`st71`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: Identical 4-node (`16`-TPU) cluster and `8.0s` `PartialExecution`-only `JobSet`s as `st71`. `execution-time.json` carries `trueDurationSeconds: 16.0` on `js-0..1` and `22.0` on `js-2..3`, while CLI `--interrupted-duration-override` overrides `js-2..3` to `24s`.
- **Expected Runtime Behavior**: `js-preempted-rec-0..1` run for exact **`16.0s`** (from schema `trueDurationSeconds`), while `js-preempted-rec-2..3` run for exact **`24.0s`** (CLI override taking highest precedence over schema `22.0s`), creating a clean two-step (`16s` -> `24s`) completion profile (`avg = 20s`, `~83%` cluster saturation over `24s`).

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `~80%–90% (16 TPUs 0-16s, 8 TPUs 16-24s)` |
| **Section `5.2` Queue Wait Time** | `< 1.5s` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `~78%–88% (83.3% theoretical)` |

### Detailed Metric Checkpoints

- `Pre-Run Outline`: `Execution Durations: min=16.0s, max=24.0s` (`avg=20.0s`, strictly `> 8.0s` partial).
- `Section 5.4`: **`EFFECTIVE SAT ≈ 78%–88%`** (`(2*16 + 2*24) / (4*24) = 80 / 96 = 83.3%`, vs `~95%` uniform `20s` in `st71`).
- `cluster-scraper`: Records `FullExecution ≈ 16s` for `js-0..1` and `FullExecution ≈ 24s` for `js-2..3`.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ CLI `--interrupted-duration-override` failing to override `js-2..3` or affecting `js-0..1`.
- ❌ Any workload replaying for `<= 8.0s`.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
python3 testscenarios/self-tests/generate_self_test_suite.py
python3 deploy/scripts/generate_scenario_readme.py --all
python3 testscenarios/self-tests/generate_self_test_suite.py --check
```
