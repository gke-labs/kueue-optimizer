# Agent Execution & Cross-Examination Guide — `st79-config-admission-fair-sharing-enabled`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st79-config-admission-fair-sharing-enabled`
- **Category**: 16. Kueue Controller Configuration (`FairSharing` & `AdmissionFairSharing`)
- **Title**: Declarative Kueue `Configuration` Enabling Admission-Based Fair Sharing (`AdmissionFairSharing`)
- **Primary Verification Target**: Declarative `kind: Configuration` enabling `admissionFairSharing` (`usageHalfLifeTime: 20s`, `usageSamplingInterval: 1s`, `google.com/tpu: 1`): `lq-light` (`t=3s`, `0` prior usage) jumps ahead of `8` older `t=0s` `lq-heavy` backlog jobs at Wave 2 (`t=8s`, `wait ≈ 5s`)
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st79` (`--namespace kueue-optimizer-chamber-st79`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16479` | Prometheus `19126` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st80-config-admission-fair-sharing-disabled`](../st80-config-admission-fair-sharing-disabled/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST79 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st79' (API port 16479, Prom port 19126):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st79-config-admission-fair-sharing-enabled --namespace kueue-optimizer-chamber-st79 --port 16479 > /tmp/st79.log 2>&1 &
PID_ST79=$!

# Simultaneously launch paired ST80 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st80' (API port 16480, Prom port 19127):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st80-config-admission-fair-sharing-disabled --namespace kueue-optimizer-chamber-st80 --port 16480 > /tmp/st80.log 2>&1 &
PID_ST80=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST79" "$PID_ST80"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST79 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st79' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st79-config-admission-fair-sharing-enabled --namespace kueue-optimizer-chamber-st79 --port 16479 | tee /tmp/st79.log

# 2. Run paired ST80 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st80':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st80-config-admission-fair-sharing-disabled --namespace kueue-optimizer-chamber-st80 --port 16480 | tee /tmp/st80.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st79-config-admission-fair-sharing-enabled --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st79`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st79-config-admission-fair-sharing-enabled` (`kueue-optimizer-chamber-st79`) ↔ [`st80-config-admission-fair-sharing-disabled`](../st80-config-admission-fair-sharing-disabled/AGENTS.md) (`kueue-optimizer-chamber-st80`) — Exact Admission-Based Fair Sharing A/B Twin (`admissionFairSharing` Enabled `st79` vs. Disabled `st80` via `Configuration`)

- **Controlled Variable Changed**: `Configuration.admissionFairSharing` configured (`usageHalfLifeTime: 20s, usageSamplingInterval: 1s, google.com/tpu: 1`, `st79`) vs. omitted (`st80`) on identical `cq-afs` (`UsageBasedAdmissionFairSharing`), `LocalQueue`s, and 16 workloads.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st80-config-admission-fair-sharing-disabled --namespace kueue-optimizer-chamber-st80 --port 16480`
- **Cross-Examination Assertion**: In `st79`, `kueue_local_queue_admission_fair_sharing_usage` is `> 0` and all `4` `lq-light` (`p-light`) jobs jump into Wave 2 (**`wait ≈ 5s–6.5s`**); in `st80`, FIFO holds `p-light` until Wave 4 (**`wait ≈ 21s–24s`**, `+16s` delta) with `0` evictions in both.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `75%–100% (16/16 TPUs across 4 waves)` (`st79`) vs. `75%–100% (16/16 TPUs across 4 waves)` (`st80`)
  - `Sec 5.2 Wait Time`: `p-light AVG ~7.5s < p-heavy AVG ~17.3s` (`st79`) vs. `p-light AVG ~27.8s > p-heavy AVG ~10.4s` (`st80`)
  - `Sec 5.3 Evictions`: `0 evictions (admission-time ordering)` (`st79`) vs. `0 evictions` (`st80`)
  - `Sec 5.4 Effective Sat`: `75%–92% (16 completed)` (`st79`) vs. `75%–92% (16 completed)` (`st80`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 4-node (`16`-TPU) cluster with `kind: Configuration` setting `admissionFairSharing` (`usageHalfLifeTime: 20s`, `usageSamplingInterval: 1s`, `resourceWeights: {google.com/tpu: 1, cpu: 0, memory: 0}`). `cq-afs` (`16` TPUs) sets `admissionScope.admissionMode: UsageBasedAdmissionFairSharing`; `lq-heavy` (`weight: 1`, `p-heavy=100`) submits `12` `4`-TPU `8s` jobs at `t=0s`; `lq-light` (`weight: 1`, `p-light=100`) submits `4` `4`-TPU `8s` jobs at `t=3s`.
- **Expected Runtime Behavior**: During Wave 1 (`0s–8s`), `lq-heavy` consumes `16` TPUs (`kueue_local_queue_admission_fair_sharing_usage{name='lq-heavy'} > 0`) while `lq-light` usage stays `0`. When Wave 1 finishes at `t=8s`, `AdmissionFairSharing` orders `lq-light` ahead of `lq-heavy`'s `8` older `t=0s` jobs (`0.0 + penalty < ~3.88`), running all `4` `lq-light` jobs in Wave 2 (`8s–16s`, `wait ≈ 5s`) before `lq-heavy` finishes in Waves 3–4 (`16s–32s`).

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `75%–100% (16/16 TPUs across 4 waves)` |
| **Section `5.2` Queue Wait Time** | `p-light AVG ~7.5s < p-heavy AVG ~17.3s` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions (admission-time ordering)` |
| **Section `5.4` Effective Saturation** | `75%–92% (16 completed)` |

### Detailed Metric Checkpoints

- `Section 1 Gauge (kueue_local_queue_admission_fair_sharing_usage)`: Actively emitted (`> 0`) for `lq-heavy` and `lq-light`.
- `Section 5.2 (Wait Time)` & `Section 3 (kueue_local_queue_admission_wait_time_seconds)`: **`p-light` (`lq-light`) waits `~5.5s–9s`** (`P50 ≈ 7.5s, AVG ≈ 7s–9s`), overtaking `p-heavy` (`AVG ≈ 16s–19.5s`).
- `Section 5.3`: **`0`** evictions (pure admission-time ordering, zero preemptions).
- `Section 5.4`: **`~75%–92%`** (`16/16` jobs complete in `~38s–45s`).

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ `p-light` waiting `~25s–28s` behind all `12` `lq-heavy` jobs (would mean `admissionFairSharing` in `Configuration` was not applied).
- ❌ `kueue_local_queue_admission_fair_sharing_usage` missing or remaining `0` for `lq-heavy`.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
