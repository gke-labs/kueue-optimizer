# Agent Execution & Cross-Examination Guide — `st80-config-admission-fair-sharing-disabled`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st80-config-admission-fair-sharing-disabled`
- **Category**: 16. Kueue Controller Configuration (`FairSharing` & `AdmissionFairSharing`)
- **Title**: Declarative Kueue `Configuration` Without Admission-Based Fair Sharing (FIFO Control — Twin of st79)
- **Primary Verification Target**: Direct A/B twin of `st79` omitting `admissionFairSharing` in `kind: Configuration`: even though `cq-afs` declares `UsageBasedAdmissionFairSharing`, Kueue disables AFS globally and falls back to FIFO (`p-light` waits `~27s–28s` in Wave 4)
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st80` (`--namespace kueue-optimizer-chamber-st80`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16480` | Prometheus `19127` (collision-free across all 95 scenarios `st01`–`st95`)
- **Mandatory Paired Counterpart(s)**: [`st79-config-admission-fair-sharing-enabled`](../st79-config-admission-fair-sharing-enabled/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st95`) and non-overlapping port pair (`16401/19048` .. `16495/19142`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST80 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st80' (API port 16480, Prom port 19127):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st80-config-admission-fair-sharing-disabled --namespace kueue-optimizer-chamber-st80 --port 16480 > /tmp/st80.log 2>&1 &
PID_ST80=$!

# Simultaneously launch paired ST79 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st79' (API port 16479, Prom port 19126):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st79-config-admission-fair-sharing-enabled --namespace kueue-optimizer-chamber-st79 --port 16479 > /tmp/st79.log 2>&1 &
PID_ST79=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST80" "$PID_ST79"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST80 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st80' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st80-config-admission-fair-sharing-disabled --namespace kueue-optimizer-chamber-st80 --port 16480 | tee /tmp/st80.log

# 2. Run paired ST79 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st79':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st79-config-admission-fair-sharing-enabled --namespace kueue-optimizer-chamber-st79 --port 16479 | tee /tmp/st79.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st80-config-admission-fair-sharing-disabled --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st80`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st80-config-admission-fair-sharing-disabled` (`kueue-optimizer-chamber-st80`) ↔ [`st79-config-admission-fair-sharing-enabled`](../st79-config-admission-fair-sharing-enabled/AGENTS.md) (`kueue-optimizer-chamber-st79`) — Exact Admission-Based Fair Sharing A/B Twin (`admissionFairSharing` Disabled `st80` vs. Enabled `st79` via `Configuration`)

- **Controlled Variable Changed**: `Configuration` omits `admissionFairSharing` (`st80`) vs. enables `admissionFairSharing` (`st79`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st79-config-admission-fair-sharing-enabled --namespace kueue-optimizer-chamber-st79 --port 16479`
- **Cross-Examination Assertion**: Proves `ClusterQueue.spec.admissionScope.admissionMode: UsageBasedAdmissionFairSharing` is inert without `Configuration.admissionFairSharing`: `p-light` waits `~21s–24s` (`st80`) vs. `~5s–6.5s` (`st79`).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `75%–100% (16/16 TPUs across 4 waves)` (`st80`) vs. `75%–100% (16/16 TPUs across 4 waves)` (`st79`)
  - `Sec 5.2 Wait Time`: `p-light AVG ~27.8s > p-heavy AVG ~10.4s` (`st80`) vs. `p-light AVG ~7.5s < p-heavy AVG ~17.3s` (`st79`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st80`) vs. `0 evictions (admission-time ordering)` (`st79`)
  - `Sec 5.4 Effective Sat`: `75%–92% (16 completed)` (`st80`) vs. `75%–92% (16 completed)` (`st79`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: Identical 4-node (`16`-TPU) cluster, `cq-afs` (`admissionScope.admissionMode: UsageBasedAdmissionFairSharing`), `lq-heavy` / `lq-light` (`weight: 1`), and 16 workloads (`12` `p-heavy` at `t=0s`, `4` `p-light` at `t=3s`) as `st79`, changing ONLY `kind: Configuration` in `infra.json` to omit `admissionFairSharing`.
- **Expected Runtime Behavior**: Because `cfg.AdmissionFairSharing == nil` in the chamber's deployed `Configuration`, Kueue ignores `LocalQueue` AFS usage and orders equal-priority (`100 == 100`) workloads strictly by creation timestamp (`t=0s < t=3s`). All `12` `lq-heavy` jobs run first across Waves 1–3 (`0s–24s`), pushing all `4` `lq-light` jobs to Wave 4 (`24s–32s`, `AVG ≈ 27s–28s`).

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `75%–100% (16/16 TPUs across 4 waves)` |
| **Section `5.2` Queue Wait Time** | `p-light AVG ~27.8s > p-heavy AVG ~10.4s` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `75%–92% (16 completed)` |

### Detailed Metric Checkpoints

- `Section 5.2 (Wait Time)` & `Section 3 (kueue_local_queue_admission_wait_time_seconds)`: **`p-light` (`lq-light`) wait jumps from `~7.5s` (`st79`) to `AVG ≈ 25s–29s` (`P50 ≈ 30s`)**, while `p-heavy` `AVG` wait drops to `~9.5s–11.5s`.
- `Section 4 (Not Emitted)`: `kueue_local_queue_admission_fair_sharing_usage` is unrecorded (AFS sampling inactive).
- `Section 5.3`: **`0`** evictions.
- `Section 5.4`: **`~75%–92%`** (`16/16` jobs complete in `~38s–45s`).

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ `p-light` jumping ahead of `t=0s` `p-heavy` jobs into Wave 2 (`wait < 12s`) when `admissionFairSharing` is omitted from `Configuration`.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
