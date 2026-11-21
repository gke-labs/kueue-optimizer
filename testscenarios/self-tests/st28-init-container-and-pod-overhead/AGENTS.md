# Agent Execution & Cross-Examination Guide — `st28-init-container-and-pod-overhead`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and re-run:
> ```bash
> python3 testscenarios/self-tests/generate_self_test_suite.py
> python3 deploy/scripts/generate_scenario_readme.py --all
> python3 testscenarios/self-tests/generate_self_test_suite.py --check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st28-init-container-and-pod-overhead`
- **Category**: 6. Tricky Accounting & Edge Cases
- **Title**: Kubernetes `initContainers` High-Water Mark & `spec.overhead` Resource Formula
- **Primary Verification Target**: Verifies `effective_req = max(max(initContainers), sum(containers)) + overhead` in `Section 5.4` matches Kueue `Section 5.1`
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st28` (`--namespace kueue-optimizer-chamber-st28`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16428` | Prometheus `19075` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st02-100pct-full-cluster-saturation`](../st02-100pct-full-cluster-saturation/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST28 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st28' (API port 16428, Prom port 19075):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st28-init-container-and-pod-overhead --namespace kueue-optimizer-chamber-st28 --port 16428 > /tmp/st28.log 2>&1 &
PID_ST28=$!

# Simultaneously launch paired ST02 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st02' (API port 16402, Prom port 19049):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st02-100pct-full-cluster-saturation --namespace kueue-optimizer-chamber-st02 --port 16402 > /tmp/st02.log 2>&1 &
PID_ST02=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST28" "$PID_ST02"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST28 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st28' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st28-init-container-and-pod-overhead --namespace kueue-optimizer-chamber-st28 --port 16428 | tee /tmp/st28.log

# 2. Run paired ST02 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st02':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st02-100pct-full-cluster-saturation --namespace kueue-optimizer-chamber-st02 --port 16402 | tee /tmp/st02.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st28-init-container-and-pod-overhead --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st28`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st28-init-container-and-pod-overhead` (`kueue-optimizer-chamber-st28`) ↔ [`st02-100pct-full-cluster-saturation`](../st02-100pct-full-cluster-saturation/AGENTS.md) (`kueue-optimizer-chamber-st02`) — `initContainers` High-Water Mark + `spec.overhead` (`st28`) vs. Standard Container Spec (`st02`) Pair

- **Controlled Variable Changed**: `st28` splits the `4`-TPU request between `initContainer` (`4` TPUs) and main `container` (`2` TPUs) and adds `spec.overhead.cpu: 2`, whereas `st02` puts `4` TPUs directly on `container`.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st02-100pct-full-cluster-saturation --namespace kueue-optimizer-chamber-st02 --port 16402`
- **Cross-Examination Assertion**: `Sec 5.4` TPU saturation MUST be `93%–100%` in BOTH `st28` and `st02` (`max(4, 2) = 4` TPUs, never `50%` and never `150%`), and `cpu` in `st28` MUST be `~46%–50%` (`64 / 128` CPUs).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `tpu ~100%, cpu ~50%` (`st28`) vs. `95%–100% (32/32 TPUs)` (`st02`)
  - `Sec 5.2 Wait Time`: `< 1.0s` (`st28`) vs. `< 1.0s (immediate)` (`st02`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st28`) vs. `0 evictions` (`st02`)
  - `Sec 5.4 Effective Sat`: `tpu ~96% (32/32), cpu ~48% (64/128)` (`st28`) vs. `93%–100%` (`st02`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 8-node (`32`-TPU, `128`-CPU) cluster. Each of 8 jobs (`35s`) defines an `initContainer` requesting `4` TPUs + a main `container` requesting only `2` TPUs (`effective = max(4, 2) = 4` TPUs), plus `6` container CPUs + `2` `spec.overhead.cpu` (`effective = 6 + 2 = 8` CPUs = `50%` of node's `16` CPUs).
- **Expected Runtime Behavior**: Both Kueue (`Section 5.1`) and `kueue_metrics_report.py` (`Section 5.4`) apply the exact Kubernetes pod resource formula (`max(initContainers, sum(containers)) + overhead`), reporting **`100%` TPU saturation** (`8 × 4 = 32` TPUs, NOT `8 × 2 = 16` = `50%`) and **`50%` CPU saturation** (`8 × (6+2) = 64 / 128` CPUs, NOT `48/128 = 37.5%`).

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `tpu ~100%, cpu ~50%` |
| **Section `5.2` Queue Wait Time** | `< 1.0s` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `tpu ~96% (32/32), cpu ~48% (64/128)` |

### Detailed Metric Checkpoints

- `Section 5.1` & `Section 5.4`: `google.com/tpu` = **`93%–100%`** (`32/32` TPUs), `cpu` = **`46%–50%`** (`64/128` CPUs).

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ `Section 5.4` ignoring `initContainers` and reporting only `50%` TPU saturation (`16/32` from main container alone) while `Section 5.1` reports `100%`.
- ❌ `Section 5.4` summing `initContainer + container` (`4 + 2 = 6` TPUs = `150%`) instead of taking `max(4, 2) = 4`.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
python3 testscenarios/self-tests/generate_self_test_suite.py
python3 deploy/scripts/generate_scenario_readme.py --all
python3 testscenarios/self-tests/generate_self_test_suite.py --check
```
