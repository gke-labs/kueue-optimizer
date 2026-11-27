# Agent Execution & Cross-Examination Guide — `st27-cpu-memory-secondary-bottleneck`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st27-cpu-memory-secondary-bottleneck`
- **Category**: 6. Tricky Accounting & Edge Cases
- **Title**: Multi-Resource Bottleneck Inversion (`cpu` & `memory` at `100%`, `google.com/tpu` at `25%`)
- **Primary Verification Target**: Per-resource reporting across `cpu`, `memory`, `pods`, and `google.com/tpu` in Sections `5.1` and `5.4` when non-TPU host memory/CPU saturates the cluster
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st27` (`--namespace kueue-optimizer-chamber-st27`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16427` | Prometheus `19074` (collision-free across all 95 scenarios `st01`–`st95`)
- **Mandatory Paired Counterpart(s)**: [`st03-25pct-light-underloaded-cluster`](../st03-25pct-light-underloaded-cluster/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st95`) and non-overlapping port pair (`16401/19048` .. `16495/19142`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST27 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st27' (API port 16427, Prom port 19074):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st27-cpu-memory-secondary-bottleneck --namespace kueue-optimizer-chamber-st27 --port 16427 > /tmp/st27.log 2>&1 &
PID_ST27=$!

# Simultaneously launch paired ST03 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st03' (API port 16403, Prom port 19050):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st03-25pct-light-underloaded-cluster --namespace kueue-optimizer-chamber-st03 --port 16403 > /tmp/st03.log 2>&1 &
PID_ST03=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST27" "$PID_ST03"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST27 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st27' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st27-cpu-memory-secondary-bottleneck --namespace kueue-optimizer-chamber-st27 --port 16427 | tee /tmp/st27.log

# 2. Run paired ST03 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st03':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st03-25pct-light-underloaded-cluster --namespace kueue-optimizer-chamber-st03 --port 16403 | tee /tmp/st03.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st27-cpu-memory-secondary-bottleneck --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st27`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st27-cpu-memory-secondary-bottleneck` (`kueue-optimizer-chamber-st27`) ↔ [`st03-25pct-light-underloaded-cluster`](../st03-25pct-light-underloaded-cluster/AGENTS.md) (`kueue-optimizer-chamber-st03`) — Host CPU/Memory `100%` Bottleneck (`st27`) vs. Unconstrained `25%` TPU Load (`st03`) Pair

- **Controlled Variable Changed**: Both `st27` (`16/64` TPUs) and `st03` (`8/32` TPUs) use exact `25.0%` of cluster TPUs, but `st27` consumes `100%` of node `cpu` (`128/128`) and `memory` (`512Gi/512Gi`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st03-25pct-light-underloaded-cluster --namespace kueue-optimizer-chamber-st03 --port 16403`
- **Cross-Examination Assertion**: `google.com/tpu` is `~24%–26%` in BOTH `st27` and `st03`, while `cpu` and `memory` jump from `~25%` (`st03`) to `~95%–100%` (`st27`), proving independent per-resource normalization.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `cpu/mem ~100%, tpu ~25%` (`st27`) vs. `23%–26% (8/32 TPUs)` (`st03`)
  - `Sec 5.2 Wait Time`: `< 1.0s` (`st27`) vs. `< 1.0s` (`st03`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st27`) vs. `0 evictions` (`st03`)
  - `Sec 5.4 Effective Sat`: `cpu/mem ~96%, tpu ~24%–25%` (`st27`) vs. `23%–25%` (`st03`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 8-node cluster where each node has `8` TPUs (`64` total), `16` CPUs (`128` total), and `64Gi` RAM (`512Gi` total). 8 jobs arrive at `t=0s` requesting `16` CPU + `64Gi` RAM (`100%` of host CPU/RAM) but only `2` TPUs (`25%` of host TPUs) for `35s`.
- **Expected Runtime Behavior**: All 8 jobs admit at `t=0s` (1 pod per node), saturating **100%** of cluster `cpu` (`128/128`) and `memory` (`512Gi/512Gi`) while utilizing **exact 25.0%** (`16/64`) of `google.com/tpu`.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `cpu/mem ~100%, tpu ~25%` |
| **Section `5.2` Queue Wait Time** | `< 1.0s` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `cpu/mem ~96%, tpu ~24%–25%` |

### Detailed Metric Checkpoints

- `Section 5.1` & `Section 5.4` per-resource rows: `cpu` = **`93%–100%`**, `memory` = **`93%–100%`**, `google.com/tpu` = **`23%–26%`** (`16/64` TPUs), and `pods` = **`~0.9%`** (`8/880` pods).

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ `memory` (binary `Gi` IEC units: `512Gi = 549,755,813,888` bytes) or `cpu` mis-scaled by `1000x` or `1024/1000` unit conversion bugs.
- ❌ All resource rows reporting identical percentages when `cpu/memory = 100%` and `tpu = 25%`.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
