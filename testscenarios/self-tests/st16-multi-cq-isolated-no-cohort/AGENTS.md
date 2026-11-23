# Agent Execution & Cross-Examination Guide — `st16-multi-cq-isolated-no-cohort`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st16-multi-cq-isolated-no-cohort`
- **Category**: 4. Multi-CQ & Cohort Borrowing/Reclaim
- **Title**: Multi-ClusterQueue Strict Quota Isolation Without a Cohort
- **Primary Verification Target**: Per-CQ breakdown in `Section 5.1` when `cq-alpha` is 200% overloaded and `cq-beta` is 0% idle with no shared Cohort
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st16` (`--namespace kueue-optimizer-chamber-st16`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16416` | Prometheus `19063` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st17-cohort-borrowing-idle-peer-no-reclaim`](../st17-cohort-borrowing-idle-peer-no-reclaim/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST16 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st16' (API port 16416, Prom port 19063):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st16-multi-cq-isolated-no-cohort --namespace kueue-optimizer-chamber-st16 --port 16416 > /tmp/st16.log 2>&1 &
PID_ST16=$!

# Simultaneously launch paired ST17 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st17' (API port 16417, Prom port 19064):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st17-cohort-borrowing-idle-peer-no-reclaim --namespace kueue-optimizer-chamber-st17 --port 16417 > /tmp/st17.log 2>&1 &
PID_ST17=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST16" "$PID_ST17"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST16 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st16' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st16-multi-cq-isolated-no-cohort --namespace kueue-optimizer-chamber-st16 --port 16416 | tee /tmp/st16.log

# 2. Run paired ST17 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st17':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st17-cohort-borrowing-idle-peer-no-reclaim --namespace kueue-optimizer-chamber-st17 --port 16417 | tee /tmp/st17.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st16-multi-cq-isolated-no-cohort --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st16`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st16-multi-cq-isolated-no-cohort` (`kueue-optimizer-chamber-st16`) ↔ [`st17-cohort-borrowing-idle-peer-no-reclaim`](../st17-cohort-borrowing-idle-peer-no-reclaim/AGENTS.md) (`kueue-optimizer-chamber-st17`) — Exact A/B Twin (Isolated Multi-CQ vs. Shared Cohort Borrowing)

- **Controlled Variable Changed**: ONLY `cohortName: cohort-shared` is added on `cq-alpha` and `cq-beta` in `st17`; both CQs keep `16`-TPU nominal quota and identical 8 jobs in `cq-alpha` (`0` in `cq-beta`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st17-cohort-borrowing-idle-peer-no-reclaim --namespace kueue-optimizer-chamber-st17 --port 16417`
- **Cross-Examination Assertion**: Adding the cohort flips `cq-alpha` `Sec 5.1` usage from `~100%` (`16/16` TPUs in `st16`) to `~200%` (`32/16` TPUs in `st17`), cuts total makespan in half, and doubles `Sec 5.4 EFFECTIVE SAT` from `~50%` (`st16`) to `~96%` (`st17`).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `ClusterQueue: cq-alpha ~100%, cq-beta 0%, All ~50%` (`st16`) vs. `cq-alpha ~200%, Cohort ~100%` (`st17`)
  - `Sec 5.2 Wait Time`: `P50 <=1s, AVG ~0.5*T_wave (~10s @1x)` (`st16`) vs. `< 1.0s` (`st17`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st16`) vs. `0 evictions` (`st17`)
  - `Sec 5.4 Effective Sat`: `~50.0%` (`st16`) vs. `93%–100%` (`st17`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 8-node (`32`-TPU) cluster partitioned into two standalone `ClusterQueue`s (`cq-alpha`: `16` TPUs, `cq-beta`: `16` TPUs, **no `cohortName`**). 8 jobs (`32` TPUs total, `20s` each) arrive in `cq-alpha`; `0` jobs arrive in `cq-beta`.
- **Expected Runtime Behavior**: Because `cq-alpha` and `cq-beta` do not belong to a cohort, `cq-alpha` CANNOT borrow a single TPU from idle `cq-beta`. `cq-alpha` runs 2 waves of 4 jobs (`16` TPUs) over `40s` while `cq-beta` sits at `0%`.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `ClusterQueue: cq-alpha ~100%, cq-beta 0%, All ~50%` |
| **Section `5.2` Queue Wait Time** | `P50 <=1s, AVG ~0.5*T_wave (~10s @1x)` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `~50.0%` |

### Detailed Metric Checkpoints

- `Section 5.1`: Named rows `ClusterQueue: cq-alpha` = **`95%–100%`** (`16/16` TPUs) and `ClusterQueue: cq-beta` = **`0.00%`** (`0/16` TPUs), alongside `ClusterQueue Quotas (All)` = **`~50%`** (`16/32` TPUs).
- `Section 5.2`: Exact 4+4 bimodal wave split (`<=1s: 4` for Wave 1, `~T_wave` for Wave 2 where `T_wave = 20s / speedup`): `P50 <= 1.00s`, `AVG ≈ 0.5 * T_wave` (`~10s` at `1.0x`), `P90 ≈ 0.9 * T_wave` — matching `st01` identically because zero borrowing occurs.
- `Section 5.4`: **`EFFECTIVE SAT ≈ 50.0%`** (`46%–52%` across any `--speedup`).

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ `cq-alpha` using `> 16` TPUs (`> 100%` of its nominal quota) without a Cohort.
- ❌ `cq-beta` showing non-zero resource usage.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
