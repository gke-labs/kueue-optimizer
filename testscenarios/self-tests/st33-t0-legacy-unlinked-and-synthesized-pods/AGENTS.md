# Agent Execution & Cross-Examination Guide — `st33-t0-legacy-unlinked-and-synthesized-pods`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st33-t0-legacy-unlinked-and-synthesized-pods`
- **Category**: 7. Initial t0 Workloads — Handling & Accounting
- **Title**: Legacy Unlinked Hashed `t0` Pods (Tier 3) & Synthetic Missing-Pod Placement (Tier 4)
- **Primary Verification Target**: Backward-compatible Tier 3 resource-profile matching for `--hash-names` traces without `workloadName` + Tier 4 automatic pod synthesis when `infra.json` omits `t0` pod events
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st33` (`--namespace kueue-optimizer-chamber-st33`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16433` | Prometheus `19080` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st32-t0-mixed-kueue-workloads-and-infra-pods`](../st32-t0-mixed-kueue-workloads-and-infra-pods/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST33 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st33' (API port 16433, Prom port 19080):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st33-t0-legacy-unlinked-and-synthesized-pods --namespace kueue-optimizer-chamber-st33 --port 16433 > /tmp/st33.log 2>&1 &
PID_ST33=$!

# Simultaneously launch paired ST32 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st32' (API port 16432, Prom port 19079):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st32-t0-mixed-kueue-workloads-and-infra-pods --namespace kueue-optimizer-chamber-st32 --port 16432 > /tmp/st32.log 2>&1 &
PID_ST32=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST33" "$PID_ST32"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST33 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st33' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st33-t0-legacy-unlinked-and-synthesized-pods --namespace kueue-optimizer-chamber-st33 --port 16433 | tee /tmp/st33.log

# 2. Run paired ST32 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st32':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st32-t0-mixed-kueue-workloads-and-infra-pods --namespace kueue-optimizer-chamber-st32 --port 16432 | tee /tmp/st32.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st33-t0-legacy-unlinked-and-synthesized-pods --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st33`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st33-t0-legacy-unlinked-and-synthesized-pods` (`kueue-optimizer-chamber-st33`) ↔ [`st32-t0-mixed-kueue-workloads-and-infra-pods`](../st32-t0-mixed-kueue-workloads-and-infra-pods/AGENTS.md) (`kueue-optimizer-chamber-st32`) — Tier-3/4 Fallback Matching (`st33`) vs. Tier-1 Enriched Linkage (`st32`) Pair

- **Controlled Variable Changed**: 4 unlinked `--hash-names` pods + 2 podless `Admitted` workflows (`st33`) vs. enriched scraper pod linkage (`st32`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st32-t0-mixed-kueue-workloads-and-infra-pods --namespace kueue-optimizer-chamber-st32 --port 16432`
- **Cross-Examination Assertion**: `st33` bootstraps `6` `PodGroup`s (`6` pods on `st-node-00..05`), reports `Initial Infra Pods: 0`, and runs 2 arrivals on `st-node-06..07` (`32/32` TPUs, `failed=0`).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `~90%–100% (32/32 TPUs)` (`st33`) vs. `~92%–100% (24/24 CQ TPUs)` (`st32`)
  - `Sec 5.2 Wait Time`: `< 1.5s` (`st33`) vs. `< 1.5s` (`st32`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st33`) vs. `0 evictions` (`st32`)
  - `Sec 5.4 Effective Sat`: `~88%–98% (8 Kueue workloads, 0 infra)` (`st33`) vs. `~90%–98% (6 Kueue + 2 infra = 32/32 TPUs)` (`st32`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 8-node (`32`-TPU) cluster, `cq-legacy` (`32` TPUs). At `t0`: 4 hashed `Admitted` `JobSet`s (`h-000...01..04`) accompany 4 unlinked hashed `Pod` events on `st-node-00..03` (`pod_mode='unlinked'`), plus 2 `Admitted` `JobSet`s (`init-synth-0..1`) with zero `Pod` events in `infra.json` (`pod_mode='none'`). 2 `NotAdmitted` arrivals (`arr-legacy-0..1`, `30s`) submit at `t=0s`.
- **Expected Runtime Behavior**: Tier 3 claims all 4 unlinked hashed pods into `PodGroup` `Workload`s (`Initial Infra Pods: 0`), Tier 4 synthesizes 2 non-colliding pods onto `st-node-04..05`, and `arr-legacy-0..1` schedule onto `st-node-06..07` (`32/32` TPUs).

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `~90%–100% (32/32 TPUs)` |
| **Section `5.2` Queue Wait Time** | `< 1.5s` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `~88%–98% (8 Kueue workloads, 0 infra)` |

### Detailed Metric Checkpoints

- `Execution Summary`: `Initial Kueue Workloads: 6 PodGroup(s) (6 pod(s))`, `Initial Infra Pods: 0`, `Workflows Submitted: 2/2`, `KWOK Pods Status: ... succeeded=8, failed=0`.
- `Section 5.1 (cq-legacy)`: **`~90%–100%`** (`32/32` TPUs).
- `Section 5.4 (Effective Saturation)`: `Qualifying Workloads: 8 (completed=8 [8 pods])`, **`EFFECTIVE SAT ≈ 88%–98%`**.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ Unlinked hashed pods falling through to `Initial Infra Pods: 4` when matching `Admitted` `workflows.json` candidates exist.
- ❌ Tier 4 synthetic pods colliding onto `st-node-00..03` (already full with `4` TPUs each).

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
