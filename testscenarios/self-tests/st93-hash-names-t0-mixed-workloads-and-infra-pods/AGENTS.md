# Agent Execution & Cross-Examination Guide — `st93-hash-names-t0-mixed-workloads-and-infra-pods`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st93-hash-names-t0-mixed-workloads-and-infra-pods`
- **Category**: 19. Anonymization & Sensitive-Data Hashing (--hash-names)
- **Title**: `--hash-names` `t0` Mixed Kueue `JobSet`s & Unmanaged `infra-pods` Separation
- **Primary Verification Target**: Verifies hashed `t0` workflow pods (`h-<32hex>` linked via `ownerName`/`workloadName`) are filtered out of `infra-pods` and bootstrapped as `InitialWorkloadPodGroup`s while hashed non-Kueue pods remain in `infra-pods`
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st93` (`--namespace kueue-optimizer-chamber-st93`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16493` | Prometheus `19140` (collision-free across all 95 scenarios `st01`–`st95`)
- **Mandatory Paired Counterpart(s)**: [`st32-t0-mixed-kueue-workloads-and-infra-pods`](../st32-t0-mixed-kueue-workloads-and-infra-pods/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st95`) and non-overlapping port pair (`16401/19048` .. `16495/19142`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST93 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st93' (API port 16493, Prom port 19140):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st93-hash-names-t0-mixed-workloads-and-infra-pods --no-skip-initial-offset --namespace kueue-optimizer-chamber-st93 --port 16493 > /tmp/st93.log 2>&1 &
PID_ST93=$!

# Simultaneously launch paired ST32 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st32' (API port 16432, Prom port 19079):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st32-t0-mixed-kueue-workloads-and-infra-pods --namespace kueue-optimizer-chamber-st32 --port 16432 > /tmp/st32.log 2>&1 &
PID_ST32=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST93" "$PID_ST32"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST93 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st93' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st93-hash-names-t0-mixed-workloads-and-infra-pods --no-skip-initial-offset --namespace kueue-optimizer-chamber-st93 --port 16493 | tee /tmp/st93.log

# 2. Run paired ST32 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st32':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st32-t0-mixed-kueue-workloads-and-infra-pods --namespace kueue-optimizer-chamber-st32 --port 16432 | tee /tmp/st32.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st93-hash-names-t0-mixed-workloads-and-infra-pods --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st93`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st93-hash-names-t0-mixed-workloads-and-infra-pods` (`kueue-optimizer-chamber-st93`) ↔ [`st32-t0-mixed-kueue-workloads-and-infra-pods`](../st32-t0-mixed-kueue-workloads-and-infra-pods/AGENTS.md) (`kueue-optimizer-chamber-st32`) — Hashed `t0` Hybrid `JobSet` + `infra-pods` (`st93`) vs. Plaintext `t0` Hybrid Baseline (`st32`)

- **Controlled Variable Changed**: All `t0` workflow, workload, and unmanaged pod names replaced with independent `h-<32hex>` digests (`st93`) vs. plaintext names (`st32`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st32-t0-mixed-kueue-workloads-and-infra-pods --namespace kueue-optimizer-chamber-st32 --port 16432`
- **Cross-Examination Assertion**: `st93` MUST separate exact `2` `InitialWorkloadPodGroup`s and `2` `Initial Infra Pods` (zero leakage of hashed workflow pods into `infra-pods`), matching `st32`'s hybrid separation behavior.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `~75%–95% (12/12 CQ TPUs)` (`st93`) vs. `~92%–100% (24/24 CQ TPUs)` (`st32`)
  - `Sec 5.2 Wait Time`: `p-std AVG ~5s (0s / ~10s)` (`st93`) vs. `< 1.5s` (`st32`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st93`) vs. `0 evictions` (`st32`)
  - `Sec 5.4 Effective Sat`: `4 completed + 2 infra pods` (`st93`) vs. `~90%–98% (6 Kueue + 2 infra = 32/32 TPUs)` (`st32`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 4 nodes (`16` TPUs), `cq-hash-t0` (`12` TPUs). At `t0`: 2 pre-admitted hashed `JobSet`s (`4` TPUs × `12s` = `8` TPUs on `st-node-00..01`) + 2 hashed non-Kueue unmanaged pods (`2` TPUs each = `4` TPUs packed on `st-node-02`, leaving `st-node-03` free). At `t=+2s`: 2 hashed `JobSet`s (`4` TPUs × `12s`) arrive.
- **Expected Runtime Behavior**: Loader produces exact `2` `InitialWorkloadPodGroup`s (occupying `8/12` CQ TPUs) and `2` `InitialPodSpec`s in `infra-pods` (occupying `4` physical TPUs on `st-node-02` without CQ quota). Post-`t0` job 0 admits immediately (`8+4=12/12`, schedules onto `st-node-03`); job 1 waits `~10s` until a `t0` `JobSet` finishes at `t=12s`.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `~75%–95% (12/12 CQ TPUs)` |
| **Section `5.2` Queue Wait Time** | `p-std AVG ~5s (0s / ~10s)` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `4 completed + 2 infra pods` |

### Detailed Metric Checkpoints

- `Pre-Run Outline`: `Initial Kueue Workloads: 2 (2 pods)`, `Initial Infra Pods: 2`.
- `Section 5.1`: `AVG ~75%–95%` (`12/12` TPUs during `t=2..10s`).
- `Section 5.2`: `p-std` `ADMISSIONS=2` (post-`t0`), job 0 `wait < 2s`, job 1 `wait ~10s`, `AVG ~5s`.
- `Section 5.3`: **`0`** evictions.
- `Section 5.4`: `4` Kueue completed workloads + `2` `Initial Infra Pods` (`active_at_end`), `0` double-counted pods.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ Leaking the 2 hashed `t0` `JobSet` pods into `infra-pods` (`Initial Infra Pods` MUST be `2`, never `4`).
- ❌ Dropping the 2 genuine hashed unmanaged pods from `infra-pods`.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
