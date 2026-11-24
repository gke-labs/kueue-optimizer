# Agent Execution & Cross-Examination Guide — `st94-hash-names-deployment-salted-upgrade-and-scale`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st94-hash-names-deployment-salted-upgrade-and-scale`
- **Category**: 19. Anonymization & Sensitive-Data Hashing (--hash-names)
- **Title**: `--hash-names` `Deployment` Salted `templateHash` Rolling `Upgrade` & `ScaleDown -> 0`
- **Primary Verification Target**: Verifies an anonymized `Deployment` (`h-<32hex>` name & matching hashed selectors) rolls over cleanly across distinct HMAC-salted `th-<16hex>` `templateHash` annotations, releases quota on `ScaleDown 4 -> 2` to unblock a hashed batch `Job`, and credits all retired replicas on `ScaleDown -> 0`
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st94` (`--namespace kueue-optimizer-chamber-st94`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16494` | Prometheus `19141` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st85-deployment-rolling-upgrade-max-unavailable`](../st85-deployment-rolling-upgrade-max-unavailable/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST94 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st94' (API port 16494, Prom port 19141):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st94-hash-names-deployment-salted-upgrade-and-scale --duration 34s --namespace kueue-optimizer-chamber-st94 --port 16494 > /tmp/st94.log 2>&1 &
PID_ST94=$!

# Simultaneously launch paired ST85 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st85' (API port 16485, Prom port 19132):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st85-deployment-rolling-upgrade-max-unavailable --duration 40s --namespace kueue-optimizer-chamber-st85 --port 16485 > /tmp/st85.log 2>&1 &
PID_ST85=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST94" "$PID_ST85"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST94 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st94' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st94-hash-names-deployment-salted-upgrade-and-scale --duration 34s --namespace kueue-optimizer-chamber-st94 --port 16494 | tee /tmp/st94.log

# 2. Run paired ST85 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st85':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st85-deployment-rolling-upgrade-max-unavailable --duration 40s --namespace kueue-optimizer-chamber-st85 --port 16485 | tee /tmp/st85.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st94-hash-names-deployment-salted-upgrade-and-scale --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st94`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st94-hash-names-deployment-salted-upgrade-and-scale` (`kueue-optimizer-chamber-st94`) ↔ [`st85-deployment-rolling-upgrade-max-unavailable`](../st85-deployment-rolling-upgrade-max-unavailable/AGENTS.md) (`kueue-optimizer-chamber-st85`) — Hashed `Deployment` Salted `templateHash` Upgrade & ScaleDown (`st94`) vs. Plaintext `Deployment` Rolling Upgrade (`st85`)

- **Controlled Variable Changed**: `h-<32hex>` Deployment name/selectors + HMAC-salted `th-<16hex>` `templateHash` + `ScaleDown 4 -> 2 -> 0` with hashed batch `Job` (`st94`) vs. plaintext rolling upgrade (`st85`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st85-deployment-rolling-upgrade-max-unavailable --duration 40s --namespace kueue-optimizer-chamber-st85 --port 16485`
- **Cross-Examination Assertion**: `st94` rolls all `4` v1 pods to `4` v2 pods cleanly (`0` evictions), unblocks the hashed batch `Job` at `t=+18s`, and credits all `9` workloads (`completed=9, active=0, excluded=0`).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `~90%–100% (8/8 CQ TPUs)` (`st94`) vs. `88%–100% (~8/8 CQ TPUs across rollout)` (`st85`)
  - `Sec 5.2 Wait Time`: `9 WLs (batch wait ~2s)` (`st94`) vs. `p-std < 2.5s (8 total admitted: 4 v1 + 4 v2)` (`st85`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st94`) vs. `0 evictions` (`st85`)
  - `Sec 5.4 Effective Sat`: `completed=9, active=0, excluded=0` (`st94`) vs. `44%–50% (completed=4 v1 + active_at_end=4 v2)` (`st85`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 4 nodes (`16` TPUs), `cq-hash-dep` (`8` TPUs). Hashed `Deployment` (`replicas: 4`, `2` TPUs/pod = `8` TPUs, `maxUnavailable: 1, maxSurge: 0`) rolls from salted `th-2c68b6005f729c08` to `th-51f79e685f6edc12` at `t=+8s`; hashed batch `Job` (`4` TPUs, `6s`) arrives at `t=+16s`; `ScaleDown 4 -> 2` at `t=+18s` unblocks it; `ScaleDown 2 -> 0` at `t=+26s`.
- **Expected Runtime Behavior**: Distinct salted `templateHash` triggers a real `ReplicaSet` rollover (`4` v1 pods cleanly replaced by `4` v2 pods under `8`-TPU cap). At `t=+18s`, `ScaleDown 4 -> 2` frees `4` TPUs so the hashed batch `Job` admits (`wait ~2s`) and finishes at `~t=24s`. At `t=+26s`, `ScaleDown 2 -> 0` retires the last `2` replicas cleanly (`0` evictions).

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `~90%–100% (8/8 CQ TPUs)` |
| **Section `5.2` Queue Wait Time** | `9 WLs (batch wait ~2s)` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `completed=9, active=0, excluded=0` |

### Detailed Metric Checkpoints

- `Execution Summary`: `pods running=0, succeeded=1` (hashed batch `Job`).
- `Section 5.2`: `p-std` records `9` admitted workloads (`4` v1 + `4` v2 serving replicas + `1` hashed batch `Job`).
- `Section 5.3`: **`0`** evictions.
- `Section 5.4`: `Qualifying Workloads: 9 (completed=9 [9 pods], active_at_end=0 [0 pods])`, `Excluded: 0`, `Unadmitted: 0`.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ Selector mismatch between hashed `spec.selector.matchLabels` and hashed `spec.template.metadata.labels`.
- ❌ Dropping the rolling `Upgrade` because `templateHash` was stripped or colliding.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
