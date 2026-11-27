# Agent Execution & Cross-Examination Guide — `st91-hash-names-jobset-batch-anonymized-replay`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st91-hash-names-jobset-batch-anonymized-replay`
- **Category**: 19. Anonymization & Sensitive-Data Hashing (--hash-names)
- **Title**: `--hash-names` Anonymized Batch `JobSet` Replay (Independent Hashed Names & Labels)
- **Primary Verification Target**: Verifies `testrunner.py` replays a `--hash-names` trace (`h-<32hex>` `JobSet` names, independent hashed `workloadName`s, hashed sensitive label values, plaintext queues/nodes) with identical metrics to its plaintext twin `st92`
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st91` (`--namespace kueue-optimizer-chamber-st91`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16491` | Prometheus `19138` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st92-hash-names-jobset-batch-plaintext-control`](../st92-hash-names-jobset-batch-plaintext-control/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST91 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st91' (API port 16491, Prom port 19138):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st91-hash-names-jobset-batch-anonymized-replay --namespace kueue-optimizer-chamber-st91 --port 16491 > /tmp/st91.log 2>&1 &
PID_ST91=$!

# Simultaneously launch paired ST92 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st92' (API port 16492, Prom port 19139):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st92-hash-names-jobset-batch-plaintext-control --namespace kueue-optimizer-chamber-st92 --port 16492 > /tmp/st92.log 2>&1 &
PID_ST92=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST91" "$PID_ST92"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST91 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st91' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st91-hash-names-jobset-batch-anonymized-replay --namespace kueue-optimizer-chamber-st91 --port 16491 | tee /tmp/st91.log

# 2. Run paired ST92 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st92':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st92-hash-names-jobset-batch-plaintext-control --namespace kueue-optimizer-chamber-st92 --port 16492 | tee /tmp/st92.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st91-hash-names-jobset-batch-anonymized-replay --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st91`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st91-hash-names-jobset-batch-anonymized-replay` (`kueue-optimizer-chamber-st91`) ↔ [`st92-hash-names-jobset-batch-plaintext-control`](../st92-hash-names-jobset-batch-plaintext-control/AGENTS.md) (`kueue-optimizer-chamber-st92`) — Exact `--hash-names` vs. Plaintext Batch `JobSet` A/B Twin (`st91` Hashed vs. `st92` Plaintext)

- **Controlled Variable Changed**: HMAC-SHA256 `h-<32hex>` workflow names, independent hashed `workloadName`s, and hashed sensitive label values (`st91`) vs. plaintext names/labels (`st92`) on identical cluster, queues, and durations.
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st92-hash-names-jobset-batch-plaintext-control --namespace kueue-optimizer-chamber-st92 --port 16492`
- **Cross-Examination Assertion**: `st91` and `st92` MUST produce matching `Sec 5.1 (~95%–100%)`, `Sec 5.2 (p-std AVG ~5.0s–6.5s)`, `Sec 5.3 (0 evictions)`, and `Sec 5.4 (8 completed, ~90%–100%)`, proving `--hash-names` anonymization preserves replay fidelity.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `~95%–100% (16/16 TPUs)` (`st91`) vs. `~95%–100% (16/16 TPUs)` (`st92`)
  - `Sec 5.2 Wait Time`: `p-std AVG ~5.0s–6.5s (8 WLs)` (`st91`) vs. `p-std AVG ~5.0s–6.5s (8 WLs)` (`st92`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st91`) vs. `0 evictions` (`st92`)
  - `Sec 5.4 Effective Sat`: `~90%–100% (8 completed, == st92)` (`st91`) vs. `~90%–100% (8 completed, == st91)` (`st92`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 4 nodes (`16` TPUs), `cq-hash-batch` (`16` TPUs, `StrictFIFO`). 8 hashed `JobSet`s (`h-<32hex>`, `4` TPUs × `10s`, independent hashed `workloadName`s, hashed `owner`/`team`/`project`/`cost-center` labels) arrive at `t=0s` across 2 waves (`20s` total).
- **Expected Runtime Behavior**: Loader matches every hashed `JobSet` to its independent hashed `workloadName` via `ownerName`, replays 2 waves of 4 `JobSet`s (`0..10s`, `10..20s`) identically to `st92`, and preserves plaintext queue/priority names in Prometheus metrics.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `~95%–100% (16/16 TPUs)` |
| **Section `5.2` Queue Wait Time** | `p-std AVG ~5.0s–6.5s (8 WLs)` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `~90%–100% (8 completed, == st92)` |

### Detailed Metric Checkpoints

- `Section 5.1`: `AVG ~95%–100%` (`16/16` TPUs across 2 waves).
- `Section 5.2`: `p-std` `ADMISSIONS=8`, Wave 1 `~0s–1s`, Wave 2 `~10s–11s`, `AVG ~5.0s–6.5s`.
- `Section 5.3`: **`0`** evictions.
- `Section 5.4`: `Qualifying Workloads: 8 (completed=8)`, `EFFECTIVE SAT ~90%–100%` (matching `st92`).

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ Falling back to default/imputed durations because hashed `workloadName` differs from hashed `JobSet` name.
- ❌ Any plaintext secret/team/project string (`alice-researcher`, `ml-core`, `secret-llm-v4`, `cc-999`) appearing in `st91` files.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
