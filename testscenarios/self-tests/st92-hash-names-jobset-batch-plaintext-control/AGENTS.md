# Agent Execution & Cross-Examination Guide — `st92-hash-names-jobset-batch-plaintext-control`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated via `make self-tests` from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and run:
> ```bash
> make self-tests
> make self-tests-check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st92-hash-names-jobset-batch-plaintext-control`
- **Category**: 19. Anonymization & Sensitive-Data Hashing (--hash-names)
- **Title**: Plaintext Batch `JobSet` Control & Live `--scrape-hash-names` Target (Twin of `st91`)
- **Primary Verification Target**: Exact plaintext counterpart of `st91` carrying realistic sensitive labels (`owner`, `team`, `project`, `corp.example.com/cost-center`) for A/B metric parity and live `--scrape-hash-names` validation
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st92` (`--namespace kueue-optimizer-chamber-st92`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16492` | Prometheus `19139` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st91-hash-names-jobset-batch-anonymized-replay`](../st91-hash-names-jobset-batch-anonymized-replay/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST92 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st92' (API port 16492, Prom port 19139):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st92-hash-names-jobset-batch-plaintext-control --namespace kueue-optimizer-chamber-st92 --port 16492 > /tmp/st92.log 2>&1 &
PID_ST92=$!

# Simultaneously launch paired ST91 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st91' (API port 16491, Prom port 19138):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st91-hash-names-jobset-batch-anonymized-replay --namespace kueue-optimizer-chamber-st91 --port 16491 > /tmp/st91.log 2>&1 &
PID_ST91=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST92" "$PID_ST91"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST92 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st92' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st92-hash-names-jobset-batch-plaintext-control --namespace kueue-optimizer-chamber-st92 --port 16492 | tee /tmp/st92.log

# 2. Run paired ST91 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st91':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st91-hash-names-jobset-batch-anonymized-replay --namespace kueue-optimizer-chamber-st91 --port 16491 | tee /tmp/st91.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st92-hash-names-jobset-batch-plaintext-control --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st92`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st92-hash-names-jobset-batch-plaintext-control` (`kueue-optimizer-chamber-st92`) ↔ [`st91-hash-names-jobset-batch-anonymized-replay`](../st91-hash-names-jobset-batch-anonymized-replay/AGENTS.md) (`kueue-optimizer-chamber-st91`) — Plaintext Control Twin of `st91` & Live `--scrape-hash-names` Capture Source

- **Controlled Variable Changed**: Plaintext names & sensitive organizational labels (`st92`) vs. pre-hashed `--hash-names` bundle (`st91`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st91-hash-names-jobset-batch-anonymized-replay --namespace kueue-optimizer-chamber-st91 --port 16491`
- **Cross-Examination Assertion**: Replay metrics MUST match `st91`, and attaching `--scrape-hash-names` to `st92` MUST strip all raw names and sensitive label values (`alice-researcher`, `ml-core`, `secret-llm-v4`, `cc-999`).
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `~95%–100% (16/16 TPUs)` (`st92`) vs. `~95%–100% (16/16 TPUs)` (`st91`)
  - `Sec 5.2 Wait Time`: `p-std AVG ~5.0s–6.5s (8 WLs)` (`st92`) vs. `p-std AVG ~5.0s–6.5s (8 WLs)` (`st91`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st92`) vs. `0 evictions` (`st91`)
  - `Sec 5.4 Effective Sat`: `~90%–100% (8 completed, == st91)` (`st92`) vs. `~90%–100% (8 completed, == st92)` (`st91`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: Identical to `st91` (`4` nodes / `16` TPUs, `cq-hash-batch`, `8` `JobSet`s × `4` TPUs × `10s`) using plaintext names `batch-train-00..07` and sensitive organizational labels (`alice-researcher`, `ml-core`, `secret-llm-v4`, `cc-999`).
- **Expected Runtime Behavior**: Replays identically to `st91` (`2` waves, `~20s`). When run with `--scrape-output-dir /tmp/st92-scraped --scrape-hash-names`, `cluster-scraper` must replace every `batch-train-*` name and sensitive label value with deterministic `h-<32hex>` digests.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `~95%–100% (16/16 TPUs)` |
| **Section `5.2` Queue Wait Time** | `p-std AVG ~5.0s–6.5s (8 WLs)` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `~90%–100% (8 completed, == st91)` |

### Detailed Metric Checkpoints

- `Section 5.1`: `AVG ~95%–100%` (matches `st91`).
- `Section 5.2`: `p-std` `WORKLOADS=8`, `AVG ~5.0s–6.5s` (matches `st91`).
- `Section 5.3`: **`0`** evictions (matches `st91`).
- `Section 5.4`: `Qualifying Workloads: 8 (completed=8)`, `EFFECTIVE SAT ~90%–100%` (matches `st91`).

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ Metric divergence between `st91` (hashed) and `st92` (plaintext) beyond normal `~1s` chamber jitter.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
make self-tests
make self-tests-check
```
