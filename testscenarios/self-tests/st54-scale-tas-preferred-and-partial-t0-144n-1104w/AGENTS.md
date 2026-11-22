# Agent Execution & Cross-Examination Guide — `st54-scale-tas-preferred-and-partial-t0-144n-1104w`

> **AUTO-GENERATED FILE — DO NOT HAND-EDIT.**
> This file is generated from [`../generate_self_test_suite.py`](../generate_self_test_suite.py).
> If you change any scenario definition, pairing rule, or expected metric band, update `generate_self_test_suite.py` and re-run:
> ```bash
> python3 testscenarios/self-tests/generate_self_test_suite.py
> python3 deploy/scripts/generate_scenario_readme.py --all
> python3 testscenarios/self-tests/generate_self_test_suite.py --check
> ```

## 1. Scenario Identity & Dedicated Chamber Isolation

- **Scenario ID / Directory**: `st54-scale-tas-preferred-and-partial-t0-144n-1104w`
- **Category**: 11. Large-Scale TAS Packing, Slices & Multi-Pool (>100 Nodes, >1,000 Workloads)
- **Title**: 144-Node, 1,104-Workload Partial Vanilla-K8s `t0` TAS Binding + Required/Preferred/Unconstrained Modes
- **Primary Verification Target**: Tests vanilla K8s `t0` snapshots where `24` of `48` `2`-pod TAS jobs have only `1/2` pods bound in `infra.json` (Tier 4 synthesizes missing pods onto `topologyAssignment` domains) plus all 3 TAS constraint annotations
- **Dedicated `kueue-optimizer` Chamber Namespace**: `kueue-optimizer-chamber-st54` (`--namespace kueue-optimizer-chamber-st54`)
- **Dedicated Local Port Allocation**: KWOK API `--port 16454` | Prometheus `19101` (collision-free across all 45 scenarios `st01`–`st45`)
- **Mandatory Paired Counterpart(s)**: [`st53-scale-tas-backfill-best-effort-128n-1056w`](../st53-scale-tas-backfill-best-effort-128n-1056w/AGENTS.md)
- **Suite Master Protocol**: See [`../AGENTS.md`](../AGENTS.md) and [`../README.md`](../README.md); auto-generated static stats in [`README.md`](README.md).

---

## 2. How to Fire This Test (Strict Separate-Chamber Isolation)

**CRITICAL METRIC-ISOLATION RULE**: Never run paired scenarios in the same `kueue-optimizer` chamber with `--skip-chamber-deploy`. Kueue Prometheus counters (`kueue_admitted_workloads_total`, `kueue_evicted_workloads_total`), cumulative histogram buckets (`kueue_admission_wait_time_seconds_bucket`, `kueue_pods_ready_to_evicted_time_seconds_bucket`), and chamber Prometheus TSDB series persist inside a running chamber and will contaminate subsequent runs. **Always fire each scenario in its own dedicated `kueue-optimizer` chamber namespace (`--namespace kueue-optimizer-chamber-stXX --port 164XX`)** so every run starts with a brand-new `etcd`, `kueue-controller-manager`, and `prometheus` instance at exact zero baseline.

### Option A: Parallel Side-by-Side Execution in Separate `kueue-optimizer` Chambers

Because every scenario in `testscenarios/self-tests/` has its own unique `kueue-optimizer` chamber namespace (`kueue-optimizer-chamber-st01` .. `kueue-optimizer-chamber-st45`) and non-overlapping port pair (`16401/19048` .. `16445/19092`), you can fire this scenario and its paired counterpart(s) **concurrently in separate chambers** with zero cross-contamination:

```bash
# Launch ST54 in dedicated kueue-optimizer chamber 'kueue-optimizer-chamber-st54' (API port 16454, Prom port 19101):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st54-scale-tas-preferred-and-partial-t0-144n-1104w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st54 --port 16454 > /tmp/st54.log 2>&1 &
PID_ST54=$!

# Simultaneously launch paired ST53 in its own separate kueue-optimizer chamber 'kueue-optimizer-chamber-st53' (API port 16453, Prom port 19100):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st53-scale-tas-backfill-best-effort-128n-1056w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st53 --port 16453 > /tmp/st53.log 2>&1 &
PID_ST53=$!

# Wait for all isolated kueue-optimizer chambers to finish and tear down cleanly:
wait "$PID_ST54" "$PID_ST53"
```

### Option B: Sequential Execution in Separate Fresh `kueue-optimizer` Chambers

Run from the repository root:

```bash
# 1. Run ST54 in its own isolated kueue-optimizer chamber 'kueue-optimizer-chamber-st54' (auto-deployed & auto-cleaned):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st54-scale-tas-preferred-and-partial-t0-144n-1104w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st54 --port 16454 | tee /tmp/st54.log

# 2. Run paired ST53 in its own separate fresh kueue-optimizer chamber 'kueue-optimizer-chamber-st53':
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st53-scale-tas-backfill-best-effort-128n-1056w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st53 --port 16453 | tee /tmp/st53.log

# Fast offline manifest & timeline sanity check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/self-tests/st54-scale-tas-preferred-and-partial-t0-144n-1104w --dry-run
```

---

## 3. Mandatory Side-by-Side Paired Cross-Examination Protocol

Never evaluate this scenario in isolation when validating scheduler behavior or `kueue_metrics_report.py` changes. Always execute it in its dedicated `kueue-optimizer` chamber (`kueue-optimizer-chamber-st54`) alongside its paired counterpart(s) in their dedicated chamber(s) below and verify every cross-scenario delta:

### Pair: `st54-scale-tas-preferred-and-partial-t0-144n-1104w` (`kueue-optimizer-chamber-st54`) ↔ [`st53-scale-tas-backfill-best-effort-128n-1056w`](../st53-scale-tas-backfill-best-effort-128n-1056w/AGENTS.md) (`kueue-optimizer-chamber-st53`) — Partial Vanilla-K8s `t0` TAS Synthesis + 3 TAS Modes (`st54`) vs. TAS Rack Backfill (`st53`)

- **Controlled Variable Changed**: `24` partially bound `t0` TAS jobs (`1/2` pods in `infra.json`) + `preferred`/`unconstrained` TAS modes (`st54`) vs. `required`-only rack backfill (`st53`).
- **Isolated Peer Command**: `python3 deploy/scripts/testrunner.py testscenarios/self-tests/st53-scale-tas-backfill-best-effort-128n-1056w --no-skip-initial-offset --namespace kueue-optimizer-chamber-st53 --port 16453`
- **Cross-Examination Assertion**: `st54` reports `Initial Kueue Workloads: 48 PodGroup(s) (96 pod(s))` (verifying partial-pod Tier 4 synthesis) and `1,104` completed workloads.
- **Expected Target Shift**:
  - `Sec 5.1 Quota Util`: `Peak 100% (1,152/1,152 TPUs), AVG ~68%–90%` (`st54`) vs. `Backfill > 90% during rack block, AVG ~68%–90%` (`st53`)
  - `Sec 5.2 Wait Time`: `3 TAS modes co-scheduled cleanly` (`st54`) vs. `Full-rack jobs wait >= 15s; small jobs backfill` (`st53`)
  - `Sec 5.3 Evictions`: `0 evictions` (`st54`) vs. `0 evictions` (`st53`)
  - `Sec 5.4 Effective Sat`: `~62%–90% (1,104 completed)` (`st54`) vs. `~64%–90% (1,056 completed)` (`st53`)

---

## 4. Scenario Mechanics & Expected Live Metrics

- **Setup**: 144 nodes (`36` racks, `1,152` TPUs) with TAS. At `t0`, `48` `Admitted` `2`-pod (`8`-TPU) TAS `Job`s (`384` TPUs): `24` have both pods bound in `infra.json` (`48` pods) and `24` have `1` bound pod + `podSetAssignments[].topologyAssignment` (`24` bound + `24` Tier-4-synthesized = `96` `t0` pods total!). Arrivals: `1,056` `4`-TPU TAS `Job`s evenly split (`352` each) across `podset-required-topology`, `podset-preferred-topology`, and `podset-unconstrained-topology` (**`1,104` workloads**).
- **Expected Runtime Behavior**: Loader reconstructs all `48` `t0` `PodGroup`s with exact **`96` `t0` pods** (`384` TPUs) placed on their exact `2x2x2-id` node pairs, and Kueue schedules all `1,056` required/preferred/unconstrained arrivals across `144` nodes with `0` failures.

### Target Metric Scorecard

| Report Section | Expected Healthy Band |
| :--- | :--- |
| **Section `5.1` Quota Utilization** | `Peak 100% (1,152/1,152 TPUs), AVG ~68%–90%` |
| **Section `5.2` Queue Wait Time** | `3 TAS modes co-scheduled cleanly` |
| **Section `5.3` Evictions & Reclaims** | `0 evictions` |
| **Section `5.4` Effective Saturation** | `~62%–90% (1,104 completed)` |

### Detailed Metric Checkpoints

- `Execution Summary`: `Initial Kueue Workloads: 48 PodGroup(s) (96 pod(s))` (not `72` pods!), `Workflows Submitted: 1056/1056`.
- `Section 5.4`: `Qualifying Workloads: 1104 (completed=1104 [1152 pods])`.

### Hard Failure Invariants (What Definitely MUST NOT Happen)

- ❌ Under-counting `t0` quota at `288` TPUs (`72` pods) when `24` vanilla-K8s `t0` pods were still unscheduled at scrape time.
- ❌ Placing synthesized `t0` TAS pods outside their `topologyAssignment` node-pair domain.

---

## 5. Synchronization & Anti-Drift Rule

If `infra.json`, `workflows.json`, `execution-time.json`, `README.md`, or this `AGENTS.md` gets out of sync with `generate_self_test_suite.py` or `deploy/scripts/generate_scenario_readme.py`, immediately synchronize all artifacts and verify zero drift:

```bash
python3 testscenarios/self-tests/generate_self_test_suite.py
python3 deploy/scripts/generate_scenario_readme.py --all
python3 testscenarios/self-tests/generate_self_test_suite.py --check
```
