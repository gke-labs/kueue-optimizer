# Agent Guide — `testscenarios/demo`

## 1. Scenario Purpose & Hypothesis

Baseline smoke-test and demonstration scenario for **`kueue-optimizer` evaluation chambers**:

- **Cluster Topology**: `10` simulated KWOK CPU nodes (`kwok-node-0`..`kwok-node-9`), each with `4` CPU cores, `16Gi` memory, and `110` max pods (`40` CPU cores and `160Gi` RAM total).
- **Queue Configuration**: Single `ResourceFlavor` (`default-flavor`), single `ClusterQueue` (`ai-lab-cq`, `StrictFIFO`, nominal quota `40` CPU / `160Gi` memory / `1100` pods, preemption disabled), and single `LocalQueue` (`ai-lab/ai-lab-lq`).
- **Workload Mix**: `20` multi-pod `JobSet` arrivals (`jobset-01`..`jobset-20`, `154` pods total, `390` CPU / `1,237Gi` aggregate demand) with ground-truth pod execution durations between `11s` and `21s`.
- **A/B Use Case**: Comparing `StrictFIFO` (`--strict-fifo`, default) vs. `BestEffortFIFO` (`--best-effort-fifo`) backfilling when multi-pod `JobSet` gang requests leave residual CPU/memory slack across the `10` nodes.

## 2. Isolated Execution Commands & Runtime Budget

```bash
# 1. Offline schema, timeline & manifest pre-check (<1s, no cluster required):
python3 deploy/scripts/testrunner.py testscenarios/demo --dry-run

# 2. Native 1x replay in an isolated kueue-optimizer chamber (~3 min active runtime):
python3 deploy/scripts/testrunner.py testscenarios/demo   --namespace kueue-optimizer-chamber-demo   --port 16450

# 3. Fast smoke-test replay at 2x speedup (~90s active runtime):
python3 deploy/scripts/testrunner.py testscenarios/demo   --namespace kueue-optimizer-chamber-demo-fast   --port 16451   --speedup 2
```

Always use a fresh isolated `--namespace` and `--port` per run (never reuse a chamber across A/B runs with `--skip-chamber-deploy`).

## 3. Expected Metrics & Pass Criteria (`Sections 5.1`–`5.4`)

- **Lifecycle Completion**:
  - `Workloads Status`: `admitted=0, pending=0, finished=20` (`20/20` `JobSet` workloads complete cleanly).
  - `KWOK Pods Status`: `succeeded=154, failed=0`.
- **`Section 5.1` (Quota Utilization)**:
  - `ai-lab-cq` sustains high `cpu` and `memory` quota utilization (`AVG ~55%–85%` under `StrictFIFO`, higher under `BestEffortFIFO` backfilling) and drops back to `0` once all `20` `JobSet`s finish.
- **`Section 5.2` (Wait Time by Priority)**:
  - `ADMISSIONS = 20` admitted and measured across the single priority tier.
- **`Section 5.3` (Evictions & Reclaims)**:
  - `Total Evictions = 0` (`withinClusterQueue: Never`, no cohort reclamation).
- **`Section 5.4` (Effective Uninterrupted Cluster Saturation)**:
  - `Qualifying Workloads: 20 (completed: 20, active-at-end: 0) | Excluded: 0 | Unadmitted: 0`.
  - `EFFECTIVE SAT` closely tracks `Section 5.1 AVG` (`cpu` is the primary bottleneck resource).

## 4. Diagnostic Red Flags

| Symptom | Likely Root Cause to Inspect |
| :--- | :--- |
| `KWOK Pods Status: running=0, succeeded=0` while `admitted > 0` | Missing KWOK fake-node toleration, webhook certificate mismatch, or `kwok-controller` `Stage` CRs not applied |
| `failed > 0` or `Excluded > 0` | Pod rejection or unexpected eviction; inspect `kubectl --kubeconfig=./kwok-<ns>.kubeconfig get pods,workloads -A` |
| `Total Evictions > 0` | Unexpected preemption policy override on `ai-lab-cq` |
| Effective saturation drops sharply | Excessive `--speedup` compressing `11s` pod runtimes down to controller reconciliation latency (`~1s`); keep `--speedup <= 3` on `demo` |

## 5. Regeneration & Maintenance

Whenever `infra.json`, `workflows.json`, or `execution-time.json` changes, regenerate `README.md` and verify offline:

```bash
python3 deploy/scripts/generate_scenario_readme.py testscenarios/demo
python3 deploy/scripts/testrunner.py testscenarios/demo --dry-run
```
