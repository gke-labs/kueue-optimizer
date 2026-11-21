---
name: generate-testscenario
description: >-
  Author, generate, and validate new kueue-optimizer Kubernetes + Kueue test scenarios
  (`infra.json`, `workflows.json`, `execution-time.json`, auto-generated `README.md`,
  and per-scenario `AGENTS.md`) from a user prompt, scheduling hypothesis, bug report,
  or workload specification. Use this skill whenever creating new synthetic micro-benchmarks,
  paired A/B scenarios, or realistic multi-queue workload traces under `testscenarios/`.
---

# Generating New Test Scenarios from User Prompts

This skill defines the end-to-end procedure for translating any natural-language user prompt, scheduling question, bug reproduction, or workload design into a complete, schema-compliant, self-documenting scenario directory under `testscenarios/`.

---

## 1. Directory Layout & Placement Convention

Unless the user explicitly requests a different target directory, place newly created scenarios under `testscenarios/<collection-or-slug>/` (**never** hardcode machine- or user-specific absolute paths in any file):

```text
testscenarios/<scenario-slug>/
├── generate_scenario.py   # Reproducible Python generator script (recommended)
├── infra.json             # Single-line NDJSON cluster, queue, & initial-pod events
├── workflows.json         # Single-line NDJSON chronological workload arrivals
├── execution-time.json    # Single-line NDJSON indexed workload execution durations
├── README.md              # Auto-generated via deploy/scripts/generate_scenario_readme.py (NEVER hand-edit)
└── AGENTS.md              # Mandatory guide: expected behavior/metrics + failure red flags
```

When creating a **controlled A/B pair** (recommended whenever testing a scheduler policy, queue knob, or preemption rule), generate two sibling directories (`<slug>-a-<control>` and `<slug>-b-<treatment>`) from the same generator script that differ **only** in the single independent variable under test, and cross-link both scenarios in each directory's `AGENTS.md`.

---

## 2. Translating the User Prompt into Exact Scenario Parameters

Before writing code, map the user's description onto concrete quantities so expected metrics can be derived analytically *before* running the chamber:

1. **Cluster Physical Topology (`infra.json` Nodes)**:
   - Choose node count $N$, per-node CPU, memory (binary IEC `Gi`, e.g., `64Gi`), max pods (`110`), accelerator resource key (e.g., `google.com/tpu` or `nvidia.com/gpu`) and per-node count, plus node labels matching `ResourceFlavor.spec.nodeLabels` (and `Topology` labels if testing Topology-Aware Scheduling).
   - Set **both** `metadata.annotations` (`kueue-optimizer.gke.io/node-cpu`, `kueue-optimizer.gke.io/node-memory`, `kueue-optimizer.gke.io/node-pods`, `kueue-optimizer.gke.io/node-accel-resource`, `kueue-optimizer.gke.io/node-accel-count`, `kwok.x-k8s.io/node: "fake"`) **and** matching `.status.capacity` / `.status.allocatable` so KWOK's `node-initialize` stage and `kueue_metrics_report.py` agree 100%.
2. **Prerequisite & Queue Hierarchy (`infra.json`)**:
   - Emit `Namespace`, `ResourceFlavor` (`kueue.x-k8s.io/v1beta2`), optional `Topology` (`kueue.x-k8s.io/v1beta2`), optional `Cohort` (`kueue.x-k8s.io/v1beta2`), `WorkloadPriorityClass`es (`kueue.x-k8s.io/v1beta2`), `ClusterQueue`s (`kueue.x-k8s.io/v1beta2`), and `LocalQueue`s (`kueue.x-k8s.io/v1beta2`).
   - **Critical `RuntimeClass` rule**: Whenever any pod template sets non-empty `spec.overhead`, also emit a cluster-scoped `RuntimeClass` with **`apiVersion: "node.k8s.io/v1"`** (`handler: "kwok"`, matching `overhead.podFixed`) in `infra.json` and set `spec.runtimeClassName` on the pod spec so Kubernetes' `RuntimeClass` admission plugin accepts pod creation.
3. **Initial `t0` Cluster Occupancy (Optional `infra.json` `Pod` Events & `t0`-Admitted Workflows)**:
   - **Pre-admitted Kueue workloads running at `t = 0`**: Emit `"workloadState": "Admitted"` workflow `"New"` events at `t0` in `workflows.json` (plus matching `"Admitted"` / `"FullExecution"` records in `execution-time.json` and `Pod` `"New"`/`"Deleted"` events in `infra.json` carrying `workloadName`, `ownerKind`, and `ownerName`). `deploy/scripts/testrunner.py` bootstraps these before `ClusterQueue` activation as pre-admitted Kueue `PodGroup` `Workload`s + pre-bound member `Pod`s so `ClusterQueue`/`Cohort` quota, TAS domain usage, and physical node occupancy stay 100% synchronized.
   - **Non-Kueue background/system load at `t = 0`**: Emit `Pod` `"New"` events at `t0` without `workloadName`/`ownerName` (with `"apiVersion": "v1"`, `"kind": "Pod"`, `placement` pinned to target node names, and explicit `resourceRequest`) paired with `Pod` `"Deleted"` events at `t0 + remaining_sec`. `deploy/scripts/testrunner.py` automatically places these into namespace `infra-pods` (`kueue.x-k8s.io/exclude=true`), and `deploy/scripts/kueue_metrics_report.py` accounts for them in `Section 5.4` under `Initial Infra Pods`.
4. **Strict Validation Rules Enforced by `testrunner.py`**:
   - Every event `timestamp` (and optional timestamp fields) must be valid RFC3339 UTC.
   - All `name` and `namespace` fields must be valid RFC 1123 DNS labels (`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`).
   - Only `(apiVersion, kind)` pairs in `SCRAPER-SCHEMA.md` are accepted (`v1/Node`, `v1/Namespace`, `v1/Pod`, `node.k8s.io/v1/RuntimeClass`, and `kueue.x-k8s.io/v1beta2` `ClusterQueue`/`LocalQueue`/`ResourceFlavor`/`Topology`/`Cohort`/`WorkloadPriorityClass` in `infra.json`; `batch/v1/Job`, `jobset.x-k8s.io/v1alpha2/JobSet`, `leaderworkerset.x-k8s.io/v1/LeaderWorkerSet`, `apps/v1/Deployment`, `ray.io/v1/RayJob`, `ray.io/v1/RayCluster`, and `v1/Pod` in `workflows.json`, with `payload.objectKind == payload.object.kind`).
5. **Workload Arrivals & Durations (`workflows.json` + `execution-time.json`)**:
   - Design arrival timestamps and durations so the scenario runs cleanly within **`~1–5 minutes` of active wall-clock time** by default (either `35s–90s` native at `--speedup 1` for focused synthetic tests, or scaled cleanly via `--speedup <S>` with compressed pod durations staying `>= 8s–10s`).
   - Every workload in `workflows.json` MUST have a matching entry in `execution-time.json` (keyed by `(namespace, objectKind, name)`) with `"eventType": "Admitted"` and `"eventType": "FullExecution"` (`"executionDurationSeconds": <dur>`) so `testrunner.py` indexes 100% of durations deterministically.

---

## 3. Exact Single-Line JSONL Event Schemas (Per `scraper/SCRAPER-SCHEMA.md`)

All three files MUST be **newline-delimited JSON (NDJSON)** — one compact JSON object per line (`json.dumps(ev, separators=(",", ":")) + "\n"`), **never** a pretty-printed multi-line JSON array:

- **`infra.json` line envelope**:
  ```json
  {"timestamp":"2026-06-01T00:00:00.000000000Z","eventType":"New","payload":{"apiVersion":"...","kind":"...","metadata":{...},"spec":{...}}}
  ```
- **`workflows.json` line envelope** (sorted strictly non-decreasing by `timestamp`):
  ```json
  {"timestamp":"2026-06-01T00:00:05.000000000Z","eventType":"New","workloadState":"NotAdmitted","payload":{"objectKind":"JobSet","name":"js-train-0","namespace":"workloads","creationTime":"2026-06-01T00:00:05Z","localQueueName":"lq-main","object":{"apiVersion":"jobset.x-k8s.io/v1alpha2","kind":"JobSet","metadata":{"name":"js-train-0","namespace":"workloads","labels":{"kueue.x-k8s.io/queue-name":"lq-main","kueue.x-k8s.io/priority-class":"p-high"}},"spec":{"suspend":true,"replicatedJobs":[...]}}}}
  ```
  - Always include KWOK fake-node toleration (`key: "kwok.x-k8s.io/node", operator: "Equal", value: "fake", effect: "NoSchedule"`), `restartPolicy: "Never"`, and `image: "registry.k8s.io/pause:3.10"` on every container/initContainer.
- **`execution-time.json` line pair per workload**:
  ```json
  {"timestamp":"2026-06-01T00:00:05.000000000Z","eventType":"Admitted","workloadState":"Admitted","payload":{"workloadName":"jobset-js-train-0","namespace":"workloads","ownerKind":"JobSet","ownerName":"js-train-0","ownerAPIVersion":"jobset.x-k8s.io/v1alpha2","localQueueName":"lq-main","clusterQueueName":"cq-main","creationTime":"2026-06-01T00:00:05Z","admittedTime":"2026-06-01T00:00:05Z","preempted":false,"reason":"Admitted","message":"Admitted"}}
  {"timestamp":"2026-06-01T00:00:35.000000000Z","eventType":"FullExecution","workloadState":"Admitted","payload":{"executionType":"Full","workloadName":"jobset-js-train-0","namespace":"workloads","ownerKind":"JobSet","ownerName":"js-train-0","ownerAPIVersion":"jobset.x-k8s.io/v1alpha2","localQueueName":"lq-main","clusterQueueName":"cq-main","creationTime":"2026-06-01T00:00:05Z","admittedTime":"2026-06-01T00:00:05Z","endTime":"2026-06-01T00:00:35Z","executionDurationSeconds":30.0,"executionDuration":"30s","preempted":false,"reason":"Succeeded","message":"Completed"}}
  ```

---

## 4. Mandatory `README.md` Generation & `AGENTS.md` Specification

Every scenario directory under `testscenarios/<scenario-slug>/` MUST include **both** `README.md` and `AGENTS.md`:

### 4.1 Generate `README.md` Automatically (Never Hand-Write)
Immediately after writing `infra.json`, `workflows.json`, and `execution-time.json`, run:
```bash
python3 deploy/scripts/generate_scenario_readme.py testscenarios/<scenario-slug>
```

### 4.2 Author a Comprehensive `AGENTS.md` Inside the Scenario Directory
Write `testscenarios/<scenario-slug>/AGENTS.md` using strictly portable repository-relative paths covering:

1. **Scenario Purpose & Hypothesis**.
2. **Exact Isolated Execution Command & Recommended Runtime Budget**:
   ```bash
   # Offline schema & timeline pre-check (<1s, no cluster required):
   python3 deploy/scripts/testrunner.py testscenarios/<scenario-slug> --dry-run

   # Live execution in a dedicated isolated chamber:
   python3 deploy/scripts/testrunner.py testscenarios/<scenario-slug> \
     --namespace kueue-optimizer-chamber-<short-id> \
     --port 16450
   ```
3. **What Is Expected (Quantitative Pass Criteria Across Sections `5.1`–`5.4`)**.
4. **What Indicates Something Is Going Wrong (Diagnostic Red-Flag Table)**.
5. **Regeneration & Maintenance Commands**.

---

## 5. Mandatory Pre-Completion Verification Checklist

Before finishing any scenario-generation task, ALWAYS execute all three verification steps and report the results:

```bash
# 1. Regenerate and schema-verify README.md:
python3 deploy/scripts/generate_scenario_readme.py testscenarios/<scenario-slug>

# 2. Verify full bundle parsing, execution-time indexing (100% hit rate), and resource totals offline:
python3 deploy/scripts/testrunner.py testscenarios/<scenario-slug> --dry-run

# 3. Verify zero hardcoded machine- or user-specific absolute paths in tracked/new files:
git grep -nE "/(usr/local|home)/" -- testscenarios/ skills/ AGENTS.md || true
```
