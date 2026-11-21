---
name: chamber-evaluation
description: >-
  Design, execute, and analyze isolated kueue-optimizer evaluation chambers
  using `deploy/scripts/testrunner.py` and `deploy/scripts/kueue_metrics_report.py`.
  Use this skill whenever running scenario replays (`testscenarios/*`), formulating
  falsifiable scheduling hypotheses, conducting controlled A/B or multi-arm experiments,
  calibrating time-compression (`--speedup`) and wall-clock budgets (`--duration`),
  or interpreting Sections 5.1–5.4 of the aggregated Kueue and physical saturation report.
---

# `kueue-optimizer` Evaluation Chamber & `testrunner.py` Methodology

This skill defines the standard engineering workflow for validating Kueue, Kubernetes scheduler, and workload behavior inside isolated **`kueue-optimizer` evaluation chambers** using `deploy/scripts/testrunner.py`, `deploy/scripts/chamber_common.py`, and `deploy/scripts/kueue_metrics_report.py`.

---

## 1. What a `kueue-optimizer` Chamber Is & How It Works

Each **`kueue-optimizer` chamber** (never called a `"KWOK chamber"`; use `KWOK` strictly for individual components such as `kwok-kube-apiserver`, `kwok-etcd`, `kwok-controller`, `kwok-registry`, KWOK `Stage` CRs, or simulated KWOK `Node`s) is a self-contained, ephemeral Kubernetes + Kueue control plane deployed into its own host-cluster namespace (`--namespace kueue-optimizer-chamber-<id>`):

- **Control-Plane Components**: Dedicated per-chamber in-memory container image registry (`kwok-registry`, `emptyDir: { medium: Memory, sizeLimit: 4Gi }` on `NodePort 30500 + port - 16443`), `kwok-etcd`, `kwok-kube-apiserver` (`v1.36.4`), `kwok-kube-controller-manager`, `kwok-kube-scheduler`, `kwok-controller` (simulating node heartbeats and timed pod execution via `Stage` CRs), `jobset-controller-manager`, `kueue-controller-manager` (Preemption-Based Fair Sharing enabled by default via `fairSharing.preemptionStrategies: [LessThanOrEqualToFinalShare, LessThanInitialShare]`, toggleable at deploy time via `--fair-sharing` / `--no-fair-sharing` [incompatible with `--skip-chamber-deploy`], plus `enableClusterQueueResources: true`), and `prometheus` scraping Kueue `/metrics` every `10s` by default. Every chamber namespace carries the ownership label `kueue-optimizer.gke.io/chamber: "true"`, which `deploy_chamber`, `stage_registry_only`, `cleanup_chamber`, and `make undeploy` verify strictly before applying to or deleting an existing namespace.
- **Deterministic Trace & Scenario Inputs** (per `scraper/SCRAPER-SCHEMA.md` and `TESTRUNNER.md`):
  1. `infra.json` — Nodes (capacity, allocatable, topology/accelerator labels), `Namespace`s, `RuntimeClass`es (`node.k8s.io/v1`), `Topology` CRs, `ResourceFlavor`s, `Cohort`s, `WorkloadPriorityClass`es, `ClusterQueue`s, `LocalQueue`s, and pre-existing `t0` running `Pod`s.
  2. `workflows.json` — Chronological workload arrivals (`JobSet`, `Job`, `Deployment`, `LeaderWorkerSet`, `RayJob`, `Pod`) with initial `NotAdmitted` or `Admitted` state.
  3. `execution-time.json` — Indexed per-workload ground-truth execution durations (`FullExecution` / `PartialExecution`) injected as plain numeric seconds in `kueue-optimizer.gke.io/pod-execution-time-seconds` annotations on every pod template before admission.
- **End-to-End Execution Pipeline**:
  1. Indexes `execution-time.json` and parses `infra.json` + `workflows.json` into a `ScenarioBundle` (printing the pre-flight infrastructure, queue, and workload outline).
  2. Deploys the isolated chamber in `--namespace` with auto-generated TLS PKI and opens local port-forwards (`--port <P>` for `kwok-kube-apiserver`, `19090 + (P - 16443)` by default for `prometheus` unless overridden via `--prometheus-port`, and `30500 + (P - 16443)` by default for the per-chamber in-memory image registry `svc/kwok-registry` unless overridden via `--registry-port`).
  3. If `--kueue-build-dir <dir>` or a local Docker image `--kueue-image <tag>` is provided, stages the per-chamber in-memory `kwok-registry` first, pushes the ad-hoc Kueue image into `localhost:<registry-port>/kueue:<tag>` (verifying the manifest `sha256` digest in RAM), and deploys `kueue-controller-manager` pulling directly over each worker node's loopback `NodePort`.
  4. Applies KWOK `Node`s and waits for all nodes to reach `Ready`.
  5. Applies base prerequisite CRs (`Namespace`, `RuntimeClass`, `Topology`, `ResourceFlavor`, `Cohort`, `WorkloadPriorityClass`), bootstraps any `t0`-admitted Kueue workloads as pre-admitted `PodGroup` `Workload`s + pre-bound member `Pod`s in their workload namespaces alongside non-Kueue `t0` pods in `--infra-pods-namespace infra-pods` (`kueue.x-k8s.io/exclude=true`), waits for all initial pods to reach `Running`, and then applies `ClusterQueue`s and `LocalQueue`s so Kueue's `Cache.AddClusterQueue` loads initial quota, Cohort FairSharing, and TAS usage atomically before replay begins.
  6. Replays `workflows.json` arrivals at `(raw_offset_sec / speedup)` (dynamically re-submitting any `t0` `PodGroup` parent workflow if Kueue preempts/evicts it during replay), monitors completion (or until `--duration` elapses), executes `deploy/scripts/kueue_metrics_report.py` (passing `--kubeconfig ./kwok-<namespace>.kubeconfig`) to produce **Sections `5.1`–`5.4`**, and tears down the chamber cleanly inside a `try/finally` block (wiping all `tmpfs` registry pages in RAM on completion, and on error only when the current run created the namespace, unless `--no-cleanup` / `--keep` is set or `--skip-chamber-deploy` is used without `--cleanup-after`).

### 1.1 Per-Chamber In-Memory Container Registry (`kwok-registry`)

Every chamber namespace (`kueue-optimizer-chamber-<id>`) runs its own isolated OCI Distribution v2 registry (`deploy/base/registry.yaml`) backed strictly by kernel RAM (`emptyDir: { medium: Memory, sizeLimit: 4Gi }`, configurable via `--registry-memory-limit`) and exposed via `Service/kwok-registry` (`type: NodePort`, default `30500 + (port - 16443)` inside `30000..32767`).

**Mandatory rule:** All ad-hoc, patched, experimental, or custom-version Kueue controller images (and any other ad-hoc chamber container images) MUST be pushed into and served from the target chamber's in-memory registry (`kwok-registry`). Never push ad-hoc Kueue builds to shared external remote registries, never manually import images onto worker nodes, and never hardcode ad-hoc tags or `localhost:<port>/...` digests into `deploy/base/kueue.yaml`.

- **Single-command evaluation of local or source-built Kueue images**:
  ```bash
  # Evaluate a locally built Docker image inside an isolated chamber:
  python3 deploy/scripts/testrunner.py testscenarios/<scenario> \
    --namespace kueue-optimizer-chamber-exp1 --port 16451 \
    --kueue-image my-local-kueue:dev

  # Build Kueue from a source checkout, push to the chamber's RAM registry, and run:
  python3 deploy/scripts/testrunner.py testscenarios/<scenario> \
    --namespace kueue-optimizer-chamber-exp2 --port 16452 \
    --kueue-build-dir ./kueue
  ```
- **Standalone per-chamber registry management (`deploy/scripts/chamber_registry.py`)**:
  - `python3 deploy/scripts/chamber_registry.py status --namespace <ns> [--port <P>]` — inspect registry readiness, `NodePort`, live `tmpfs` RAM usage, and stored image tags/digests.
  - `python3 deploy/scripts/chamber_registry.py push <local-image> --namespace <ns> [--port <P>] [--tag <tag>] [--set-kueue]` — tag & push into the chamber's in-memory registry and optionally hot-swap `deployment/kueue-controller-manager`.
  - `python3 deploy/scripts/chamber_registry.py build-kueue <kueue-dir> --namespace <ns> [--port <P>] [--tag <tag>] [--set-kueue]` — build Kueue from source, push into the chamber's RAM registry, and optionally roll out `deployment/kueue-controller-manager`.
  - `python3 deploy/scripts/chamber_registry.py delete <repo:tag> --namespace <ns>` / `gc` — delete manifests and run `registry garbage-collect` to free `tmpfs` pages immediately.

---

## 2. What Can Be Checked in an Evaluation Chamber

Use the matrix below to choose or construct the right scenario(s) for any scheduling, quota, topology, or accounting question:

| Evaluation Dimension | Key Configuration / Mechanism | Primary Ground-Truth Signals |
| :--- | :--- | :--- |
| **Queueing Strategy & Backfilling** | `ClusterQueue.spec.queueingStrategy` (`StrictFIFO` vs `BestEffortFIFO`), CLI override `--strict-fifo` / `--best-effort-fifo` | `Sec 5.1` quota utilization jump when small jobs bypass HoL blocker; `Sec 5.2` tail vs median wait shift; `Sec 5.4` makespan & uninterrupted saturation |
| **Multi-Priority Queue Ordering** | `WorkloadPriorityClass` (`value`), non-preemptive priority ordering (`withinClusterQueue: Never`) | `Sec 5.2` strict priority monotonicity (`AVG(p-urgent) < AVG(p-high) < AVG(p-low)`); `Sec 5.3` `Total Evictions == 0` |
| **Intra-CQ Preemption & Timestamp Protection** | `preemption.withinClusterQueue`: `Never` vs `LowerPriority` vs `LowerOrNewerEqualPriority` (`V.CreationTimestamp > P.CreationTimestamp`) | `Sec 5.3` `InClusterQueue` preemption count & `Unique workloads evicted once` (zero ping-pong loops); `Sec 5.1 - Sec 5.4` preemption badput gap |
| **Cohort Borrowing, Lending Limits & Reclamation** | `cohortName`, `borrowingLimit`, `lendingLimit`, `reclaimWithinCohort` (`Any` vs `Never`), `borrowWithinCohort` priority threshold | `Sec 5.1` per-CQ usage (`> 100%` of nominal quota when borrowing, capped at `(nominal + borrowingLimit)/nominal`); `Sec 5.3` `InCohortReclamation` vs `0` |
| **Cohort Fair Sharing & Weight Convergence** | Default `--fair-sharing` (`--no-fair-sharing` for Classical Preemption), `Cohort` nominal quota + `ClusterQueue.spec.fairSharing.weight`, optional `--cq-fair-sharing-preemption` override on legacy scraped `ClusterQueue`s | `Sec 5.1` `ClusterQueue: <name>` `<pct>% (cohort)` and `<used> / 0.00 (borrowed)` ratios tracking configured `weight` ratios; `Sec 5.3` `InCohortFairSharing` |
| **Topology-Aware Scheduling (TAS)** | `Topology` hierarchy levels + `ResourceFlavor.spec.topologyName` + workload topology annotations (`required` / `preferred`) | Fragmentation vs contiguous domain placement; `Sec 5.1` / `Sec 5.4` accelerator saturation & admission wait times |
| **Multi-Resource Bottleneck Inversion** | Independent `cpu`, `memory` (binary IEC `Gi`), `pods`, and accelerator (`google.com/tpu`, `nvidia.com/gpu`) quotas & node capacities | `Sec 5.1` & `Sec 5.4` per-resource rows showing secondary resource bottleneck while accelerators remain underutilized |
| **Pod Resource Formula Edge Cases** | Kubernetes `max(max(initContainers), sum(containers)) + overhead` with `node.k8s.io/v1` `RuntimeClass` & `runtimeClassName` | Exact ratio agreement between `Sec 5.1` (Kueue admission accounting) and `Sec 5.4` (physical node saturation accounting) across all resource types |
| **Heterogeneous Multi-Role Workloads & Admission Guards** | Multi-`replicatedJobs` `JobSet` (e.g., CPU-only coordinator + accelerator workers); permanently `Inadmissible` oversized requests | `Sec 5.4` per-resource accounting across heterogeneous roles; `Unadmitted: <N> [<M> pods]` vs `Excluded: 0` when oversized jobs do not starve valid jobs |
| **Initial `t0` Cluster State & Scraped Trace Replay** | Pre-existing `t0` pods in `infra-pods` (`kueue.x-k8s.io/exclude=true`); multi-queue traces scraped via `scraper/` | `Initial Pods Placed: K/K`; `Sec 5.4` `Initial Infra Pods: K` + full physical cluster saturation; end-to-end throughput & wait distributions |

---

## 3. Recommended Duration Budget (`<= 5 min` Default & When to Adjust)

### Default Target: `~1–5 minutes` Active Execution Wall Clock
Aim for **`1–5 minutes` (`60s–300s`) of active scenario execution** per chamber (plus `~45s–90s` for control-plane rollout and teardown) unless a clear technical reason justifies a shorter or longer window:

1. **Always inspect the scenario timeline first (zero-cost offline check)**:
   ```bash
   python3 deploy/scripts/testrunner.py testscenarios/demo --dry-run
   ```
   Inspect `Total Scraped Span`, `Active Execution Span`, `Execution Durations` (`min`, `p50`, `avg`, `p90`, `max`), and total workflows/pods.
2. **How to choose `--speedup` and `--duration`**:
   - **Short synthetic scenarios (`30s–180s` raw span)**: Run at native **`--speedup 1`** (or modest `2x–5x` speedup) so compressed pod runtimes remain well above Kubernetes reconciliation latency (`~0.5s–1.2s`).
   - **Medium & large traces (`10 min` to multi-hour raw span)**: Pick `--speedup <S>` and/or `--duration 300s` so the active test window is `~3–5 min` (`180s–300s`), capturing `18–30` Prometheus scrape intervals while keeping typical compressed pod runtimes `>= 5s–8s`.
   - **Scenarios with intentionally non-completing / `Inadmissible` / infinite backlog workloads**: Always pass an explicit `--duration <sec>` so `testrunner.py` terminates deterministically.

---

## 4. Scientific Validation Workflow: Hypotheses & Controlled A/B Testing

Follow this 5-step protocol for every investigation, bug fix, scheduler tuning experiment, or feature validation:

### Step 1: Formulate Explicit, Falsifiable Hypotheses *Before* Launching
1. **Primary Hypothesis**: Exact expected directional and quantitative shift across `Sections 5.1–5.4`.
2. **Conservation & Sanity Invariants**:
   - `Workloads Status`: `admitted + pending + finished == total_submitted_top_level_workloads`.
   - `KWOK Pods Status`: `failed == 0` (unless testing explicit pod-failure scenarios).
   - `Sec 5.4`: `Qualifying Workloads + Excluded + Unadmitted == total_top_level_kueue_workloads`.

### Step 2: Enforce Strict Chamber Isolation per Run / Arm (Mandatory)
- **Never** run two scenarios or A/B arms back-to-back in the same chamber via `--skip-chamber-deploy`.
- Always give every scenario / arm its own **unique host namespace and unique local port**:
  ```bash
  # Control Arm A (isolated chamber + port 16401 -> Prometheus port 19048)
  python3 deploy/scripts/testrunner.py testscenarios/<arm-a> \
    --namespace kueue-optimizer-chamber-arm-a \
    --port 16401 \
    --speedup 10 \
    --duration 300s

  # Treatment Arm B (isolated chamber + port 16402 -> Prometheus port 19049)
  python3 deploy/scripts/testrunner.py testscenarios/<arm-b> \
    --namespace kueue-optimizer-chamber-arm-b \
    --port 16402 \
    --speedup 10 \
    --duration 300s
  ```

### Step 3: Change Exactly One Independent Variable per A/B Pair
Hold `infra.json`, `workflows.json`, `execution-time.json`, `--speedup`, `--duration`, and `--scrape-interval` constant across `Arm A` and `Arm B`, varying only the single policy or Kueue image under test.

### Step 4: Execute Chambers in Parallel & Guarantee Cleanup
Run up to `3–5` isolated chambers concurrently on distinct `--namespace` and `--port` values, and wrap runs so chamber namespaces are always cleaned up (`kubectl delete ns <ns> --ignore-not-found`).

### Step 5: Cross-Examine Sections `5.1`–`5.4`
1. **`Section 5.1` — ClusterQueue & Cohort Quota Utilization**: Inspect aggregate and per-queue/cohort `P50 / P90 / P99 / AVG` and `AVG USED / QUOTA`.
2. **`Section 5.2` — Wait Time Across All ClusterQueues per Workload Priority**: Compare `P50`, `P90`, `P99`, `AVG`, and `WORKLOADS` across priority classes.
3. **`Section 5.3` — Total Evictions and Reclaims**: Cross-check `Total Evictions` vs `Unique workloads evicted once` and separate `InClusterQueue`, `InCohortReclamation`, and `FairSharing` reasons.
4. **`Section 5.4` — Effective Uninterrupted Physical Cluster Saturation**: Compare `Sec 5.4 EFFECTIVE SAT` against `Sec 5.1 AVG`; in heavy preemption runs, `Sec 5.1 - Sec 5.4` measures wasted compute (badput) from aborted executions.
5. **Post-Run Hygiene**: Confirm `kubectl get ns | grep kueue-optimizer` is empty and run `make test` when modifying scripts.
