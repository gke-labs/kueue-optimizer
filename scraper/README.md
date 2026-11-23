# Kubernetes & Kueue Cluster Scraper

`cluster-scraper` connects to a Kubernetes + [Kueue](https://kueue.sigs.k8s.io/) cluster and records infrastructure state, submitted workflows, and workload execution timings into streaming JSON Lines (NDJSON) event logs. All captured workflow specs are automatically sanitized of business-specific details (container images, commands, arguments, environment variables, volumes, secrets, and non-Kubernetes annotations) so traces can be safely shared and replayed in deterministic testing environments such as KWOK.

> **Note:** Throughout this documentation and codebase, **workflow** refers to any job-like Kubernetes object managed by Kueue (e.g., `Job`, `JobSet`, `LeaderWorkerSet`, `Deployment`, `RayJob`, `RayCluster`, or standalone Kueue-managed `Pod`). The name "workflow" is chosen deliberately to differentiate these user-submitted objects from Kueue's own `Workload` API resource (`kueue.x-k8s.io`).

For the complete output format, event envelope, `workloadState` state machine, and payload specifications, see [`SCRAPER-SCHEMA.md`](SCRAPER-SCHEMA.md).

---

## Features

- **Hybrid Periodic + Watch Recording**:
  - Periodically snapshots sanitized `Node`s (scheduling-relevant spec & status), `Namespace`s, `RuntimeClass`es, `PriorityClass`es (`scheduling.k8s.io/v1`), Kueue controller `Configuration` (`ConfigMap/kueue-manager-config` plus `Deployment/kueue-controller-manager` `--feature-gates` CLI overrides and custom `--config` ConfigMap discovery in `--kueue-namespace`), and Kueue `ClusterQueue`s, `Cohort`s, `LocalQueue`s, `ResourceFlavor`s, `Topology` objects, and `WorkloadPriorityClass`es, emitting events only when canonical object state changes (ignoring volatile `metadata.resourceVersion`, `metadata.generation`, and `status.conditions[].lastHeartbeatTime`).
  - Watches `Node`s, `Pod`s, Kueue-managed `Job`s, `JobSet`s, `LeaderWorkerSet`s, `Deployment`s, `RayJob`s, `RayCluster`s, Kueue `ClusterQueue`s/`Cohort`s/`LocalQueue`s/`ResourceFlavor`s/`Topology`/`WorkloadPriorityClass`es, and Kueue `Workload`s in real time (preserving `ResourceVersion`s across watch reconnects) to capture exact mid-run node/queue/quota changes, workload creation, admission, scheduling, interruptions (`PartialExecution` across `Preempted`, `NodeFailure`, `PodsReadyTimeout`, `Evicted`, `Failed`, `Deleted`, and shutdown `ScrapeEnded`), `unhealthyNodes` TAS hot-swaps, completion (`FullExecution`), and deletion timestamps.
  - Gracefully handles optional CRDs (`JobSet`, `LeaderWorkerSet`, `RayJob`, `RayCluster`, Kueue APIs) if they are not installed on the target cluster, retrying periodically without failing.
- **Strict Data Sanitization** (see [`SCRAPER-SCHEMA.md`](SCRAPER-SCHEMA.md) for the full field-by-field specification):
  - **Nodes (`infra.json`)**: Retains only scheduling-relevant spec (`taints`, `unschedulable`) and status (`capacity`, `allocatable`, `conditions`, `phase`), stripping node network/system metadata (`nodeInfo`, `daemonEndpoints`, `addresses`), cached images, attached volumes, cloud/cluster-identifying node labels, and disallowed annotations.
  - **Queues, Cohorts, RuntimeClasses & Topology (`infra.json`)**: `Namespace`, `RuntimeClass`, `ClusterQueue`, `Cohort`, `LocalQueue`, `ResourceFlavor`, `Topology`, and `WorkloadPriorityClass` records omit `.status` (and free-form `WorkloadPriorityClass.description`), `metadata.managedFields`, and disallowed/sensitive annotations.
  - **Scheduled Pods (`infra.json`)**: Stores **only** node placement, effective resource requests, scheduling time, first notice time, deletion time, and optional sanitized Kueue workload/owner linkage (`workloadName`, `ownerKind`, `ownerName`, `podSetName`, `isTAS`).
  - **Workflow Specs (`workflows.json`)**:
    - Replaces container and Ray sidecar images with a pause image (`registry.k8s.io/pause:3.10` by default) and clears commands, arguments, and environment variables (`env` / `envFrom`).
    - Strips probes, lifecycle hooks, volumes, secrets, credentials, pod/service/network identity fields, `.status`, and `metadata.managedFields`.
    - Retains only official Kubernetes/Kueue domain annotations (`kubernetes.io`, `k8s.io`, `x-k8s.io`, `kueue-optimizer.gke.io`, and their subdomains) that are not on the sensitive annotation denylist.
- **Optional Name & Label Anonymization (`--hash-names`)**:
  - Disabled by default (names and label values remain unchanged).
  - When enabled, uses a per-run 256-bit `HMAC-SHA256` key to deterministically hash:
    - **Resource names & references**: Workload, Pod, Job, JobSet, Deployment, LeaderWorkerSet, and Ray object names, along with `ownerReferences` and name-holding annotations.
    - **Label values (keys are never hashed)**: Values of built-in organizational/identity labels (e.g. `owner`, `team`, `customer`, `project`, `cost-center`), workload-referencing labels, and any additional keys or `<domain>/*` patterns passed via `--extra-hash-labels` across metadata, selectors, and affinities.
  - Infrastructure object names (`Node`, `Namespace`, `RuntimeClass`, `ClusterQueue`, `Cohort`, `LocalQueue`, `ResourceFlavor`, `Topology`, `WorkloadPriorityClass`) remain in plain text so cluster topology and queue hierarchy stay inspectable.
- **Streaming NDJSON Output & Unified `t0` Startup Barrier**:
  - Executes all initial `List()` barriers (`Workload`s, `JobSet`s, `Job`s, `Deployment`s, dynamic CRDs, `Pod`s, `Node`s, and Kueue queues/topologies) against a single unified `t0` timestamp (skipping pre-`t0` already-terminal historical tombstones while capturing bound non-terminal pods in both `Running` and `Pending`/`ContainerCreating` phases, and emitting `Deleted` events when pods reach `Succeeded`/`Failed` or are deleted), flushing via `fsync` at each barrier/reconciliation tick and on shutdown.

---

## Prerequisites

- **Go**: Go `1.27.3` or newer (as specified in [`.go-version`](.go-version) and [`go.mod`](go.mod)). If you have an earlier Go version with toolchain management enabled (`GOTOOLCHAIN=auto`), Go will download the required toolchain automatically.
- **Kubernetes Cluster Access**: A valid kubeconfig file (`--kubeconfig`, `$KUBECONFIG`, or `~/.kube/config`) or an in-cluster ServiceAccount when running inside a Pod.
- **Read-Only RBAC Permissions**: The scraper never mutates cluster state; it only requires `list` and `watch` permissions across the scraped resource types (see [RBAC Permissions](#rbac-permissions) below).

---

## Building

From the repository root:

```bash
cd scraper
go build -o cluster-scraper .
```

To cross-compile a static binary for a Linux amd64 host or container:

```bash
cd scraper
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o cluster-scraper .
```

### Running Tests

```bash
cd scraper
go test -v ./...
```

---

## Usage

At startup, `cluster-scraper` prints a configuration banner summarizing the scrape interval, target output files, scraped resource kinds, sanitization rules, and SHA-256 hashing status, runs an immediate initial scrape, and continues recording until `--duration` elapses or it receives `SIGINT` (`Ctrl+C`) / `SIGTERM`.

> **Note:** Output files (`infra.json`, `workflows.json`, `execution-time.json`) are created or **truncated** at startup. Point `--output-dir` to a dedicated directory per capture session to avoid overwriting previous traces.

### Basic Run (Default Kubeconfig, Run Until `Ctrl+C`)

```bash
./cluster-scraper --output-dir ./traces/run-01
```

### Bounded Scrape with Custom Interval and Anonymized Names

Scrape every `30s` for `1h` using a specific kubeconfig and anonymize all workload/pod names with SHA-256:

```bash
./cluster-scraper \
  --kubeconfig ~/.kube/config \
  --interval 30s \
  --duration 1h \
  --hash-names \
  --output-dir ./traces/prod-capture
```

### Running Directly via `go run`

```bash
cd scraper
go run . -n 15s --duration 10m --output-dir /tmp/kueue-trace
```

### CLI Flags Reference

| Flag | Shorthand / Alias | Default | Description |
| :--- | :--- | :--- | :--- |
| `--kubeconfig` | — | `""` | Path to kubeconfig file. Falls back to `$KUBECONFIG`, `~/.kube/config`, and finally in-cluster config. |
| `--interval` | `-n` | `60s` | Periodic infrastructure scrape interval. Accepts Go duration strings (e.g. `30s`, `2m`) or plain positive seconds (e.g. `60`). |
| `--duration` | — | `0` | Total run duration (e.g. `5m`, `2h`). When `0`, runs until interrupted (`SIGINT` / `SIGTERM`). |
| `--output-dir` | — | `.` | Directory where output JSON Lines files are written (created automatically if it does not exist). |
| `--infra-file` | — | `infra.json` | Filename (relative to `--output-dir`) or absolute path for infrastructure and scheduled/running pod events. |
| `--workflows-file` | — | `workflows.json` | Filename (relative to `--output-dir`) or absolute path for sanitized workflow creation, update, and deletion events. |
| `--execution-time-file` | — | `execution-time.json` | Filename (relative to `--output-dir`) or absolute path for Kueue `Workload` admission and partial/full execution events. |
| `--hash-names` | `--sha256-names` | `false` | Replace names of `Workload`s, `Pod`s, `Job`s, `JobSet`s, `LeaderWorkerSet`s, `Deployment`s, `RayJob`s, and `RayCluster`s, and also enable hashing of sensitive, workload-referencing, and `--extra-hash-labels` label values across scraped objects and selectors using per-run HMAC-SHA256 digests. |
| `--pause-image` | — | `registry.k8s.io/pause:3.10` | Pause container image substituted into all sanitized workflow container specs. |
| `--kueue-namespace` | — | `kueue-system` | Namespace containing the Kueue controller `ConfigMap` (`""` disables `Configuration` scraping). |
| `--kueue-configmap` | — | `kueue-manager-config` | Name of the Kueue controller `ConfigMap` holding `controller_manager_config.yaml` (`""` disables `Configuration` scraping). |
| `--extra-hash-labels` | — | `""` | Comma-separated additional list of label keys or `<domain>/*` patterns (matched case-insensitively; flag may also be repeated) to deterministically hash across scraped objects and selectors in addition to the built-in sensitive label list (requires `--hash-names`). |
| `--qps` | — | `200` | Kubernetes API client QPS rate limit. |
| `--burst` | — | `400` | Kubernetes API client burst limit. |

---

## Output Files

Each run produces three append-only **JSON Lines (NDJSON)** files (one compact JSON event per line):

| File | Recorded Resources & Events |
| :--- | :--- |
| `infra.json` | `"New"`, `"Update"`, and `"Deleted"` events for sanitized `Node`s (scheduling-relevant `.spec` + `.status`), `Namespace`s, `RuntimeClass`es, `ClusterQueue`s, `Cohort`s, `LocalQueue`s, `ResourceFlavor`s, `Topology` objects, `WorkloadPriorityClass`es (without `.status`, with annotations filtered), and scheduled/running `Pod` placements and resource requests. |
| `workflows.json` | Watch-driven `"New"`, `"Update"`, and `"Deleted"` events for `Job`, `JobSet`, `LeaderWorkerSet`, Kueue-managed `Deployment`, `RayJob`, `RayCluster`, and standalone Kueue-managed `Pod` specs, annotated with top-level `workloadState` (`"NotAdmitted"`, `"AdmissionInProgress"`, `"Admitted"`). |
| `execution-time.json` | Watch-driven `"Admitted"`, `"PartialExecution"` (preempted), and `"FullExecution"` (completed without preemption) timing records for Kueue `Workload` objects. |

### Inspecting Traces with `jq`

Count events by type in each log:

```bash
jq -r '.eventType' traces/run-01/infra.json | sort | uniq -c
jq -r '[.payload.objectKind, .eventType, .workloadState] | @tsv' traces/run-01/workflows.json | sort | uniq -c
jq -r '.eventType' traces/run-01/execution-time.json | sort | uniq -c
```

Filter full execution durations from `execution-time.json`:

```bash
jq -c 'select(.eventType == "FullExecution") | {workload: .payload.workloadName, queue: .payload.localQueueName, duration: .payload.executionDuration}' \
  traces/run-01/execution-time.json
```

See [`SCRAPER-SCHEMA.md`](SCRAPER-SCHEMA.md) for detailed field-by-field documentation and streaming reader examples in Go and Python.

---

## RBAC Permissions

When running `cluster-scraper` with a dedicated Kubernetes `ServiceAccount` (either via kubeconfig or in-cluster), bind it to a `ClusterRole` with read-only `list` and `watch` access:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: kueue-cluster-scraper
rules:
  - apiGroups: [""]
    resources: ["nodes", "namespaces", "pods"]
    verbs: ["get", "list", "watch"]
  - apiGroups: [""]
    resources: ["configmaps"]
    resourceNames: ["kueue-manager-config"]
    verbs: ["get"]
  - apiGroups: ["node.k8s.io"]
    resources: ["runtimeclasses"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["apps"]
    resources: ["deployments", "replicasets"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["batch"]
    resources: ["jobs"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["kueue.x-k8s.io"]
    resources:
      - "clusterqueues"
      - "cohorts"
      - "localqueues"
      - "resourceflavors"
      - "topologies"
      - "workloadpriorityclasses"
      - "workloads"
    verbs: ["get", "list", "watch"]
  - apiGroups: ["jobset.x-k8s.io"]
    resources: ["jobsets"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["leaderworkerset.x-k8s.io"]
    resources: ["leaderworkersets"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["ray.io"]
    resources: ["rayjobs", "rayclusters"]
    verbs: ["get", "list", "watch"]
```
