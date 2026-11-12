# Kubernetes & Kueue Cluster Scraper

`cluster-scraper` connects to a Kubernetes + [Kueue](https://kueue.sigs.k8s.io/) cluster and records infrastructure state, submitted workflows, and workload execution timings into streaming JSON Lines (NDJSON) event logs. All captured workflow specs are automatically sanitized of business-specific details (container images, commands, arguments, environment variables, volumes, secrets, and non-Kubernetes annotations) so traces can be safely shared and replayed in deterministic testing environments such as KWOK.

> **Note:** Throughout this documentation and codebase, **workflow** refers to any job-like Kubernetes object managed by Kueue (e.g., `Job`, `JobSet`, `LeaderWorkerSet`, `Deployment`, `RayJob`, `RayCluster`, or standalone Kueue-managed `Pod`). The name "workflow" is chosen deliberately to differentiate these user-submitted objects from Kueue's own `Workload` API resource (`kueue.x-k8s.io`).

For the complete output format, event envelope, `workloadState` state machine, and payload specifications, see [`SCRAPER-SCHEMA.md`](SCRAPER-SCHEMA.md).

---

## Features

- **Hybrid Periodic + Watch Recording**:
  - Periodically snapshots sanitized `Node`s (scheduling-relevant spec & status), `Namespace`s, and Kueue `ClusterQueue`s, `LocalQueue`s, `ResourceFlavor`s, and `Topology` objects, emitting events only when canonical object state changes.
  - Watches `Pod`s, `Job`s, `JobSet`s, `LeaderWorkerSet`s, `Deployment`s, `RayJob`s, `RayCluster`s, and Kueue `Workload`s in real time to capture exact creation, admission, scheduling, preemption, completion, and deletion timestamps.
  - Gracefully handles optional CRDs (`JobSet`, `LeaderWorkerSet`, `RayJob`, `RayCluster`, Kueue APIs) if they are not installed on the target cluster, retrying periodically without failing.
- **Strict Data Sanitization**:
  - `Node` records in `infra.json` retain only scheduling-relevant spec (`taints`, `unschedulable`) and status (`capacity`, `allocatable`, `conditions`, `phase`) fields, stripping cached `.status.images`, `.status.addresses`, `.status.nodeInfo`, attached volumes, `.spec.providerID`/CIDRs, cloud/cluster-identifying node labels, and disallowed annotations.
  - `Namespace`, `ClusterQueue`, `LocalQueue`, `ResourceFlavor`, and `Topology` records in `infra.json` omit `.status`, `metadata.managedFields`, and any disallowed/sensitive annotations.
  - Scheduled/running `Pod` records in `infra.json` store **only** node placement, effective resource requests, scheduling time, first notice time, and deletion time.
  - Workflow specs in `workflows.json` have container images replaced with a pause image (`registry.k8s.io/pause:3.10` by default), and strip commands, args, `env`/`envFrom`, probes (`livenessProbe`, `readinessProbe`, `startupProbe`), `lifecycle` hooks, `serviceAccountName`/`hostname`/`hostnameOverride`/`subdomain`/`hostAliases`/`dnsConfig` (and `JobSet.spec.network.subdomain`), volumes, secrets, `.status`, sensitive Kubernetes annotations (such as `kubectl.kubernetes.io/last-applied-configuration` and `kubernetes.io/change-cause`), and any annotations outside the official `kubernetes.io`, `k8s.io`, `x-k8s.io`, and `kueue-optimizer.gke.io` domains and their subdomains (e.g. `*.kubernetes.io`, `*.k8s.io`, `*.x-k8s.io`, `*.kueue-optimizer.gke.io`).
- **Optional Salted HMAC-SHA256 Name & Label Anonymization (`--hash-names`)**:
  - When `--hash-names` is enabled, deterministically hashes workload, pod, job, jobset, deployment, LWS, and Ray resource names (along with cross-referencing annotations and `ownerReferences`) **and** sensitive organizational label values (`owner`, `team`, `customer`, `project`, `cost-center`, ...), workload-referencing labels, and any additional label keys or `<domain>/*` patterns supplied via `--extra-hash-labels` across metadata, selectors, and pod/node affinities using a per-run CSPRNG `HMAC-SHA256` key, while keeping infrastructure names (`Node`, `Namespace`, `ClusterQueue`, `LocalQueue`, `ResourceFlavor`, `Topology`) and all label keys in plain text. When `--hash-names` is disabled, a no-op hasher leaves names and label values in plain text.
- **Streaming NDJSON Output**:
  - Appends and `fsync`s one compact JSON object per line with no outer array wrapper, enabling `O(1)`-memory streaming consumption.

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
| `--extra-hash-labels` | — | `""` | Comma-separated additional list of label keys or `<domain>/*` patterns (matched case-insensitively; flag may also be repeated) to deterministically hash across scraped objects and selectors in addition to the built-in sensitive label list (requires `--hash-names`). |
| `--qps` | — | `200` | Kubernetes API client QPS rate limit. |
| `--burst` | — | `400` | Kubernetes API client burst limit. |

---

## Output Files

Each run produces three append-only **JSON Lines (NDJSON)** files (one compact JSON event per line):

| File | Recorded Resources & Events |
| :--- | :--- |
| `infra.json` | `"New"`, `"Update"`, and `"Deleted"` events for sanitized `Node`s (scheduling-relevant `.spec` + `.status`), `Namespace`s, `ClusterQueue`s, `LocalQueue`s, `ResourceFlavor`s, `Topology` objects (without `.status`, with annotations filtered), and scheduled/running `Pod` placements and resource requests. |
| `workflows.json` | Watch-driven `"New"`, `"Update"`, and `"Deleted"` events for `Job`, `JobSet`, `LeaderWorkerSet`, `Deployment`, `RayJob`, `RayCluster`, and standalone Kueue-managed `Pod` specs, annotated with top-level `workloadState` (`"NotAdmitted"`, `"AdmissionInProgress"`, `"Admitted"`). |
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
  - apiGroups: ["apps"]
    resources: ["deployments"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["batch"]
    resources: ["jobs"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["kueue.x-k8s.io"]
    resources:
      - "clusterqueues"
      - "localqueues"
      - "resourceflavors"
      - "topologies"
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
