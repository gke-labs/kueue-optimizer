# kueue-optimizer

Agentic evolutionary optimization and deterministic replay evaluation chambers for Kubernetes [Kueue](https://kueue.sigs.k8s.io/), [JobSet](https://jobset.sigs.k8s.io/), and Workload-Aware Scheduling (WAS).

`kueue-optimizer` deploys self-contained **`kueue-optimizer` evaluation chambers** (`--namespace kueue-optimizer-chamber-<id>`) inside an existing host Kubernetes cluster to replay sanitized cluster traces and synthetic scenarios against real Kueue and JobSet controllers, evaluate custom Kueue builds via an isolated per-chamber in-memory container registry, and compute aggregated Prometheus and physical cluster saturation metrics.

---

## Architecture

Every **`kueue-optimizer` chamber** runs a complete, isolated virtual cluster control plane inside a single host Kubernetes namespace (default: `kueue-optimizer-chamber-01`):

| Host Cluster Resource | Image | Role |
| :--- | :--- | :--- |
| `Deployment/kwok-registry` & `Service/kwok-registry` (`Service/registry`) | `mirror.gcr.io/library/registry:2` | Ephemeral per-chamber OCI Distribution v2 registry backed by kernel `tmpfs` RAM (`emptyDir: { medium: Memory, sizeLimit: 4Gi }`) on `NodePort 30500 + (port - 16443)` for isolated ad-hoc Kueue builds |
| `Deployment/kwok-etcd` & `Service/kwok-etcd` | `registry.k8s.io/etcd:3.6.10-0` | Backing datastore for `kwok-kube-apiserver` |
| `Deployment/kwok-kube-apiserver` & `Service/kwok-kube-apiserver` | `registry.k8s.io/kube-apiserver:v1.36.4` | Virtual cluster API server (`:6443`) |
| `Deployment/kwok-kube-controller-manager` | `registry.k8s.io/kube-controller-manager:v1.36.4` | Reconciles Jobs, Pods, Namespaces, ServiceAccounts, and garbage collection inside the chamber |
| `Deployment/kwok-kube-scheduler` | `registry.k8s.io/kube-scheduler:v1.36.4` | Schedules Pods onto simulated KWOK `Node`s inside the chamber |
| `Deployment/kwok-controller` & `Service/kwok-controller` | `registry.k8s.io/kwok/kwok:v0.8.0` | Bootstraps KWOK CRDs, Node/Pod `Stage` rules (pods run for `kueue-optimizer.gke.io/pod-execution-time-seconds` and exit with code `0`), and simulated `Node`s |
| `Deployment/jobset-controller-manager` & `Service/kwok-jobset` | `us-central1-docker.pkg.dev/k8s-staging-images/jobset/jobset:main` | Bootstraps JobSet CRDs and webhooks into `kwok-kube-apiserver`, then runs JobSet `/manager` |
| `Deployment/kueue-controller-manager` & `Service/kwok-kueue` | `us-central1-docker.pkg.dev/k8s-staging-images/kueue/kueue:main` | Bootstraps Kueue CRDs, visibility `APIService`, and webhooks into `kwok-kube-apiserver`, then runs Kueue `/manager` with Preemption-Based Fair Sharing enabled by default (`--no-fair-sharing` to switch to Classical Preemption, or any custom image pushed into the chamber's `kwok-registry`) |
| `Deployment/prometheus` & `Service/prometheus` (`Service/kwok-prometheus`) | `quay.io/prometheus/prometheus:v3.15.1` | Scrapes Kueue metrics (`https://kwok-kueue:8443/metrics`) every `10s` into an in-memory TSDB and exposes an HTTP query API (`:9090`) |

---

## Directory Structure

```text
.
├── AGENTS.md                                      # Repository engineering & AI agent instructions
├── README.md                                      # Architecture overview, directory map & quick-start guide
├── TESTRUNNER.md                                  # Reference guide for testrunner.py & kueue_metrics_report.py (Sections 5.1–5.4)
├── Makefile                                       # Chamber deployment, scenario replay, demo & test targets
├── deploy/                                        # Chamber Kustomize manifests & Python orchestration tooling
│   ├── kustomization.yaml                         # Default entrypoint (namespace: kueue-optimizer-chamber-01 + sample queues)
│   ├── base/                                      # Core per-chamber control-plane manifests, configs & bootstrap CRDs
│   ├── overlays/with-sample-queues/               # Sample Namespace, ResourceFlavor, ClusterQueue & LocalQueue
│   └── scripts/
│       ├── chamber_common.py                      # ChamberManager, auto-PKI/kubeconfig, registry plumbing & SSA helpers
│       ├── chamber_registry.py                    # Per-chamber in-memory OCI registry CLI (status/push/build-kueue/delete/gc)
│       ├── testrunner.py                          # End-to-end scenario replay & evaluation engine
│       ├── kueue_metrics_report.py                # Aggregated Prometheus & physical saturation reporter (Sections 5.1–5.4)
│       ├── demo.py                                # Synthetic 10-node, 20-JobSet smoke test & demo runner
│       ├── generate_scenario_readme.py            # Auto-generates per-scenario README.md summaries (--all or single dir)
│       ├── generate.sh                            # Regenerates embedded manifests from ./kwok, ./kueue, and ./jobset
│       ├── test_chamber_registry.py               # Unit tests for per-chamber OCI registry & NodePort allocation
│       ├── test_kueue_metrics_report.py           # Unit tests for Sections 5.1–5.4 metrics & saturation accounting
│       └── test_testrunner_and_readme.py          # Unit tests for testrunner, generate_scenario_readme & shared helpers
├── testscenarios/
│   └── demo/                                      # Baseline 3-file scenario bundle (infra.json, workflows.json, execution-time.json)
├── scraper/                                       # Live Kubernetes & Kueue cluster trace scraper & sanitizer (Go)
├── skills/                                        # Repository agent skills
│   ├── chamber-evaluation/SKILL.md                # Chamber execution, per-chamber registry & controlled A/B testing protocol
│   └── generate-testscenario/SKILL.md             # Parametric scenario authoring & validation workflow
└── docs/
    └── contributing.md                            # Contribution guidelines & CLA instructions
```

---

## Prerequisites

- **`kubectl`** (`v1.30+` with built-in `kubectl kustomize`) configured against a host Kubernetes cluster (for example, a GKE or Kind cluster with `~8` vCPUs and `~12Gi` free memory per concurrent chamber).
- **`python3`** (`3.12+`, standard library only — no third-party pip packages required) and **`openssl`** (used to generate ephemeral per-chamber TLS PKI at deploy time).
- **`go`** (`1.27.3+`, optional — required only when building `scraper/` or custom Kueue binaries) and **`docker`** (optional — required only when pushing custom Kueue images to a chamber's in-memory `kwok-registry`).

---

## Quick Start

### 1. Replay a 3-File Scenario Bundle (`deploy/scripts/testrunner.py`)

`deploy/scripts/testrunner.py` deploys an isolated **`kueue-optimizer` chamber**, recreates the cluster topology and queues from `infra.json`, replays `workflows.json` with durations from `execution-time.json`, prints the **Sections `5.1`–`5.4`** evaluation report via `deploy/scripts/kueue_metrics_report.py`, and cleans up the chamber namespace automatically:

```bash
# Inspect scenario timeline, node capacity, and queue hierarchy offline (no cluster needed):
python3 deploy/scripts/testrunner.py testscenarios/demo --dry-run

# Deploy an isolated chamber, replay testscenarios/demo at 2x speedup, evaluate, and clean up:
python3 deploy/scripts/testrunner.py testscenarios/demo \
  --namespace kueue-optimizer-chamber-demo \
  --port 16450 \
  --speedup 2

# Compare StrictFIFO (Arm A) vs. BestEffortFIFO (Arm B) in separate isolated chambers:
python3 deploy/scripts/testrunner.py testscenarios/demo \
  --namespace kueue-optimizer-chamber-arm-a --port 16451 --speedup 2 --strict-fifo

python3 deploy/scripts/testrunner.py testscenarios/demo \
  --namespace kueue-optimizer-chamber-arm-b --port 16452 --speedup 2 --best-effort-fifo
```

### 2. Evaluate Custom or Patched Kueue Builds via Per-Chamber In-Memory Registry

Every chamber includes an ephemeral in-memory OCI registry (`kwok-registry`) backed by kernel `tmpfs` RAM so custom Kueue images never need to be pushed to external registries:

```bash
# Evaluate a local Docker image in an isolated chamber:
python3 deploy/scripts/testrunner.py testscenarios/demo \
  --namespace kueue-optimizer-chamber-exp1 --port 16453 \
  --kueue-image my-local-kueue:dev --speedup 2

# Or build Kueue directly from a local ./kueue checkout, push to kwok-registry, and evaluate:
python3 deploy/scripts/testrunner.py testscenarios/demo \
  --namespace kueue-optimizer-chamber-exp2 --port 16454 \
  --kueue-build-dir ./kueue --speedup 2
```

### 3. Run the Standalone Synthetic Demo (`deploy/scripts/demo.py`)

```bash
# Deploy a chamber, create 10 simulated KWOK nodes (4 CPU / 16Gi), submit 20 JobSets,
# print the Kueue metrics report, and tear down the chamber:
python3 deploy/scripts/demo.py

# Or via Make:
make demo NAMESPACE=kueue-optimizer-chamber-01
```

### 4. Scrape a Live Cluster into a Replayable Scenario (`scraper/`)

See [`scraper/README.md`](scraper/README.md) and [`scraper/SCRAPER-SCHEMA.md`](scraper/SCRAPER-SCHEMA.md) for capturing sanitized `infra.json`, `workflows.json`, and `execution-time.json` traces from an existing Kubernetes + Kueue cluster:

```bash
make scraper-build
./scraper/cluster-scraper --duration 30m --hash-names --output-dir ./testscenarios/captured-run
python3 deploy/scripts/generate_scenario_readme.py ./testscenarios/captured-run
```

### 5. Run Unit Tests

```bash
make test
```

---

## Contributing

This project is licensed under the [Apache 2.0 License](LICENSE).

We welcome contributions! Please see [docs/contributing.md](docs/contributing.md) for more information.

We follow [Google's Open Source Community Guidelines](https://opensource.google.com/conduct/).

## Disclaimer

This is not an officially supported Google product.

This project is not eligible for the Google Open Source Software Vulnerability Rewards Program.
