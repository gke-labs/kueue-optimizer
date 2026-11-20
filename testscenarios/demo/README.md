# Test Scenario Summary: `demo`

> **Auto-generated scenario analysis** from [`testscenarios/demo/infra.json`](./infra.json), [`testscenarios/demo/workflows.json`](./workflows.json), and [`testscenarios/demo/execution-time.json`](./execution-time.json) via [`deploy/scripts/generate_scenario_readme.py`](../../deploy/scripts/generate_scenario_readme.py).

## 1. Scenario Overview

| Dimension | Summary |
| :--- | :--- |
| **Scenario Directory** | `testscenarios/demo` |
| **Raw Event Counts** | `infra.json`: **333** events · `workflows.json`: **119** events · `execution-time.json`: **40** events |
| **Cluster Capacity** | **10 nodes** · `40 cores` · `160 GiB` (1 distinct node group) |
| **Topology Configuration** | **0** `Topology` CRD(s) (none) |
| **Initial In-Flight Pods at $t_0$** | **0** pods across **0** / 10 nodes (initial `cpu` utilization: **0.00%**) |
| **Kueue Objects** | **1** `ResourceFlavor`(s) · **0** `Cohort`(s) · **1** `ClusterQueue`(s) · **1** `LocalQueue`(s) · **0** `WorkloadPriorityClass`(es) |
| **Test Arrival Window ($T_{\text{test}}$)** | **0.3s** (`2026-10-30T12:06:01.442Z` $\rightarrow$ `2026-10-30T12:06:01.749Z`) |
| **Unadmitted Workloads** | **20** `NotAdmitted` arrivals (154 total pods; gang size min=5, p50=8, p90=10, max=10) + **0** pre-admitted at $t_0$ |
| **Execution Duration Quantiles** | `min=11s`, `P50=11s`, `AVG=13s`, `P90=20s`, `P99=21s`, `max=21s` |
| **1. Saturation (Arrival Window $T_{\text{test}}$)** | `cpu`: **39499.76%** · `memory`: **31657.30%** *(over $T_{\text{test}} = 0.3s$)* |
| **2. Saturation (Reference `6h` / `21,600s` Window)** | `cpu`: **0.56%** · `memory`: **0.45%** *(over $T_{6\text{h}} = 21{,}600\text{s}$)* |
| **3. Recommended `100%` Saturation Settings (`5m` & `10m`)** | **5m (`300s`)**: `--speedup 0.404 --duration 5m` · **10m (`600s`)**: `--speedup 0.202 --duration 10m` *(bottleneck: `cpu`, $T_{100\%} = 121.1s (2.0m)$)* |

---

## 2. Cluster Nodes & Distinct Node Groups

The cluster contains **10** `v1/Node` objects partitioned into **1** distinct hardware/label configuration group(s).

### 2.1 Aggregate Physical Cluster Capacity & Allocatable Resources

| Resource | Total Capacity | Total Allocatable | Per-Node Average (Capacity) |
| :--- | ---: | ---: | ---: |
| `nodes` | **10** | **10** | `1` |
| `cpu` | **40 cores** | **40 cores** | 4 cores |
| `memory` | **160 GiB** | **160 GiB** | 16 GiB |
| `pods` | **1,100** | **1,100** | 110 |

### 2.2 Summary of Distinct Node Groups

| Group | Node Count | Share | Sample Node Name | `cpu` / Node | `memory` / Node | `pods` / Node | Accelerators | Key Identifying Labels |
| :--- | ---: | ---: | :--- | ---: | ---: | ---: | :---: | :--- |
| **Group 1: CPU Node (4 CPU, 16Gi RAM)** | **10** | 100.0% | `kwok-node-0` | `4` | `16Gi` | `110` | none | `kubernetes.io/role=agent`, `kueue-optimizer.gke.io/node-role=agent`, `type=kwok` |

### 2.3 Sample Node Manifest (YAML) from Each Distinct Group

#### Group 1: CPU Node (4 CPU, 16Gi RAM) — Sample Node `kwok-node-0` (10 nodes in group)

```yaml
apiVersion: v1
kind: Node
metadata:
  name: kwok-node-0
  creationTimestamp: "2026-10-30T11:56:46Z"
  labels:
    beta.kubernetes.io/arch: amd64
    beta.kubernetes.io/os: linux
    kueue-optimizer.gke.io/node-role: agent
    kubernetes.io/arch: amd64
    kubernetes.io/hostname: kwok-node-0
    kubernetes.io/os: linux
    kubernetes.io/role: agent
    node-role.kubernetes.io/agent: ""
    type: kwok
  annotations:
    kueue-optimizer.gke.io/node-cpu: "4"
    kueue-optimizer.gke.io/node-memory: 16Gi
    kueue-optimizer.gke.io/node-pods: "110"
    kwok.x-k8s.io/node: fake
    node.alpha.kubernetes.io/ttl: "0"
spec: {}
status:
  capacity:
    cpu: "4"
    memory: 16Gi
    pods: "110"
  allocatable:
    cpu: "4"
    memory: 16Gi
    pods: "110"
  conditions:
  - lastHeartbeatTime: "2026-10-30T11:56:46Z"
    lastTransitionTime: "2026-10-30T11:56:46Z"
    message: "kubelet is posting ready status"
    reason: KubeletReady
    status: "True"
    type: Ready
  - lastHeartbeatTime: "2026-10-30T11:56:46Z"
    lastTransitionTime: "2026-10-30T11:56:46Z"
    message: "kubelet has sufficient memory available"
    reason: KubeletHasSufficientMemory
    status: "False"
    type: MemoryPressure
  - lastHeartbeatTime: "2026-10-30T11:56:46Z"
    lastTransitionTime: "2026-10-30T11:56:46Z"
    message: "kubelet has no disk pressure"
    reason: KubeletHasNoDiskPressure
    status: "False"
    type: DiskPressure
  - lastHeartbeatTime: "2026-10-30T11:56:46Z"
    lastTransitionTime: "2026-10-30T11:56:46Z"
    message: "kubelet has sufficient PID available"
    reason: KubeletHasSufficientPID
    status: "False"
    type: PIDPressure
  - lastHeartbeatTime: "2026-10-30T11:56:46Z"
    lastTransitionTime: "2026-10-30T11:56:46Z"
    message: "RouteController created a route"
    reason: RouteCreated
    status: "False"
    type: NetworkUnavailable
  phase: Running
```

---

## 3. Cluster Topology & Node Distribution

No `kueue.x-k8s.io/Topology` CRD is defined in `infra.json` (Topology-Aware Scheduling is not explicitly configured via a `Topology` object).

---

## 4. Initial Running Pods & Initial Cluster Utilization at $t_0$

- **Initial Running Pods in `infra.json`**: **`0`** (the cluster starts completely empty at $t_0 = 2026-10-30T12:05:52.657Z$).
- **Initial Cluster Utilization**: **`0.00%`** across all resources (`cpu`, `memory`, `pods`, accelerators).

---

## 5. Kueue Setup: ResourceFlavors, Cohorts, ClusterQueues, Namespaces & LocalQueues

### 5.1 ResourceFlavors (`ResourceFlavor`) & Topology Binding

| ResourceFlavor Name | Bound `topologyName` | Node Selector Labels (`spec.nodeLabels`) | Node Taints / Tolerations |
| :--- | :--- | :--- | :--- |
| `default-flavor` | `*(none)*` | `type=kwok` | none |

### 5.2 Workload Priority Classes (`WorkloadPriorityClass`)

*(No explicit `WorkloadPriorityClass` objects defined in `infra.json`; all workloads run at default priority unless annotated.)*

### 5.3 Cohorts & ClusterQueues — Quotas, Preemption & Fair Sharing

#### ClusterQueue Quotas, Preemption & Fair-Sharing Configuration

| ClusterQueue | Cohort | Queueing Strategy | FS Weight | Flavors | Nominal `cpu` | Nominal `memory` | Nominal `pods` | Borrowing / Lending Limits | Preemption Policy (`withinCQ` / `reclaim` / `borrow`) |
| :--- | :--- | :--- | :---: | :--- | ---: | ---: | ---: | :--- | :--- |
| `ai-lab-cq` | `*(standalone)*` | `StrictFIFO` | `1` | `default-flavor` | **40 cores** | **160 GiB** | **1,100** | unbounded (default) | `Never` / `Never` / `Never` |
| **Total Nominal Quota (CQs + Cohort)** | — | — | — | — | **40 cores** (100.0% of cluster) | **160 GiB** (100.0% of cluster) | **1,100** (100.0% of cluster) | — | — |

### 5.4 Namespaces & LocalQueues (`Namespace` & `LocalQueue`)

- **Namespaces (`7`)**: `ai-lab`, `default`, `jobset-system`, `kube-node-lease`, `kube-public`, `kube-system`, `kueue-system`

| Namespace | LocalQueue Name | Target `ClusterQueue` (`spec.clusterQueue`) | Target CQ Exists? | Target Cohort | Unadmitted Workloads Submitted |
| :--- | :--- | :--- | :---: | :--- | ---: |
| `ai-lab` | `ai-lab-lq` | `ai-lab-cq` | Yes | `*(standalone)*` | **20** |

---

## 6. Scenario Timeline & Arrival Dynamics

| Timeline Metric | Value |
| :--- | :--- |
| **Infrastructure Bootstrap Timestamp ($t_0$)** | `2026-10-30T12:05:52.657Z` |
| **First Unadmitted Workload Arrival ($t_{\text{first}}$)** | `2026-10-30T12:06:01.442Z` (offset `+8.79s` from $t_0$) |
| **Last Unadmitted Workload Arrival ($t_{\text{last}}$)** | `2026-10-30T12:06:01.749Z` (offset `+9.09s` from $t_0$) |
| **Active Test Arrival Duration ($T_{\text{test}} = t_{\text{last}} - t_{\text{first}}$)** | **0.3s** |
| **Unadmitted Workload Submission Count** | **20** workloads (plus **0** pre-admitted at $t_0$) |
| **Mean Arrival Rate ($\lambda$)** | **3914.10 workloads/min** (**234845.8 workloads/hour**) |
| **Inter-Arrival Time ($\Delta t_i$) Distribution** | `min=0.00s`, `P50=0.00s`, `AVG=0.02s`, `P90=0.03s`, `P99=0.13s`, `max=0.14s` |

```text
=== Unadmitted Workload Arrival Distribution Across 12 Equal Time Windows (total=20, span=0.3s) ===
  [   0.0s -    0.0s] |     8 ( 40.0%) | ████████████████████████████████
  [   0.0s -    0.1s] |     2 ( 10.0%) | ████████
  [   0.1s -    0.1s] |     0 (  0.0%) | 
  [   0.1s -    0.1s] |     0 (  0.0%) | 
  [   0.1s -    0.1s] |     0 (  0.0%) | 
  [   0.1s -    0.2s] |     5 ( 25.0%) | ████████████████████
  [   0.2s -    0.2s] |     3 ( 15.0%) | ████████████
  [   0.2s -    0.2s] |     0 (  0.0%) | 
  [   0.2s -    0.2s] |     0 (  0.0%) | 
  [   0.2s -    0.3s] |     0 (  0.0%) | 
  [   0.3s -    0.3s] |     0 (  0.0%) | 
  [   0.3s -    0.3s] |     2 ( 10.0%) | ████████
```

---

## 7. Workload Priorities, Cluster Saturation, Per-LocalQueue Dissection & Duration Histograms

### 7.0 Three-Part Offered Cluster Saturation & Recommended `5m` / `10m` Replay Settings

For any resource $r \in \{\text{accelerators}, \text{cpu}, \text{memory}\}$ with physical cluster capacity $C_{\text{cluster}}(r)$ and total offered resource-seconds $W(r) = \sum_{w} \left(\text{size}_w(r) \times \text{duration}_w\right)$, let $T_{100\%}(r) = W(r) / C_{\text{cluster}}(r)$ denote the ideal `1.0x` full-cluster service time required at continuous `100%` utilization:

$$\text{Sat}_{\text{window}}(r) = \frac{W(r)}{T_{\text{window}} \times C_{\text{cluster}}(r)} \times 100\% = \frac{T_{100\%}(r)}{T_{\text{window}}} \times 100\%, \qquad N_{100\%}(T_{\text{target}}) = \frac{T_{100\%}(r^*)}{T_{\text{target}}}$$

#### Parts 1 & 2 — Offered Saturation: Active Arrival Window ($T_{\text{test}}$) vs. Default `6h` (`21,600s`) Duration

| Resource ($r$) | Physical Cluster Capacity $C_{\text{cluster}}(r)$ | Total Offered Work ($1.0\text{x}$) | Ideal `100%`-Util Service Time $T_{100\%}(r)$ | (1) Saturation over Arrival Window ($T_{\text{test}} = 0.3s$) | (2) Saturation over Default `6h` ($21{,}600\text{s}$) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **`cpu`** *(bottleneck)* | 40 cores | `1.35` core-h | `121.1s (2.0m)` | **39499.76%** | **0.56%** |
| `memory` | 160 GiB | `4.31` GiB-h | `97.1s (1.6m)` | 31657.30% | 0.45% |

#### Part 3 — Recommended `testrunner.py` Settings for `100%` Saturation in `5m` (`300s`) & `10m` (`600s`)

| Target Wall-Clock Window ($T_{\text{target}}$) | Bottleneck Resource ($r^*$) | Recommended `--speedup` ($N_{100\%}$) | Recommended `--duration` | Compressed Arrival Span ($T_{\text{test}} / N_{100\%}$) | Execution & Drain Behavior |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **`5m` (`300s`)** | `cpu` | **`--speedup 0.404`** | **`--duration 5m`** | `0.8s` | All arrivals submit within `0.8s` (≤ `300s`); `--duration 5m` captures `100%` of arrivals and `100.0%` full-cluster work in `5m`. |
| **`10m` (`600s`)** | `cpu` | **`--speedup 0.202`** | **`--duration 10m`** | `1.5s` | All arrivals submit within `1.5s` (≤ `600s`); `--duration 10m` captures `100%` of arrivals and `100.0%` full-cluster work in `10m`. |

```bash
# Run at 100% offered cpu saturation in a 5-minute (300s) wall-clock window:
python3 deploy/scripts/testrunner.py --scenario testscenarios/demo --speedup 0.404 --duration 5m

# Run at 100% offered cpu saturation in a 10-minute (600s) wall-clock window:
python3 deploy/scripts/testrunner.py --scenario testscenarios/demo --speedup 0.202 --duration 10m
```

### 7.1 Workload Breakdown & Cluster Saturation by Priority Class

| Priority Class | Priority Value | Workloads | Share | Total Pods | Gang Size (`min / P50 / P90 / max`) | Duration (`P50 / AVG / P90 / P99`) | Offered Core-Hours | `cpu` Saturation | `memory` Saturation |
| :--- | ---: | ---: | ---: | ---: | :---: | :---: | ---: | ---: | ---: |
| `<default>` | `0` | 20 | 100.0% | 154 | `5 / 8 / 10 / 10` | `11s / 13s / 20s / 21s` | 1.3 | 39499.76% | 31657.30% |
| **ALL (Total)** | — | **20** | 100.0% | 154 | `5 / 8 / 10 / 10` | `11s / 13s / 20s / 21s` | 1.3 | **39499.76%** | **31657.30%** |

### 7.2 Workload Dissection per `LocalQueue` (and Target `ClusterQueue`)

| LocalQueue (`ns/name`) | Target `ClusterQueue` | Workloads | Share | Total Pods | Duration (`P50 / AVG / P90 / P99`) | Offered Core-Hours | Cluster `cpu` Saturation | Target CQ Nominal `cpu` Saturation | Priority Breakdown (Count) |
| :--- | :--- | ---: | ---: | ---: | :---: | ---: | ---: | ---: | :--- |
| `ai-lab/ai-lab-lq` | `ai-lab-cq` | **20** | 100.0% | 154 | `11s / 13s / 20s / 21s` | 1.3 | **39499.76%** | **39499.76%** | `<default>`:20 |

#### Detailed Per-`LocalQueue` × Priority Dissection

<details>
<summary><b>LocalQueue <code>ai-lab/ai-lab-lq</code> → ClusterQueue <code>ai-lab-cq</code></b> (20 workloads)</summary>

| Priority Class | Priority Value | Workloads | Share of LQ | Total Pods | Duration (`P50 / AVG / P90 / P99`) | Cluster `cpu` Saturation | Cluster `memory` Saturation |
| :--- | ---: | ---: | ---: | ---: | :---: | ---: | ---: |
| `<default>` | `0` | **20** | 100.0% | 154 | `11s / 13s / 20s / 21s` | 39499.76% | 31657.30% |

</details>

### 7.3 Workload Execution Duration Histograms (Aggregated & Per Priority)

#### Aggregated Duration Histogram (All Unadmitted Workloads)

```text
=== ALL PRIORITIES (Aggregated) | count=20 | min=11s, p50=11s (0.2m), avg=12.6s (0.2m), p90=20s (0.3m), p99=21s (0.3m), max=21s ===
  < 1m   (0-60s)       |    20 (100.0%) | ████████████████████████████████
  1-5m   (60-300s)     |     0 (  0.0%) | 
  5-10m  (300-600s)    |     0 (  0.0%) | 
  10-15m (600-900s)    |     0 (  0.0%) | 
  15-20m (900-1200s)   |     0 (  0.0%) | 
  20-30m (1200-1800s)  |     0 (  0.0%) | 
```

#### Per-Priority Duration Histograms

```text
=== Priority: <default> (value=0) | count=20 | min=11s, p50=11s (0.2m), avg=12.6s (0.2m), p90=20s (0.3m), p99=21s (0.3m), max=21s ===
  < 1m   (0-60s)       |    20 (100.0%) | ████████████████████████████████
  1-5m   (60-300s)     |     0 (  0.0%) | 
  5-10m  (300-600s)    |     0 (  0.0%) | 
  10-15m (600-900s)    |     0 (  0.0%) | 
  15-20m (900-1200s)   |     0 (  0.0%) | 
  20-30m (1200-1800s)  |     0 (  0.0%) | 
```
