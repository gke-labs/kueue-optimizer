# Test Scenario Summary: `st64-tas-multi-node-failure-eviction-and-readmit`

> **Auto-generated scenario analysis** from [`testscenarios/self-tests/st64-tas-multi-node-failure-eviction-and-readmit/infra.json`](./infra.json), [`testscenarios/self-tests/st64-tas-multi-node-failure-eviction-and-readmit/workflows.json`](./workflows.json), and [`testscenarios/self-tests/st64-tas-multi-node-failure-eviction-and-readmit/execution-time.json`](./execution-time.json) via [`deploy/scripts/generate_scenario_readme.py`](../../../deploy/scripts/generate_scenario_readme.py).

## 1. Scenario Overview

| Dimension | Summary |
| :--- | :--- |
| **Scenario Directory** | `testscenarios/self-tests/st64-tas-multi-node-failure-eviction-and-readmit` |
| **Raw Event Counts** | `infra.json`: **16** events · `workflows.json`: **1** events · `execution-time.json`: **4** events |
| **Cluster Capacity** | **8 nodes** · `128 cores` · `512 GiB` · `32` `google.com/tpu` (1 distinct node group) |
| **Topology Configuration** | **1** `Topology` CRD(s) (`st-slice-topology`) |
| **Initial In-Flight Pods at $t_0$** | **0** pods across **0** / 8 nodes (initial `google.com/tpu` utilization: **0.00%**) |
| **Kueue Objects** | **1** `ResourceFlavor`(s) · **0** `Cohort`(s) · **1** `ClusterQueue`(s) · **1** `LocalQueue`(s) · **1** `WorkloadPriorityClass`(es) |
| **Test Arrival Window ($T_{\text{test}}$)** | **0s** (`2026-06-01T00:00:00Z` $\rightarrow$ `2026-06-01T00:00:00Z`) |
| **Unadmitted Workloads** | **1** `NotAdmitted` arrivals (4 total pods; gang size min=4, p50=4, p90=4, max=4) + **0** pre-admitted at $t_0$ |
| **Execution Duration Quantiles** | `min=13s`, `P50=13s`, `AVG=13s`, `P90=13s`, `P99=13s`, `max=13s` |
| **1. Saturation (Arrival Window $T_{\text{test}}$)** | n/a (instantaneous $t=0$ burst: $T_{\text{test}} = 0$) *(over $T_{\text{test}} = 0.0s$)* |
| **2. Saturation (Reference `6h` / `21,600s` Window)** | `google.com/tpu`: **0.03%** · `cpu`: **0.02%** · `memory`: **0.02%** *(over $T_{6\text{h}} = 21{,}600\text{s}$)* |
| **3. Recommended `100%` Saturation Settings (`5m` & `10m`)** | **5m (`300s`)**: `--speedup 0.022 --duration 5m` · **10m (`600s`)**: `--speedup 0.011 --duration 10m` *(bottleneck: `google.com/tpu`, $T_{100\%} = 6.5s$)* |

---

## 2. Cluster Nodes & Distinct Node Groups

The cluster contains **8** `v1/Node` objects partitioned into **1** distinct hardware/label configuration group(s).

### 2.1 Aggregate Physical Cluster Capacity & Allocatable Resources

| Resource | Total Capacity | Total Allocatable | Per-Node Average (Capacity) |
| :--- | ---: | ---: | ---: |
| `nodes` | **8** | **8** | `1` |
| `cpu` | **128 cores** | **128 cores** | 16 cores |
| `memory` | **512 GiB** | **512 GiB** | 64 GiB |
| `pods` | **880** | **880** | 110 |
| `google.com/tpu` | **32** | **32** | 4 |

### 2.2 Summary of Distinct Node Groups

| Group | Node Count | Share | Sample Node Name | `cpu` / Node | `memory` / Node | `pods` / Node | `google.com/tpu` / Node | Key Identifying Labels |
| :--- | ---: | ---: | :--- | ---: | ---: | ---: | ---: | :--- |
| **Group 1: accel=`tpu-v7x-st`** | **8** | 100.0% | `st-node-00` | `16` | `64Gi` | `110` | 4 | `cloud.google.com/gke-tpu-accelerator=tpu-v7x-st`, `kubernetes.io/role=agent`, `kueue-optimizer.gke.io/node-role=agent`, `type=kwok` |

### 2.3 Sample Node Manifest (YAML) from Each Distinct Group

#### Group 1: accel=`tpu-v7x-st` — Sample Node `st-node-00` (8 nodes in group)

```yaml
apiVersion: v1
kind: Node
metadata:
  name: st-node-00
  uid: 16751fe1-f5c4-cfa8-517e-0eb6a9ba0d70
  labels:
    type: kwok
    kubernetes.io/arch: amd64
    kubernetes.io/os: linux
    kubernetes.io/role: agent
    node-role.kubernetes.io/agent: ""
    kueue-optimizer.gke.io/node-role: agent
    kubernetes.io/hostname: st-node-00
    cloud.google.com/gke-tpu-accelerator: tpu-v7x-st
    cloud.google.com/gce-topology-block: st-block-00
    cloud.google.com/gke-tpu-partition-4x4x4-id: b00-c00
    cloud.google.com/gke-tpu-partition-2x4x4-id: b00-c00-p244-0
    cloud.google.com/gke-tpu-partition-2x2x4-id: b00-c00-p224-0
    cloud.google.com/gke-tpu-partition-2x2x2-id: b00-c00-p222-0
    cloud.google.com/gke-tpu-partition-2x2x1-id: b00-c00-p221-00
  annotations:
    kwok.x-k8s.io/node: fake
    node.alpha.kubernetes.io/ttl: "0"
    kueue-optimizer.gke.io/node-cpu: "16"
    kueue-optimizer.gke.io/node-memory: 64Gi
    kueue-optimizer.gke.io/node-pods: "110"
    kueue-optimizer.gke.io/node-accel-resource: google.com/tpu
    kueue-optimizer.gke.io/node-accel-count: "4"
spec: {}
status:
  capacity:
    cpu: "16"
    memory: 64Gi
    pods: "110"
    google.com/tpu: "4"
  allocatable:
    cpu: "16"
    memory: 64Gi
    pods: "110"
    google.com/tpu: "4"
  conditions:
  - type: Ready
    status: "True"
    reason: KubeletReady
    message: "kwok kubelet is posting ready status"
```

---

## 3. Cluster Topology & Node Distribution

This scenario defines **1** Kueue `Topology` resource(s) for Topology-Aware Scheduling (`TAS`):

### 3.1 Topology `st-slice-topology` (7 Hierarchy Levels)

```yaml
apiVersion: kueue.x-k8s.io/v1beta2
kind: Topology
metadata:
  name: st-slice-topology
  uid: 0cd5b908-2180-912b-496d-01327038d8c0
spec:
  levels:
  - nodeLabel: cloud.google.com/gce-topology-block
  - nodeLabel: cloud.google.com/gke-tpu-partition-4x4x4-id
  - nodeLabel: cloud.google.com/gke-tpu-partition-2x4x4-id
  - nodeLabel: cloud.google.com/gke-tpu-partition-2x2x4-id
  - nodeLabel: cloud.google.com/gke-tpu-partition-2x2x2-id
  - nodeLabel: cloud.google.com/gke-tpu-partition-2x2x1-id
  - nodeLabel: kubernetes.io/hostname
```

#### Node Distribution Across `st-slice-topology` Hierarchy Levels

| Level | Topology Label (`nodeLabel`) | Distinct Domains | Nodes w/ Label | Min Nodes/Domain | Median (P50) | Mean Nodes/Domain | Max Nodes/Domain | Domain Balance | Sample Domain Counts |
| :---: | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- | :--- |
| `L1` | `cloud.google.com/gce-topology-block` | **1** | `8 / 8` | `8` | `8` | `8.0` | `8` | Uniform | `st-block-00` (8) |
| `L2` | `cloud.google.com/gke-tpu-partition-4x4x4-id` | **1** | `8 / 8` | `8` | `8` | `8.0` | `8` | Uniform | `b00-c00` (8) |
| `L3` | `cloud.google.com/gke-tpu-partition-2x4x4-id` | **1** | `8 / 8` | `8` | `8` | `8.0` | `8` | Uniform | `b00-c00-p244-0` (8) |
| `L4` | `cloud.google.com/gke-tpu-partition-2x2x4-id` | **2** | `8 / 8` | `4` | `4` | `4.0` | `4` | Uniform | `b00-c00-p224-0` (4), `b00-c00-p224-1` (4) |
| `L5` | `cloud.google.com/gke-tpu-partition-2x2x2-id` | **4** | `8 / 8` | `2` | `2` | `2.0` | `2` | Uniform | `b00-c00-p222-0` (2), `b00-c00-p222-1` (2), `b00-c00-p222-2` (2), … (4x `2` nodes) |
| `L6` | `cloud.google.com/gke-tpu-partition-2x2x1-id` | **8** | `8 / 8` | `1` | `1` | `1.0` | `1` | Uniform | `b00-c00-p221-00` (1), `b00-c00-p221-01` (1), `b00-c00-p221-02` (1), … (8x `1` nodes) |
| `L7` | `kubernetes.io/hostname` | **8** | `8 / 8` | `1` | `1` | `1.0` | `1` | Uniform | `st-node-00` (1), `st-node-01` (1), `st-node-02` (1), … (8x `1` nodes) |

#### Hierarchical Tree Branching Summary (`st-slice-topology`)

```text
Cluster Total: 8 nodes
  └── L1 [cloud.google.com/gce-topology-block]: 1 total domains (1 domain(s) of 8 node(s))
    └── L2 [cloud.google.com/gke-tpu-partition-4x4x4-id]: 1 total domains (1 domain(s) of 8 node(s))
      └── L3 [cloud.google.com/gke-tpu-partition-2x4x4-id]: 1 total domains (1 domain(s) of 8 node(s))
        └── L4 [cloud.google.com/gke-tpu-partition-2x2x4-id]: 2 total domains (2 domain(s) of 4 node(s))
          └── L5 [cloud.google.com/gke-tpu-partition-2x2x2-id]: 4 total domains (4 domain(s) of 2 node(s))
            └── L6 [cloud.google.com/gke-tpu-partition-2x2x1-id]: 8 total domains (8 domain(s) of 1 node(s))
              └── L7 [kubernetes.io/hostname]: 8 total domains (8 domain(s) of 1 node(s))
```

---

## 4. Initial Running Pods & Initial Cluster Utilization at $t_0$

- **Initial Running Pods in `infra.json`**: **`0`** (the cluster starts completely empty at $t_0 = 2026-06-01T00:00:00Z$).
- **Initial Cluster Utilization**: **`0.00%`** across all resources (`cpu`, `memory`, `pods`, accelerators).

---

## 5. Kueue Setup: ResourceFlavors, Cohorts, ClusterQueues, Namespaces & LocalQueues

### 5.1 ResourceFlavors (`ResourceFlavor`) & Topology Binding

| ResourceFlavor Name | Bound `topologyName` | Node Selector Labels (`spec.nodeLabels`) | Node Taints / Tolerations |
| :--- | :--- | :--- | :--- |
| `st-tpu-flavor` | `st-slice-topology` | `cloud.google.com/gke-tpu-accelerator=tpu-v7x-st`, `type=kwok` | none |

### 5.2 Workload Priority Classes (`WorkloadPriorityClass`)

| Priority Class Name | Priority `value` | API Version | Description |
| :--- | ---: | :--- | :--- |
| `p-standard` | **`100`** | `kueue.x-k8s.io/v1beta2` | — |

### 5.3 Cohorts & ClusterQueues — Quotas, Preemption & Fair Sharing

#### ClusterQueue Quotas, Preemption & Fair-Sharing Configuration

| ClusterQueue | Cohort | Queueing Strategy | FS Weight | Flavors | Nominal `cpu` | Nominal `memory` | Nominal `google.com/tpu` | Borrowing / Lending Limits | Preemption Policy (`withinCQ` / `reclaim` / `borrow`) |
| :--- | :--- | :--- | :---: | :--- | ---: | ---: | ---: | :--- | :--- |
| `cq-tas-multi` | `*(standalone)*` | `BestEffortFIFO` | `1` | `st-tpu-flavor` | **128 cores** | **512 GiB** | **32** | unbounded (default) | `Never` / `Never` / `Never` |
| **Total Nominal Quota (CQs + Cohort)** | — | — | — | — | **128 cores** (100.0% of cluster) | **512 GiB** (100.0% of cluster) | **32** (100.0% of cluster) | — | — |

### 5.4 Namespaces & LocalQueues (`Namespace` & `LocalQueue`)

- **Namespaces (`1`)**: `st-workloads`

| Namespace | LocalQueue Name | Target `ClusterQueue` (`spec.clusterQueue`) | Target CQ Exists? | Target Cohort | Unadmitted Workloads Submitted |
| :--- | :--- | :--- | :---: | :--- | ---: |
| `st-workloads` | `lq-tas-multi` | `cq-tas-multi` | Yes | `*(standalone)*` | **1** |

---

## 6. Scenario Timeline & Arrival Dynamics

| Timeline Metric | Value |
| :--- | :--- |
| **Infrastructure Bootstrap Timestamp ($t_0$)** | `2026-06-01T00:00:00Z` |
| **First Unadmitted Workload Arrival ($t_{\text{first}}$)** | `2026-06-01T00:00:00Z` (offset `+0.00s` from $t_0$) |
| **Last Unadmitted Workload Arrival ($t_{\text{last}}$)** | `2026-06-01T00:00:00Z` (offset `+0.00s` from $t_0$) |
| **Active Test Arrival Duration ($T_{\text{test}} = t_{\text{last}} - t_{\text{first}}$)** | **0s** |
| **Unadmitted Workload Submission Count** | **1** workloads (plus **0** pre-admitted at $t_0$) |

```text
  All 1 unadmitted workloads arrive simultaneously at t = 0.0s.
```

---

## 7. Workload Priorities, Cluster Saturation, Per-LocalQueue Dissection & Duration Histograms

### 7.0 Three-Part Offered Cluster Saturation & Recommended `5m` / `10m` Replay Settings

For any resource $r \in \{\text{accelerators}, \text{cpu}, \text{memory}\}$ with physical cluster capacity $C_{\text{cluster}}(r)$ and total offered resource-seconds $W(r) = \sum_{w} \left(\text{size}_w(r) \times \text{duration}_w\right)$, let $T_{100\%}(r) = W(r) / C_{\text{cluster}}(r)$ denote the ideal `1.0x` full-cluster service time required at continuous `100%` utilization:

$$\text{Sat}_{\text{window}}(r) = \frac{W(r)}{T_{\text{window}} \times C_{\text{cluster}}(r)} \times 100\% = \frac{T_{100\%}(r)}{T_{\text{window}}} \times 100\%, \qquad N_{100\%}(T_{\text{target}}) = \frac{T_{100\%}(r^*)}{T_{\text{target}}}$$

#### Parts 1 & 2 — Offered Saturation: Active Arrival Window ($T_{\text{test}}$) vs. Default `6h` (`21,600s`) Duration

| Resource ($r$) | Physical Cluster Capacity $C_{\text{cluster}}(r)$ | Total Offered Work ($1.0\text{x}$) | Ideal `100%`-Util Service Time $T_{100\%}(r)$ | (1) Saturation over Arrival Window ($T_{\text{test}} = 0.0s$) | (2) Saturation over Default `6h` ($21{,}600\text{s}$) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **`google.com/tpu`** *(bottleneck)* | 32 | `0.06` `google.com/tpu`-h | `6.5s` | n/a ($T_{\text{test}}=0$) | **0.03%** |
| `cpu` | 128 cores | `0.12` core-h | `3.2s` | n/a ($T_{\text{test}}=0$) | 0.02% |
| `memory` | 512 GiB | `0.46` GiB-h | `3.2s` | n/a ($T_{\text{test}}=0$) | 0.02% |

#### Part 3 — Recommended `testrunner.py` Settings for `100%` Saturation in `5m` (`300s`) & `10m` (`600s`)

| Target Wall-Clock Window ($T_{\text{target}}$) | Bottleneck Resource ($r^*$) | Recommended `--speedup` ($N_{100\%}$) | Recommended `--duration` | Compressed Arrival Span ($T_{\text{test}} / N_{100\%}$) | Execution & Drain Behavior |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **`5m` (`300s`)** | `google.com/tpu` | **`--speedup 0.022`** | **`--duration 5m`** | `0.0s` | All arrivals submit within `0.0s` (≤ `300s`); `--duration 5m` captures `100%` of arrivals and `100.0%` full-cluster work in `5m`. |
| **`10m` (`600s`)** | `google.com/tpu` | **`--speedup 0.011`** | **`--duration 10m`** | `0.0s` | All arrivals submit within `0.0s` (≤ `600s`); `--duration 10m` captures `100%` of arrivals and `100.0%` full-cluster work in `10m`. |

```bash
# Run at 100% offered google.com/tpu saturation in a 5-minute (300s) wall-clock window:
python3 deploy/scripts/testrunner.py --scenario testscenarios/self-tests/st64-tas-multi-node-failure-eviction-and-readmit --speedup 0.022 --duration 5m

# Run at 100% offered google.com/tpu saturation in a 10-minute (600s) wall-clock window:
python3 deploy/scripts/testrunner.py --scenario testscenarios/self-tests/st64-tas-multi-node-failure-eviction-and-readmit --speedup 0.011 --duration 10m
```

### 7.1 Workload Breakdown & Cluster Saturation by Priority Class

| Priority Class | Priority Value | Workloads | Share | Total Pods | Gang Size (`min / P50 / P90 / max`) | Duration (`P50 / AVG / P90 / P99`) | Offered `google.com/tpu`-Hours | `google.com/tpu` Saturation | `cpu` Saturation | `memory` Saturation |
| :--- | ---: | ---: | ---: | ---: | :---: | :---: | ---: | ---: | ---: | ---: |
| `p-standard` | `100` | 1 | 100.0% | 4 | `4 / 4 / 4 / 4` | `13s / 13s / 13s / 13s` | 0.1 | n/a | n/a | n/a |
| **ALL (Total)** | — | **1** | 100.0% | 4 | `4 / 4 / 4 / 4` | `13s / 13s / 13s / 13s` | 0.1 | n/a | n/a | n/a |

### 7.2 Workload Dissection per `LocalQueue` (and Target `ClusterQueue`)

| LocalQueue (`ns/name`) | Target `ClusterQueue` | Workloads | Share | Total Pods | Duration (`P50 / AVG / P90 / P99`) | Offered `google.com/tpu`-h | Cluster `google.com/tpu` Saturation | Target CQ Nominal Quota Saturation | Priority Breakdown (Count) |
| :--- | :--- | ---: | ---: | ---: | :---: | ---: | ---: | ---: | :--- |
| `st-workloads/lq-tas-multi` | `cq-tas-multi` | **1** | 100.0% | 4 | `13s / 13s / 13s / 13s` | 0.1 | n/a | n/a | `p-standard`:1 |

#### Detailed Per-`LocalQueue` × Priority Dissection

<details>
<summary><b>LocalQueue <code>st-workloads/lq-tas-multi</code> → ClusterQueue <code>cq-tas-multi</code></b> (1 workloads)</summary>

| Priority Class | Priority Value | Workloads | Share of LQ | Total Pods | Duration (`P50 / AVG / P90 / P99`) | Cluster `google.com/tpu` Saturation | Cluster `cpu` Saturation |
| :--- | ---: | ---: | ---: | ---: | :---: | ---: | ---: |
| `p-standard` | `100` | **1** | 100.0% | 4 | `13s / 13s / 13s / 13s` | n/a | n/a |

</details>

### 7.3 Workload Execution Duration Histograms (Aggregated & Per Priority)

#### Aggregated Duration Histogram (All Unadmitted Workloads)

```text
=== ALL PRIORITIES (Aggregated) | count=1 | min=13s, p50=13s (0.2m), avg=13.0s (0.2m), p90=13s (0.2m), p99=13s (0.2m), max=13s ===
  < 1m   (0-60s)       |     1 (100.0%) | ████████████████████████████████
  1-5m   (60-300s)     |     0 (  0.0%) | 
  5-10m  (300-600s)    |     0 (  0.0%) | 
  10-15m (600-900s)    |     0 (  0.0%) | 
  15-20m (900-1200s)   |     0 (  0.0%) | 
  20-30m (1200-1800s)  |     0 (  0.0%) | 
```

#### Per-Priority Duration Histograms

```text
=== Priority: p-standard (value=100) | count=1 | min=13s, p50=13s (0.2m), avg=13.0s (0.2m), p90=13s (0.2m), p99=13s (0.2m), max=13s ===
  < 1m   (0-60s)       |     1 (100.0%) | ████████████████████████████████
  1-5m   (60-300s)     |     0 (  0.0%) | 
  5-10m  (300-600s)    |     0 (  0.0%) | 
  10-15m (600-900s)    |     0 (  0.0%) | 
  15-20m (900-1200s)   |     0 (  0.0%) | 
  20-30m (1200-1800s)  |     0 (  0.0%) | 
```
