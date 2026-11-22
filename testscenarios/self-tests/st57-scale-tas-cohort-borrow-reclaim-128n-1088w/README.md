# Test Scenario Summary: `st57-scale-tas-cohort-borrow-reclaim-128n-1088w`

> **Auto-generated scenario analysis** from [`testscenarios/self-tests/st57-scale-tas-cohort-borrow-reclaim-128n-1088w/infra.json`](./infra.json), [`testscenarios/self-tests/st57-scale-tas-cohort-borrow-reclaim-128n-1088w/workflows.json`](./workflows.json), and [`testscenarios/self-tests/st57-scale-tas-cohort-borrow-reclaim-128n-1088w/execution-time.json`](./execution-time.json) via [`deploy/scripts/generate_scenario_readme.py`](../../../deploy/scripts/generate_scenario_readme.py).

## 1. Scenario Overview

| Dimension | Summary |
| :--- | :--- |
| **Scenario Directory** | `testscenarios/self-tests/st57-scale-tas-cohort-borrow-reclaim-128n-1088w` |
| **Raw Event Counts** | `infra.json`: **392** events · `workflows.json`: **1,088** events · `execution-time.json`: **2,176** events |
| **Cluster Capacity** | **128 nodes** · `4,096 cores` · `16,384 GiB (16.0 TiB)` · `1,024` `google.com/tpu` (1 distinct node group) |
| **Topology Configuration** | **1** `Topology` CRD(s) (`st-slice-topology`) |
| **Initial In-Flight Pods at $t_0$** | **128** pods across **128** / 128 nodes (initial `google.com/tpu` utilization: **100.00%**) |
| **Kueue Objects** | **1** `ResourceFlavor`(s) · **1** `Cohort`(s) · **2** `ClusterQueue`(s) · **2** `LocalQueue`(s) · **0** `WorkloadPriorityClass`(es) |
| **Test Arrival Window ($T_{\text{test}}$)** | **17.0s** (`2026-06-01T00:00:03Z` $\rightarrow$ `2026-06-01T00:00:20Z`) |
| **Unadmitted Workloads** | **960** `NotAdmitted` arrivals (960 total pods; gang size min=1, p50=1, p90=1, max=1) + **128** pre-admitted at $t_0$ |
| **Execution Duration Quantiles** | `min=8.0s`, `P50=8.0s`, `AVG=8.1s`, `P90=8.0s`, `P99=10s`, `max=10s` |
| **1. Saturation (Arrival Window $T_{\text{test}}$)** | `google.com/tpu`: **194.12%** · `cpu`: **48.53%** · `memory`: **48.53%** *(over $T_{\text{test}} = 17s$)* |
| **2. Saturation (Reference `6h` / `21,600s` Window)** | `google.com/tpu`: **0.15%** · `cpu`: **0.04%** · `memory`: **0.04%** *(over $T_{6\text{h}} = 21{,}600\text{s}$)* |
| **3. Recommended `100%` Saturation Settings (`5m` & `10m`)** | **5m (`300s`)**: `--speedup 0.110 --duration 5m` · **10m (`600s`)**: `--speedup 0.055 --duration 10m` *(bottleneck: `google.com/tpu`, $T_{100\%} = 33.0s$)* |

---

## 2. Cluster Nodes & Distinct Node Groups

The cluster contains **128** `v1/Node` objects partitioned into **1** distinct hardware/label configuration group(s).

### 2.1 Aggregate Physical Cluster Capacity & Allocatable Resources

| Resource | Total Capacity | Total Allocatable | Per-Node Average (Capacity) |
| :--- | ---: | ---: | ---: |
| `nodes` | **128** | **128** | `1` |
| `cpu` | **4,096 cores** | **4,096 cores** | 32 cores |
| `memory` | **16,384 GiB (16.0 TiB)** | **16,384 GiB (16.0 TiB)** | 128 GiB |
| `pods` | **14,080** | **14,080** | 110 |
| `google.com/tpu` | **1,024** | **1,024** | 8 |

### 2.2 Summary of Distinct Node Groups

| Group | Node Count | Share | Sample Node Name | `cpu` / Node | `memory` / Node | `pods` / Node | `google.com/tpu` / Node | Key Identifying Labels |
| :--- | ---: | ---: | :--- | ---: | ---: | ---: | ---: | :--- |
| **Group 1: accel=`tpu-v7x-st`** | **128** | 100.0% | `st-node-000` | `32` | `128Gi` | `110` | 8 | `cloud.google.com/gke-tpu-accelerator=tpu-v7x-st`, `kubernetes.io/role=agent`, `kueue-optimizer.gke.io/node-role=agent`, `type=kwok` |

### 2.3 Sample Node Manifest (YAML) from Each Distinct Group

#### Group 1: accel=`tpu-v7x-st` — Sample Node `st-node-000` (128 nodes in group)

```yaml
apiVersion: v1
kind: Node
metadata:
  name: st-node-000
  uid: b7f6c26f-fbfb-ecef-761a-300700a7187c
  labels:
    type: kwok
    kubernetes.io/arch: amd64
    kubernetes.io/os: linux
    kubernetes.io/role: agent
    node-role.kubernetes.io/agent: ""
    kueue-optimizer.gke.io/node-role: agent
    kubernetes.io/hostname: st-node-000
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
    kueue-optimizer.gke.io/node-cpu: "32"
    kueue-optimizer.gke.io/node-memory: 128Gi
    kueue-optimizer.gke.io/node-pods: "110"
    kueue-optimizer.gke.io/node-accel-resource: google.com/tpu
    kueue-optimizer.gke.io/node-accel-count: "8"
spec: {}
status:
  capacity:
    cpu: "32"
    memory: 128Gi
    pods: "110"
    google.com/tpu: "8"
  allocatable:
    cpu: "32"
    memory: 128Gi
    pods: "110"
    google.com/tpu: "8"
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
| `L1` | `cloud.google.com/gce-topology-block` | **2** | `128 / 128` | `64` | `64` | `64.0` | `64` | Uniform | `st-block-00` (64), `st-block-01` (64) |
| `L2` | `cloud.google.com/gke-tpu-partition-4x4x4-id` | **8** | `128 / 128` | `16` | `16` | `16.0` | `16` | Uniform | `b00-c00` (16), `b00-c01` (16), `b00-c02` (16), … (8x `16` nodes) |
| `L3` | `cloud.google.com/gke-tpu-partition-2x4x4-id` | **16** | `128 / 128` | `8` | `8` | `8.0` | `8` | Uniform | `b00-c00-p244-0` (8), `b00-c00-p244-1` (8), `b00-c01-p244-0` (8), … (16x `8` nodes) |
| `L4` | `cloud.google.com/gke-tpu-partition-2x2x4-id` | **32** | `128 / 128` | `4` | `4` | `4.0` | `4` | Uniform | `b00-c00-p224-0` (4), `b00-c00-p224-1` (4), `b00-c00-p224-2` (4), … (32x `4` nodes) |
| `L5` | `cloud.google.com/gke-tpu-partition-2x2x2-id` | **64** | `128 / 128` | `2` | `2` | `2.0` | `2` | Uniform | `b00-c00-p222-0` (2), `b00-c00-p222-1` (2), `b00-c00-p222-2` (2), … (64x `2` nodes) |
| `L6` | `cloud.google.com/gke-tpu-partition-2x2x1-id` | **128** | `128 / 128` | `1` | `1` | `1.0` | `1` | Uniform | `b00-c00-p221-00` (1), `b00-c00-p221-01` (1), `b00-c00-p221-02` (1), … (128x `1` nodes) |
| `L7` | `kubernetes.io/hostname` | **128** | `128 / 128` | `1` | `1` | `1.0` | `1` | Uniform | `st-node-000` (1), `st-node-001` (1), `st-node-002` (1), … (128x `1` nodes) |

#### Hierarchical Tree Branching Summary (`st-slice-topology`)

```text
Cluster Total: 128 nodes
  └── L1 [cloud.google.com/gce-topology-block]: 2 total domains (2 domain(s) of 64 node(s))
    └── L2 [cloud.google.com/gke-tpu-partition-4x4x4-id]: 8 total domains (8 domain(s) of 16 node(s))
      └── L3 [cloud.google.com/gke-tpu-partition-2x4x4-id]: 16 total domains (16 domain(s) of 8 node(s))
        └── L4 [cloud.google.com/gke-tpu-partition-2x2x4-id]: 32 total domains (32 domain(s) of 4 node(s))
          └── L5 [cloud.google.com/gke-tpu-partition-2x2x2-id]: 64 total domains (64 domain(s) of 2 node(s))
            └── L6 [cloud.google.com/gke-tpu-partition-2x2x1-id]: 128 total domains (128 domain(s) of 1 node(s))
              └── L7 [kubernetes.io/hostname]: 128 total domains (128 domain(s) of 1 node(s))
```

---

## 4. Initial Running Pods & Initial Cluster Utilization at $t_0$

At scenario start ($t_0 = 2026-06-01T00:00:00Z$), **`128`** running `Pod` objects are pre-placed across **`128`** of the **`128`** cluster nodes (`100.0%` of nodes occupied; `0` nodes completely free).
Additionally, `workflows.json` records **`128`** pre-existing `Admitted` `JobSet` warmup workflow object(s) at $t_0$.

### 4.1 Initial Cluster Utilization Breakdown

| Resource | Initial Pods Requested | Cluster Allocatable | Free Allocatable at $t_0$ | Initial Utilization (% Allocatable) | Initial Utilization (% Capacity) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| `cpu` | **1,024 cores** | 4,096 cores | 3,072 cores | **25.00%** | 25.00% |
| `memory` | **4,096 GiB** | 16,384 GiB (16.0 TiB) | 12,288 GiB (12.0 TiB) | **25.00%** | 25.00% |
| `pods` | **128** | 14,080 | 13,952 | **0.91%** | 0.91% |
| `google.com/tpu` | **1,024** | 1,024 | 0 | **100.00%** | 100.00% |

### 4.2 Initial Pod Remaining Lifetimes & Request Profiles

- **Pods with Finite Remaining Lifetime (`deletionTime - t0`)**: **128** pods — `min=25s`, `P50=25s`, `AVG=25s`, `P90=25s`, `P99=25s`, `max=25s`
- **Pods per Occupied Node (`128` active nodes)**: `min=1`, `P50=1`, `AVG=1.00`, `P90=1`, `max=1`

| Rank | Initial Pod Resource Request Profile | Pod Count | Share of Initial Pods |
| :---: | :--- | ---: | ---: |
| `1` | `cpu: 8`, `google.com/tpu: 8`, `memory: 32Gi` | **128** | 100.0% |

---

## 5. Kueue Setup: ResourceFlavors, Cohorts, ClusterQueues, Namespaces & LocalQueues

### 5.1 ResourceFlavors (`ResourceFlavor`) & Topology Binding

| ResourceFlavor Name | Bound `topologyName` | Node Selector Labels (`spec.nodeLabels`) | Node Taints / Tolerations |
| :--- | :--- | :--- | :--- |
| `st-tpu-flavor` | `st-slice-topology` | `cloud.google.com/gke-tpu-accelerator=tpu-v7x-st`, `type=kwok` | none |

### 5.2 Workload Priority Classes (`WorkloadPriorityClass`)

*(No explicit `WorkloadPriorityClass` objects defined in `infra.json`; all workloads run at default priority unless annotated.)*

### 5.3 Cohorts & ClusterQueues — Quotas, Preemption & Fair Sharing

#### Cohort Configuration

| Cohort Name | Explicit CRD in `infra.json` | Parent Cohort | Fair-Sharing Weight | Member ClusterQueues | Direct Cohort Remainder Quota |
| :--- | :---: | :--- | :---: | :---: | :--- |
| `cohort-tas-reclaim` | Yes | `*(root)*` | `1 (default)` | **2** (`cq-tas-borrower`, `cq-tas-lender`) | `cpu`: 0 cores, `memory`: 0 GiB, `google.com/tpu`: 0 |

#### ClusterQueue Quotas, Preemption & Fair-Sharing Configuration

| ClusterQueue | Cohort | Queueing Strategy | FS Weight | Flavors | Nominal `cpu` | Nominal `memory` | Nominal `google.com/tpu` | Borrowing / Lending Limits | Preemption Policy (`withinCQ` / `reclaim` / `borrow`) |
| :--- | :--- | :--- | :---: | :--- | ---: | ---: | ---: | :--- | :--- |
| `cq-tas-borrower` | `cohort-tas-reclaim` | `BestEffortFIFO` | `1` | `st-tpu-flavor` | **2,048 cores** | **8,192 GiB** | **512** | unbounded (default) | `Never` / `Any` / `Never` |
| `cq-tas-lender` | `cohort-tas-reclaim` | `BestEffortFIFO` | `1` | `st-tpu-flavor` | **2,048 cores** | **8,192 GiB** | **512** | unbounded (default) | `Never` / `Any` / `Never` |
| **Total Nominal Quota (CQs + Cohort)** | — | — | — | — | **4,096 cores** (100.0% of cluster) | **16,384 GiB (16.0 TiB)** (100.0% of cluster) | **1,024** (100.0% of cluster) | — | — |

### 5.4 Namespaces & LocalQueues (`Namespace` & `LocalQueue`)

- **Namespaces (`1`)**: `st-workloads`

| Namespace | LocalQueue Name | Target `ClusterQueue` (`spec.clusterQueue`) | Target CQ Exists? | Target Cohort | Unadmitted Workloads Submitted |
| :--- | :--- | :--- | :---: | :--- | ---: |
| `st-workloads` | `lq-tas-borrower` | `cq-tas-borrower` | Yes | `cohort-tas-reclaim` | **448** |
| `st-workloads` | `lq-tas-lender` | `cq-tas-lender` | Yes | `cohort-tas-reclaim` | **512** |

---

## 6. Scenario Timeline & Arrival Dynamics

| Timeline Metric | Value |
| :--- | :--- |
| **Infrastructure Bootstrap Timestamp ($t_0$)** | `2026-06-01T00:00:00Z` |
| **First Unadmitted Workload Arrival ($t_{\text{first}}$)** | `2026-06-01T00:00:03Z` (offset `+3.00s` from $t_0$) |
| **Last Unadmitted Workload Arrival ($t_{\text{last}}$)** | `2026-06-01T00:00:20Z` (offset `+20.00s` from $t_0$) |
| **Active Test Arrival Duration ($T_{\text{test}} = t_{\text{last}} - t_{\text{first}}$)** | **17.0s** |
| **Unadmitted Workload Submission Count** | **960** workloads (plus **128** pre-admitted at $t_0$) |
| **Mean Arrival Rate ($\lambda$)** | **3388.24 workloads/min** (**203294.1 workloads/hour**) |
| **Inter-Arrival Time ($\Delta t_i$) Distribution** | `min=0.00s`, `P50=0.00s`, `AVG=0.02s`, `P90=0.00s`, `P99=0.00s`, `max=7.00s` |

```text
=== Unadmitted Workload Arrival Distribution Across 12 Equal Time Windows (total=960, span=17.0s) ===
  [   0.0s -    1.4s] |    64 (  6.7%) | ███████
  [   1.4s -    2.8s] |     0 (  0.0%) | 
  [   2.8s -    4.2s] |     0 (  0.0%) | 
  [   4.2s -    5.7s] |     0 (  0.0%) | 
  [   5.7s -    7.1s] |   300 ( 31.2%) | ████████████████████████████████
  [   7.1s -    8.5s] |     0 (  0.0%) | 
  [   8.5s -    9.9s] |     0 (  0.0%) | 
  [   9.9s -   11.3s] |     0 (  0.0%) | 
  [  11.3s -   12.8s] |   300 ( 31.2%) | ████████████████████████████████
  [  12.8s -   14.2s] |     0 (  0.0%) | 
  [  14.2s -   15.6s] |     0 (  0.0%) | 
  [  15.6s -   17.0s] |   296 ( 30.8%) | ████████████████████████████████
```

---

## 7. Workload Priorities, Cluster Saturation, Per-LocalQueue Dissection & Duration Histograms

### 7.0 Three-Part Offered Cluster Saturation & Recommended `5m` / `10m` Replay Settings

For any resource $r \in \{\text{accelerators}, \text{cpu}, \text{memory}\}$ with physical cluster capacity $C_{\text{cluster}}(r)$ and total offered resource-seconds $W(r) = \sum_{w} \left(\text{size}_w(r) \times \text{duration}_w\right)$, let $T_{100\%}(r) = W(r) / C_{\text{cluster}}(r)$ denote the ideal `1.0x` full-cluster service time required at continuous `100%` utilization:

$$\text{Sat}_{\text{window}}(r) = \frac{W(r)}{T_{\text{window}} \times C_{\text{cluster}}(r)} \times 100\% = \frac{T_{100\%}(r)}{T_{\text{window}}} \times 100\%, \qquad N_{100\%}(T_{\text{target}}) = \frac{T_{100\%}(r^*)}{T_{\text{target}}}$$

#### Parts 1 & 2 — Offered Saturation: Active Arrival Window ($T_{\text{test}}$) vs. Default `6h` (`21,600s`) Duration

| Resource ($r$) | Physical Cluster Capacity $C_{\text{cluster}}(r)$ | Total Offered Work ($1.0\text{x}$) | Ideal `100%`-Util Service Time $T_{100\%}(r)$ | (1) Saturation over Arrival Window ($T_{\text{test}} = 17s$) | (2) Saturation over Default `6h` ($21{,}600\text{s}$) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **`google.com/tpu`** *(bottleneck)* | 1,024 | `9.39` `google.com/tpu`-h | `33.0s` | **194.12%** | **0.15%** |
| `cpu` | 4,096 cores | `9.39` core-h | `8.2s` | 48.53% | 0.04% |
| `memory` | 16,384 GiB (16.0 TiB) | `37.55` GiB-h | `8.2s` | 48.53% | 0.04% |

#### Part 3 — Recommended `testrunner.py` Settings for `100%` Saturation in `5m` (`300s`) & `10m` (`600s`)

| Target Wall-Clock Window ($T_{\text{target}}$) | Bottleneck Resource ($r^*$) | Recommended `--speedup` ($N_{100\%}$) | Recommended `--duration` | Compressed Arrival Span ($T_{\text{test}} / N_{100\%}$) | Execution & Drain Behavior |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **`5m` (`300s`)** | `google.com/tpu` | **`--speedup 0.110`** | **`--duration 5m`** | `154.5s` | All arrivals submit within `154.5s` (≤ `300s`); `--duration 5m` captures `100%` of arrivals and `100.0%` full-cluster work in `5m`. |
| **`10m` (`600s`)** | `google.com/tpu` | **`--speedup 0.055`** | **`--duration 10m`** | `309.1s` | All arrivals submit within `309.1s` (≤ `600s`); `--duration 10m` captures `100%` of arrivals and `100.0%` full-cluster work in `10m`. |

```bash
# Run at 100% offered google.com/tpu saturation in a 5-minute (300s) wall-clock window:
python3 deploy/scripts/testrunner.py --scenario testscenarios/self-tests/st57-scale-tas-cohort-borrow-reclaim-128n-1088w --speedup 0.110 --duration 5m

# Run at 100% offered google.com/tpu saturation in a 10-minute (600s) wall-clock window:
python3 deploy/scripts/testrunner.py --scenario testscenarios/self-tests/st57-scale-tas-cohort-borrow-reclaim-128n-1088w --speedup 0.055 --duration 10m
```

### 7.1 Workload Breakdown & Cluster Saturation by Priority Class

| Priority Class | Priority Value | Workloads | Share | Total Pods | Gang Size (`min / P50 / P90 / max`) | Duration (`P50 / AVG / P90 / P99`) | Offered `google.com/tpu`-Hours | `google.com/tpu` Saturation | `cpu` Saturation | `memory` Saturation |
| :--- | ---: | ---: | ---: | ---: | :---: | :---: | ---: | ---: | ---: | ---: |
| `<default>` | `0` | 960 | 100.0% | 960 | `1 / 1 / 1 / 1` | `8s / 8s / 8s / 10s` | 9.4 | **194.12%** | 48.53% | 48.53% |
| **ALL (Total)** | — | **960** | 100.0% | 960 | `1 / 1 / 1 / 1` | `8s / 8s / 8s / 10s` | 9.4 | **194.12%** | **48.53%** | **48.53%** |

### 7.2 Workload Dissection per `LocalQueue` (and Target `ClusterQueue`)

| LocalQueue (`ns/name`) | Target `ClusterQueue` | Workloads | Share | Total Pods | Duration (`P50 / AVG / P90 / P99`) | Offered `google.com/tpu`-h | Cluster `google.com/tpu` Saturation | Target CQ Nominal Quota Saturation | Priority Breakdown (Count) |
| :--- | :--- | ---: | ---: | ---: | :---: | ---: | ---: | ---: | :--- |
| `st-workloads/lq-tas-lender` | `cq-tas-lender` | **512** | 53.3% | 512 | `8s / 8s / 10s / 10s` | 5.4 | **111.76%** | **223.53%** | `<default>`:512 |
| `st-workloads/lq-tas-borrower` | `cq-tas-borrower` | **448** | 46.7% | 448 | `8s / 8s / 8s / 8s` | 4.0 | **82.35%** | **164.71%** | `<default>`:448 |

#### Detailed Per-`LocalQueue` × Priority Dissection

<details>
<summary><b>LocalQueue <code>st-workloads/lq-tas-lender</code> → ClusterQueue <code>cq-tas-lender</code></b> (512 workloads)</summary>

| Priority Class | Priority Value | Workloads | Share of LQ | Total Pods | Duration (`P50 / AVG / P90 / P99`) | Cluster `google.com/tpu` Saturation | Cluster `cpu` Saturation |
| :--- | ---: | ---: | ---: | ---: | :---: | ---: | ---: |
| `<default>` | `0` | **512** | 100.0% | 512 | `8s / 8s / 10s / 10s` | 111.76% | 27.94% |

</details>

<details>
<summary><b>LocalQueue <code>st-workloads/lq-tas-borrower</code> → ClusterQueue <code>cq-tas-borrower</code></b> (448 workloads)</summary>

| Priority Class | Priority Value | Workloads | Share of LQ | Total Pods | Duration (`P50 / AVG / P90 / P99`) | Cluster `google.com/tpu` Saturation | Cluster `cpu` Saturation |
| :--- | ---: | ---: | ---: | ---: | :---: | ---: | ---: |
| `<default>` | `0` | **448** | 100.0% | 448 | `8s / 8s / 8s / 8s` | 82.35% | 20.59% |

</details>

### 7.3 Workload Execution Duration Histograms (Aggregated & Per Priority)

#### Aggregated Duration Histogram (All Unadmitted Workloads)

```text
=== ALL PRIORITIES (Aggregated) | count=960 | min=8s, p50=8s (0.1m), avg=8.1s (0.1m), p90=8s (0.1m), p99=10s (0.2m), max=10s ===
  < 1m   (0-60s)       |   960 (100.0%) | ████████████████████████████████
  1-5m   (60-300s)     |     0 (  0.0%) | 
  5-10m  (300-600s)    |     0 (  0.0%) | 
  10-15m (600-900s)    |     0 (  0.0%) | 
  15-20m (900-1200s)   |     0 (  0.0%) | 
  20-30m (1200-1800s)  |     0 (  0.0%) | 
```

#### Per-Priority Duration Histograms

```text
=== Priority: <default> (value=0) | count=960 | min=8s, p50=8s (0.1m), avg=8.1s (0.1m), p90=8s (0.1m), p99=10s (0.2m), max=10s ===
  < 1m   (0-60s)       |   960 (100.0%) | ████████████████████████████████
  1-5m   (60-300s)     |     0 (  0.0%) | 
  5-10m  (300-600s)    |     0 (  0.0%) | 
  10-15m (600-900s)    |     0 (  0.0%) | 
  15-20m (900-1200s)   |     0 (  0.0%) | 
  20-30m (1200-1800s)  |     0 (  0.0%) | 
```
