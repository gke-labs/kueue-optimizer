# Test Scenario Summary: `st95-hash-names-tas-podgroup-annotation-and-preemption`

> **Auto-generated scenario analysis** from [`testscenarios/self-tests/st95-hash-names-tas-podgroup-annotation-and-preemption/infra.json`](./infra.json), [`testscenarios/self-tests/st95-hash-names-tas-podgroup-annotation-and-preemption/workflows.json`](./workflows.json), and [`testscenarios/self-tests/st95-hash-names-tas-podgroup-annotation-and-preemption/execution-time.json`](./execution-time.json) via [`deploy/scripts/generate_scenario_readme.py`](../../../deploy/scripts/generate_scenario_readme.py).

## 1. Scenario Overview

| Dimension | Summary |
| :--- | :--- |
| **Scenario Directory** | `testscenarios/self-tests/st95-hash-names-tas-podgroup-annotation-and-preemption` |
| **Raw Event Counts** | `infra.json`: **13** events · `workflows.json`: **5** events · `execution-time.json`: **10** events |
| **Cluster Capacity** | **4 nodes** · `64 cores` · `256 GiB` · `16` `google.com/tpu` (1 distinct node group) |
| **Topology Configuration** | **1** `Topology` CRD(s) (`st-slice-topology`) |
| **Initial In-Flight Pods at $t_0$** | **2** pods across **2** / 4 nodes (initial `google.com/tpu` utilization: **50.00%**) |
| **Kueue Objects** | **1** `ResourceFlavor`(s) · **0** `Cohort`(s) · **1** `ClusterQueue`(s) · **1** `LocalQueue`(s) · **2** `WorkloadPriorityClass`(es) |
| **Test Arrival Window ($T_{\text{test}}$)** | **4.0s** (`2026-06-01T00:00:00Z` $\rightarrow$ `2026-06-01T00:00:04Z`) |
| **Unadmitted Workloads** | **3** `NotAdmitted` arrivals (3 total pods; gang size min=1, p50=1, p90=1, max=1) + **2** pre-admitted at $t_0$ |
| **Execution Duration Quantiles** | `min=8.0s`, `P50=16s`, `AVG=13s`, `P90=16s`, `P99=16s`, `max=16s` |
| **1. Saturation (Arrival Window $T_{\text{test}}$)** | `google.com/tpu`: **250.00%** · `cpu`: **62.50%** · `memory`: **62.50%** *(over $T_{\text{test}} = 4.0s$)* |
| **2. Saturation (Reference `6h` / `21,600s` Window)** | `google.com/tpu`: **0.05%** · `cpu`: **0.01%** · `memory`: **0.01%** *(over $T_{6\text{h}} = 21{,}600\text{s}$)* |
| **3. Recommended `100%` Saturation Settings (`5m` & `10m`)** | **5m (`300s`)**: `--speedup 0.033 --duration 5m` · **10m (`600s`)**: `--speedup 0.017 --duration 10m` *(bottleneck: `google.com/tpu`, $T_{100\%} = 10.0s$)* |

---

## 2. Cluster Nodes & Distinct Node Groups

The cluster contains **4** `v1/Node` objects partitioned into **1** distinct hardware/label configuration group(s).

### 2.1 Aggregate Physical Cluster Capacity & Allocatable Resources

| Resource | Total Capacity | Total Allocatable | Per-Node Average (Capacity) |
| :--- | ---: | ---: | ---: |
| `nodes` | **4** | **4** | `1` |
| `cpu` | **64 cores** | **64 cores** | 16 cores |
| `memory` | **256 GiB** | **256 GiB** | 64 GiB |
| `pods` | **440** | **440** | 110 |
| `google.com/tpu` | **16** | **16** | 4 |

### 2.2 Summary of Distinct Node Groups

| Group | Node Count | Share | Sample Node Name | `cpu` / Node | `memory` / Node | `pods` / Node | `google.com/tpu` / Node | Key Identifying Labels |
| :--- | ---: | ---: | :--- | ---: | ---: | ---: | ---: | :--- |
| **Group 1: accel=`tpu-v7x-st`** | **4** | 100.0% | `st-node-00` | `16` | `64Gi` | `110` | 4 | `cloud.google.com/gke-tpu-accelerator=tpu-v7x-st`, `kubernetes.io/role=agent`, `kueue-optimizer.gke.io/node-role=agent`, `type=kwok` |

### 2.3 Sample Node Manifest (YAML) from Each Distinct Group

#### Group 1: accel=`tpu-v7x-st` — Sample Node `st-node-00` (4 nodes in group)

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
| `L1` | `cloud.google.com/gce-topology-block` | **1** | `4 / 4` | `4` | `4` | `4.0` | `4` | Uniform | `st-block-00` (4) |
| `L2` | `cloud.google.com/gke-tpu-partition-4x4x4-id` | **1** | `4 / 4` | `4` | `4` | `4.0` | `4` | Uniform | `b00-c00` (4) |
| `L3` | `cloud.google.com/gke-tpu-partition-2x4x4-id` | **1** | `4 / 4` | `4` | `4` | `4.0` | `4` | Uniform | `b00-c00-p244-0` (4) |
| `L4` | `cloud.google.com/gke-tpu-partition-2x2x4-id` | **1** | `4 / 4` | `4` | `4` | `4.0` | `4` | Uniform | `b00-c00-p224-0` (4) |
| `L5` | `cloud.google.com/gke-tpu-partition-2x2x2-id` | **2** | `4 / 4` | `2` | `2` | `2.0` | `2` | Uniform | `b00-c00-p222-0` (2), `b00-c00-p222-1` (2) |
| `L6` | `cloud.google.com/gke-tpu-partition-2x2x1-id` | **4** | `4 / 4` | `1` | `1` | `1.0` | `1` | Uniform | `b00-c00-p221-00` (1), `b00-c00-p221-01` (1), `b00-c00-p221-02` (1), … (4x `1` nodes) |
| `L7` | `kubernetes.io/hostname` | **4** | `4 / 4` | `1` | `1` | `1.0` | `1` | Uniform | `st-node-00` (1), `st-node-01` (1), `st-node-02` (1), … (4x `1` nodes) |

#### Hierarchical Tree Branching Summary (`st-slice-topology`)

```text
Cluster Total: 4 nodes
  └── L1 [cloud.google.com/gce-topology-block]: 1 total domains (1 domain(s) of 4 node(s))
    └── L2 [cloud.google.com/gke-tpu-partition-4x4x4-id]: 1 total domains (1 domain(s) of 4 node(s))
      └── L3 [cloud.google.com/gke-tpu-partition-2x4x4-id]: 1 total domains (1 domain(s) of 4 node(s))
        └── L4 [cloud.google.com/gke-tpu-partition-2x2x4-id]: 1 total domains (1 domain(s) of 4 node(s))
          └── L5 [cloud.google.com/gke-tpu-partition-2x2x2-id]: 2 total domains (2 domain(s) of 2 node(s))
            └── L6 [cloud.google.com/gke-tpu-partition-2x2x1-id]: 4 total domains (4 domain(s) of 1 node(s))
              └── L7 [kubernetes.io/hostname]: 4 total domains (4 domain(s) of 1 node(s))
```

---

## 4. Initial Running Pods & Initial Cluster Utilization at $t_0$

At scenario start ($t_0 = 2026-06-01T00:00:00Z$), **`2`** running `Pod` objects are pre-placed across **`2`** of the **`4`** cluster nodes (`50.0%` of nodes occupied; `2` nodes completely free).
Additionally, `workflows.json` records **`2`** pre-existing `Admitted` `PodGroup` warmup workflow object(s) at $t_0$.

### 4.1 Initial Cluster Utilization Breakdown

| Resource | Initial Pods Requested | Cluster Allocatable | Free Allocatable at $t_0$ | Initial Utilization (% Allocatable) | Initial Utilization (% Capacity) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| `cpu` | **16 cores** | 64 cores | 48 cores | **25.00%** | 25.00% |
| `memory` | **64 GiB** | 256 GiB | 192 GiB | **25.00%** | 25.00% |
| `pods` | **2** | 440 | 438 | **0.45%** | 0.45% |
| `google.com/tpu` | **8** | 16 | 8 | **50.00%** | 50.00% |

### 4.2 Initial Pod Remaining Lifetimes & Request Profiles

- **Pods with No Deletion Event (Run Entire Scenario / Infinite Duration)**: **2** pods
- **Pods per Occupied Node (`2` active nodes)**: `min=1`, `P50=1`, `AVG=1.00`, `P90=1`, `max=1`

| Rank | Initial Pod Resource Request Profile | Pod Count | Share of Initial Pods |
| :---: | :--- | ---: | ---: |
| `1` | `cpu: 8`, `google.com/tpu: 4`, `memory: 32Gi` | **2** | 100.0% |

---

## 5. Kueue Setup: ResourceFlavors, Cohorts, ClusterQueues, Namespaces & LocalQueues

### 5.1 ResourceFlavors (`ResourceFlavor`) & Topology Binding

| ResourceFlavor Name | Bound `topologyName` | Node Selector Labels (`spec.nodeLabels`) | Node Taints / Tolerations |
| :--- | :--- | :--- | :--- |
| `st-tpu-flavor` | `st-slice-topology` | `cloud.google.com/gke-tpu-accelerator=tpu-v7x-st`, `type=kwok` | none |

### 5.2 Workload Priority Classes (`WorkloadPriorityClass`)

| Priority Class Name | Priority `value` | API Version | Description |
| :--- | ---: | :--- | :--- |
| `p-high` | **`1000`** | `kueue.x-k8s.io/v1beta2` | — |
| `p-low` | **`100`** | `kueue.x-k8s.io/v1beta2` | — |

### 5.3 Cohorts & ClusterQueues — Quotas, Preemption & Fair Sharing

#### ClusterQueue Quotas, Preemption & Fair-Sharing Configuration

| ClusterQueue | Cohort | Queueing Strategy | FS Weight | Flavors | Nominal `cpu` | Nominal `memory` | Nominal `google.com/tpu` | Borrowing / Lending Limits | Preemption Policy (`withinCQ` / `reclaim` / `borrow`) |
| :--- | :--- | :--- | :---: | :--- | ---: | ---: | ---: | :--- | :--- |
| `cq-hash-tas` | `*(standalone)*` | `StrictFIFO` | `1` | `st-tpu-flavor` | **64 cores** | **256 GiB** | **16** | unbounded (default) | `LowerPriority` / `Never` / `Never` |
| **Total Nominal Quota (CQs + Cohort)** | — | — | — | — | **64 cores** (100.0% of cluster) | **256 GiB** (100.0% of cluster) | **16** (100.0% of cluster) | — | — |

### 5.4 Namespaces & LocalQueues (`Namespace` & `LocalQueue`)

- **Namespaces (`1`)**: `st-workloads`

| Namespace | LocalQueue Name | Target `ClusterQueue` (`spec.clusterQueue`) | Target CQ Exists? | Target Cohort | Unadmitted Workloads Submitted |
| :--- | :--- | :--- | :---: | :--- | ---: |
| `st-workloads` | `lq-hash-tas` | `cq-hash-tas` | Yes | `*(standalone)*` | **3** |

---

## 6. Scenario Timeline & Arrival Dynamics

| Timeline Metric | Value |
| :--- | :--- |
| **Infrastructure Bootstrap Timestamp ($t_0$)** | `2026-06-01T00:00:00Z` |
| **First Unadmitted Workload Arrival ($t_{\text{first}}$)** | `2026-06-01T00:00:00Z` (offset `+0.00s` from $t_0$) |
| **Last Unadmitted Workload Arrival ($t_{\text{last}}$)** | `2026-06-01T00:00:04Z` (offset `+4.00s` from $t_0$) |
| **Active Test Arrival Duration ($T_{\text{test}} = t_{\text{last}} - t_{\text{first}}$)** | **4.0s** |
| **Unadmitted Workload Submission Count** | **3** workloads (plus **2** pre-admitted at $t_0$) |
| **Mean Arrival Rate ($\lambda$)** | **45.00 workloads/min** (**2700.0 workloads/hour**) |
| **Inter-Arrival Time ($\Delta t_i$) Distribution** | `min=0.10s`, `P50=2.00s`, `AVG=2.00s`, `P90=3.52s`, `P99=3.86s`, `max=3.90s` |

```text
=== Unadmitted Workload Arrival Distribution Across 12 Equal Time Windows (total=3, span=4.0s) ===
  [   0.0s -    0.3s] |     2 ( 66.7%) | ████████████████████████████████
  [   0.3s -    0.7s] |     0 (  0.0%) | 
  [   0.7s -    1.0s] |     0 (  0.0%) | 
  [   1.0s -    1.3s] |     0 (  0.0%) | 
  [   1.3s -    1.7s] |     0 (  0.0%) | 
  [   1.7s -    2.0s] |     0 (  0.0%) | 
  [   2.0s -    2.3s] |     0 (  0.0%) | 
  [   2.3s -    2.7s] |     0 (  0.0%) | 
  [   2.7s -    3.0s] |     0 (  0.0%) | 
  [   3.0s -    3.3s] |     0 (  0.0%) | 
  [   3.3s -    3.7s] |     0 (  0.0%) | 
  [   3.7s -    4.0s] |     1 ( 33.3%) | ████████████████
```

---

## 7. Workload Priorities, Cluster Saturation, Per-LocalQueue Dissection & Duration Histograms

### 7.0 Three-Part Offered Cluster Saturation & Recommended `5m` / `10m` Replay Settings

For any resource $r \in \{\text{accelerators}, \text{cpu}, \text{memory}\}$ with physical cluster capacity $C_{\text{cluster}}(r)$ and total offered resource-seconds $W(r) = \sum_{w} \left(\text{size}_w(r) \times \text{duration}_w\right)$, let $T_{100\%}(r) = W(r) / C_{\text{cluster}}(r)$ denote the ideal `1.0x` full-cluster service time required at continuous `100%` utilization:

$$\text{Sat}_{\text{window}}(r) = \frac{W(r)}{T_{\text{window}} \times C_{\text{cluster}}(r)} \times 100\% = \frac{T_{100\%}(r)}{T_{\text{window}}} \times 100\%, \qquad N_{100\%}(T_{\text{target}}) = \frac{T_{100\%}(r^*)}{T_{\text{target}}}$$

#### Parts 1 & 2 — Offered Saturation: Active Arrival Window ($T_{\text{test}}$) vs. Default `6h` (`21,600s`) Duration

| Resource ($r$) | Physical Cluster Capacity $C_{\text{cluster}}(r)$ | Total Offered Work ($1.0\text{x}$) | Ideal `100%`-Util Service Time $T_{100\%}(r)$ | (1) Saturation over Arrival Window ($T_{\text{test}} = 4.0s$) | (2) Saturation over Default `6h` ($21{,}600\text{s}$) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **`google.com/tpu`** *(bottleneck)* | 16 | `0.04` `google.com/tpu`-h | `10.0s` | **250.00%** | **0.05%** |
| `cpu` | 64 cores | `0.04` core-h | `2.5s` | 62.50% | 0.01% |
| `memory` | 256 GiB | `0.18` GiB-h | `2.5s` | 62.50% | 0.01% |

#### Part 3 — Recommended `testrunner.py` Settings for `100%` Saturation in `5m` (`300s`) & `10m` (`600s`)

| Target Wall-Clock Window ($T_{\text{target}}$) | Bottleneck Resource ($r^*$) | Recommended `--speedup` ($N_{100\%}$) | Recommended `--duration` | Compressed Arrival Span ($T_{\text{test}} / N_{100\%}$) | Execution & Drain Behavior |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **`5m` (`300s`)** | `google.com/tpu` | **`--speedup 0.033`** | **`--duration 5m`** | `120.0s` | All arrivals submit within `120.0s` (≤ `300s`); `--duration 5m` captures `100%` of arrivals and `100.0%` full-cluster work in `5m`. |
| **`10m` (`600s`)** | `google.com/tpu` | **`--speedup 0.017`** | **`--duration 10m`** | `240.0s` | All arrivals submit within `240.0s` (≤ `600s`); `--duration 10m` captures `100%` of arrivals and `100.0%` full-cluster work in `10m`. |

```bash
# Run at 100% offered google.com/tpu saturation in a 5-minute (300s) wall-clock window:
python3 deploy/scripts/testrunner.py --scenario testscenarios/self-tests/st95-hash-names-tas-podgroup-annotation-and-preemption --speedup 0.033 --duration 5m

# Run at 100% offered google.com/tpu saturation in a 10-minute (600s) wall-clock window:
python3 deploy/scripts/testrunner.py --scenario testscenarios/self-tests/st95-hash-names-tas-podgroup-annotation-and-preemption --speedup 0.017 --duration 10m
```

### 7.1 Workload Breakdown & Cluster Saturation by Priority Class

| Priority Class | Priority Value | Workloads | Share | Total Pods | Gang Size (`min / P50 / P90 / max`) | Duration (`P50 / AVG / P90 / P99`) | Offered `google.com/tpu`-Hours | `google.com/tpu` Saturation | `cpu` Saturation | `memory` Saturation |
| :--- | ---: | ---: | ---: | ---: | :---: | :---: | ---: | ---: | ---: | ---: |
| `p-high` | `1000` | 1 | 33.3% | 1 | `1 / 1 / 1 / 1` | `8s / 8s / 8s / 8s` | 0.0 | **50.00%** | 12.50% | 12.50% |
| `p-low` | `100` | 2 | 66.7% | 2 | `1 / 1 / 1 / 1` | `16s / 16s / 16s / 16s` | 0.0 | **200.00%** | 50.00% | 50.00% |
| **ALL (Total)** | — | **3** | 100.0% | 3 | `1 / 1 / 1 / 1` | `16s / 13s / 16s / 16s` | 0.0 | **250.00%** | **62.50%** | **62.50%** |

### 7.2 Workload Dissection per `LocalQueue` (and Target `ClusterQueue`)

| LocalQueue (`ns/name`) | Target `ClusterQueue` | Workloads | Share | Total Pods | Duration (`P50 / AVG / P90 / P99`) | Offered `google.com/tpu`-h | Cluster `google.com/tpu` Saturation | Target CQ Nominal Quota Saturation | Priority Breakdown (Count) |
| :--- | :--- | ---: | ---: | ---: | :---: | ---: | ---: | ---: | :--- |
| `st-workloads/lq-hash-tas` | `cq-hash-tas` | **3** | 100.0% | 3 | `16s / 13s / 16s / 16s` | 0.0 | **250.00%** | **250.00%** | `p-high`:1, `p-low`:2 |

#### Detailed Per-`LocalQueue` × Priority Dissection

<details>
<summary><b>LocalQueue <code>st-workloads/lq-hash-tas</code> → ClusterQueue <code>cq-hash-tas</code></b> (3 workloads)</summary>

| Priority Class | Priority Value | Workloads | Share of LQ | Total Pods | Duration (`P50 / AVG / P90 / P99`) | Cluster `google.com/tpu` Saturation | Cluster `cpu` Saturation |
| :--- | ---: | ---: | ---: | ---: | :---: | ---: | ---: |
| `p-high` | `1000` | **1** | 33.3% | 1 | `8s / 8s / 8s / 8s` | 50.00% | 12.50% |
| `p-low` | `100` | **2** | 66.7% | 2 | `16s / 16s / 16s / 16s` | 200.00% | 50.00% |

</details>

### 7.3 Workload Execution Duration Histograms (Aggregated & Per Priority)

#### Aggregated Duration Histogram (All Unadmitted Workloads)

```text
=== ALL PRIORITIES (Aggregated) | count=3 | min=8s, p50=16s (0.3m), avg=13.3s (0.2m), p90=16s (0.3m), p99=16s (0.3m), max=16s ===
  < 1m   (0-60s)       |     3 (100.0%) | ████████████████████████████████
  1-5m   (60-300s)     |     0 (  0.0%) | 
  5-10m  (300-600s)    |     0 (  0.0%) | 
  10-15m (600-900s)    |     0 (  0.0%) | 
  15-20m (900-1200s)   |     0 (  0.0%) | 
  20-30m (1200-1800s)  |     0 (  0.0%) | 
```

#### Per-Priority Duration Histograms

```text
=== Priority: p-high (value=1000) | count=1 | min=8s, p50=8s (0.1m), avg=8.0s (0.1m), p90=8s (0.1m), p99=8s (0.1m), max=8s ===
  < 1m   (0-60s)       |     1 (100.0%) | ████████████████████████████████
  1-5m   (60-300s)     |     0 (  0.0%) | 
  5-10m  (300-600s)    |     0 (  0.0%) | 
  10-15m (600-900s)    |     0 (  0.0%) | 
  15-20m (900-1200s)   |     0 (  0.0%) | 
  20-30m (1200-1800s)  |     0 (  0.0%) | 

=== Priority: p-low (value=100) | count=2 | min=16s, p50=16s (0.3m), avg=16.0s (0.3m), p90=16s (0.3m), p99=16s (0.3m), max=16s ===
  < 1m   (0-60s)       |     2 (100.0%) | ████████████████████████████████
  1-5m   (60-300s)     |     0 (  0.0%) | 
  5-10m  (300-600s)    |     0 (  0.0%) | 
  10-15m (600-900s)    |     0 (  0.0%) | 
  15-20m (900-1200s)   |     0 (  0.0%) | 
  20-30m (1200-1800s)  |     0 (  0.0%) | 
```
