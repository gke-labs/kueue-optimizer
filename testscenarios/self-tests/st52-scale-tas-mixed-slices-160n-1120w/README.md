# Test Scenario Summary: `st52-scale-tas-mixed-slices-160n-1120w`

> **Auto-generated scenario analysis** from [`testscenarios/self-tests/st52-scale-tas-mixed-slices-160n-1120w/infra.json`](./infra.json), [`testscenarios/self-tests/st52-scale-tas-mixed-slices-160n-1120w/workflows.json`](./workflows.json), and [`testscenarios/self-tests/st52-scale-tas-mixed-slices-160n-1120w/execution-time.json`](./execution-time.json) via [`deploy/scripts/generate_scenario_readme.py`](../../../deploy/scripts/generate_scenario_readme.py).

## 1. Scenario Overview

| Dimension | Summary |
| :--- | :--- |
| **Scenario Directory** | `testscenarios/self-tests/st52-scale-tas-mixed-slices-160n-1120w` |
| **Raw Event Counts** | `infra.json`: **485** events · `workflows.json`: **1,120** events · `execution-time.json`: **2,240** events |
| **Cluster Capacity** | **160 nodes** · `5,120 cores` · `20,480 GiB (20.0 TiB)` · `1,280` `google.com/tpu` (1 distinct node group) |
| **Topology Configuration** | **1** `Topology` CRD(s) (`st-slice-topology`) |
| **Initial In-Flight Pods at $t_0$** | **160** pods across **160** / 160 nodes (initial `google.com/tpu` utilization: **50.00%**) |
| **Kueue Objects** | **1** `ResourceFlavor`(s) · **0** `Cohort`(s) · **1** `ClusterQueue`(s) · **1** `LocalQueue`(s) · **0** `WorkloadPriorityClass`(es) |
| **Test Arrival Window ($T_{\text{test}}$)** | **12.0s** (`2026-06-01T00:00:01Z` $\rightarrow$ `2026-06-01T00:00:13Z`) |
| **Unadmitted Workloads** | **1,080** `NotAdmitted` arrivals (1,160 total pods; gang size min=1, p50=1, p90=1, max=2) + **40** pre-admitted at $t_0$ |
| **Execution Duration Quantiles** | `min=8.0s`, `P50=8.0s`, `AVG=8.1s`, `P90=8.0s`, `P99=9.0s`, `max=9.0s` |
| **1. Saturation (Arrival Window $T_{\text{test}}$)** | `google.com/tpu`: **245.83%** · `cpu`: **61.46%** · `memory`: **61.46%** *(over $T_{\text{test}} = 12s$)* |
| **2. Saturation (Reference `6h` / `21,600s` Window)** | `google.com/tpu`: **0.14%** · `cpu`: **0.03%** · `memory`: **0.03%** *(over $T_{6\text{h}} = 21{,}600\text{s}$)* |
| **3. Recommended `100%` Saturation Settings (`5m` & `10m`)** | **5m (`300s`)**: `--speedup 0.098 --duration 5m` · **10m (`600s`)**: `--speedup 0.049 --duration 10m` *(bottleneck: `google.com/tpu`, $T_{100\%} = 29.5s$)* |

---

## 2. Cluster Nodes & Distinct Node Groups

The cluster contains **160** `v1/Node` objects partitioned into **1** distinct hardware/label configuration group(s).

### 2.1 Aggregate Physical Cluster Capacity & Allocatable Resources

| Resource | Total Capacity | Total Allocatable | Per-Node Average (Capacity) |
| :--- | ---: | ---: | ---: |
| `nodes` | **160** | **160** | `1` |
| `cpu` | **5,120 cores** | **5,120 cores** | 32 cores |
| `memory` | **20,480 GiB (20.0 TiB)** | **20,480 GiB (20.0 TiB)** | 128 GiB |
| `pods` | **17,600** | **17,600** | 110 |
| `google.com/tpu` | **1,280** | **1,280** | 8 |

### 2.2 Summary of Distinct Node Groups

| Group | Node Count | Share | Sample Node Name | `cpu` / Node | `memory` / Node | `pods` / Node | `google.com/tpu` / Node | Key Identifying Labels |
| :--- | ---: | ---: | :--- | ---: | ---: | ---: | ---: | :--- |
| **Group 1: accel=`tpu-v7x-st`** | **160** | 100.0% | `st-node-000` | `32` | `128Gi` | `110` | 8 | `cloud.google.com/gke-tpu-accelerator=tpu-v7x-st`, `kubernetes.io/role=agent`, `kueue-optimizer.gke.io/node-role=agent`, `type=kwok` |

### 2.3 Sample Node Manifest (YAML) from Each Distinct Group

#### Group 1: accel=`tpu-v7x-st` — Sample Node `st-node-000` (160 nodes in group)

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
| `L1` | `cloud.google.com/gce-topology-block` | **3** | `160 / 160` | `32` | `64` | `53.3` | `64` | Asymmetric (`32..64`) | 2 domains × 64 nodes, 1 domain × 32 nodes |
| `L2` | `cloud.google.com/gke-tpu-partition-4x4x4-id` | **10** | `160 / 160` | `16` | `16` | `16.0` | `16` | Uniform | `b00-c00` (16), `b00-c01` (16), `b00-c02` (16), … (10x `16` nodes) |
| `L3` | `cloud.google.com/gke-tpu-partition-2x4x4-id` | **20** | `160 / 160` | `8` | `8` | `8.0` | `8` | Uniform | `b00-c00-p244-0` (8), `b00-c00-p244-1` (8), `b00-c01-p244-0` (8), … (20x `8` nodes) |
| `L4` | `cloud.google.com/gke-tpu-partition-2x2x4-id` | **40** | `160 / 160` | `4` | `4` | `4.0` | `4` | Uniform | `b00-c00-p224-0` (4), `b00-c00-p224-1` (4), `b00-c00-p224-2` (4), … (40x `4` nodes) |
| `L5` | `cloud.google.com/gke-tpu-partition-2x2x2-id` | **80** | `160 / 160` | `2` | `2` | `2.0` | `2` | Uniform | `b00-c00-p222-0` (2), `b00-c00-p222-1` (2), `b00-c00-p222-2` (2), … (80x `2` nodes) |
| `L6` | `cloud.google.com/gke-tpu-partition-2x2x1-id` | **160** | `160 / 160` | `1` | `1` | `1.0` | `1` | Uniform | `b00-c00-p221-00` (1), `b00-c00-p221-01` (1), `b00-c00-p221-02` (1), … (160x `1` nodes) |
| `L7` | `kubernetes.io/hostname` | **160** | `160 / 160` | `1` | `1` | `1.0` | `1` | Uniform | `st-node-000` (1), `st-node-001` (1), `st-node-002` (1), … (160x `1` nodes) |

#### Hierarchical Tree Branching Summary (`st-slice-topology`)

```text
Cluster Total: 160 nodes
  └── L1 [cloud.google.com/gce-topology-block]: 3 total domains (2 domain(s) of 64 node(s), 1 domain(s) of 32 node(s))
    └── L2 [cloud.google.com/gke-tpu-partition-4x4x4-id]: 10 total domains (10 domain(s) of 16 node(s))
      └── L3 [cloud.google.com/gke-tpu-partition-2x4x4-id]: 20 total domains (20 domain(s) of 8 node(s))
        └── L4 [cloud.google.com/gke-tpu-partition-2x2x4-id]: 40 total domains (40 domain(s) of 4 node(s))
          └── L5 [cloud.google.com/gke-tpu-partition-2x2x2-id]: 80 total domains (80 domain(s) of 2 node(s))
            └── L6 [cloud.google.com/gke-tpu-partition-2x2x1-id]: 160 total domains (160 domain(s) of 1 node(s))
              └── L7 [kubernetes.io/hostname]: 160 total domains (160 domain(s) of 1 node(s))
```

---

## 4. Initial Running Pods & Initial Cluster Utilization at $t_0$

At scenario start ($t_0 = 2026-06-01T00:00:00Z$), **`160`** running `Pod` objects are pre-placed across **`160`** of the **`160`** cluster nodes (`100.0%` of nodes occupied; `0` nodes completely free).
Additionally, `workflows.json` records **`40`** pre-existing `Admitted` `JobSet` warmup workflow object(s) at $t_0$.

### 4.1 Initial Cluster Utilization Breakdown

| Resource | Initial Pods Requested | Cluster Allocatable | Free Allocatable at $t_0$ | Initial Utilization (% Allocatable) | Initial Utilization (% Capacity) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| `cpu` | **640 cores** | 5,120 cores | 4,480 cores | **12.50%** | 12.50% |
| `memory` | **2,560 GiB** | 20,480 GiB (20.0 TiB) | 17,920 GiB (17.5 TiB) | **12.50%** | 12.50% |
| `pods` | **160** | 17,600 | 17,440 | **0.91%** | 0.91% |
| `google.com/tpu` | **640** | 1,280 | 640 | **50.00%** | 50.00% |

### 4.2 Initial Pod Remaining Lifetimes & Request Profiles

- **Pods with Finite Remaining Lifetime (`deletionTime - t0`)**: **160** pods — `min=11s`, `P50=11s`, `AVG=11s`, `P90=11s`, `P99=11s`, `max=11s`
- **Pods per Occupied Node (`160` active nodes)**: `min=1`, `P50=1`, `AVG=1.00`, `P90=1`, `max=1`

| Rank | Initial Pod Resource Request Profile | Pod Count | Share of Initial Pods |
| :---: | :--- | ---: | ---: |
| `1` | `cpu: 4`, `google.com/tpu: 4`, `memory: 16Gi` | **160** | 100.0% |

---

## 5. Kueue Setup: ResourceFlavors, Cohorts, ClusterQueues, Namespaces & LocalQueues

### 5.1 ResourceFlavors (`ResourceFlavor`) & Topology Binding

| ResourceFlavor Name | Bound `topologyName` | Node Selector Labels (`spec.nodeLabels`) | Node Taints / Tolerations |
| :--- | :--- | :--- | :--- |
| `st-tpu-flavor` | `st-slice-topology` | `cloud.google.com/gke-tpu-accelerator=tpu-v7x-st`, `type=kwok` | none |

### 5.2 Workload Priority Classes (`WorkloadPriorityClass`)

*(No explicit `WorkloadPriorityClass` objects defined in `infra.json`; all workloads run at default priority unless annotated.)*

### 5.3 Cohorts & ClusterQueues — Quotas, Preemption & Fair Sharing

#### ClusterQueue Quotas, Preemption & Fair-Sharing Configuration

| ClusterQueue | Cohort | Queueing Strategy | FS Weight | Flavors | Nominal `cpu` | Nominal `memory` | Nominal `google.com/tpu` | Borrowing / Lending Limits | Preemption Policy (`withinCQ` / `reclaim` / `borrow`) |
| :--- | :--- | :--- | :---: | :--- | ---: | ---: | ---: | :--- | :--- |
| `cq-tas-slices` | `*(standalone)*` | `BestEffortFIFO` | `1` | `st-tpu-flavor` | **5,120 cores** | **20,480 GiB (20.0 TiB)** | **1,280** | unbounded (default) | `Never` / `Never` / `Never` |
| **Total Nominal Quota (CQs + Cohort)** | — | — | — | — | **5,120 cores** (100.0% of cluster) | **20,480 GiB (20.0 TiB)** (100.0% of cluster) | **1,280** (100.0% of cluster) | — | — |

### 5.4 Namespaces & LocalQueues (`Namespace` & `LocalQueue`)

- **Namespaces (`1`)**: `st-workloads`

| Namespace | LocalQueue Name | Target `ClusterQueue` (`spec.clusterQueue`) | Target CQ Exists? | Target Cohort | Unadmitted Workloads Submitted |
| :--- | :--- | :--- | :---: | :--- | ---: |
| `st-workloads` | `lq-tas-slices` | `cq-tas-slices` | Yes | `*(standalone)*` | **1,080** |

---

## 6. Scenario Timeline & Arrival Dynamics

| Timeline Metric | Value |
| :--- | :--- |
| **Infrastructure Bootstrap Timestamp ($t_0$)** | `2026-06-01T00:00:00Z` |
| **First Unadmitted Workload Arrival ($t_{\text{first}}$)** | `2026-06-01T00:00:01Z` (offset `+1.00s` from $t_0$) |
| **Last Unadmitted Workload Arrival ($t_{\text{last}}$)** | `2026-06-01T00:00:13Z` (offset `+13.00s` from $t_0$) |
| **Active Test Arrival Duration ($T_{\text{test}} = t_{\text{last}} - t_{\text{first}}$)** | **12.0s** |
| **Unadmitted Workload Submission Count** | **1,080** workloads (plus **40** pre-admitted at $t_0$) |
| **Mean Arrival Rate ($\lambda$)** | **5400.00 workloads/min** (**324000.0 workloads/hour**) |
| **Inter-Arrival Time ($\Delta t_i$) Distribution** | `min=0.00s`, `P50=0.00s`, `AVG=0.01s`, `P90=0.00s`, `P99=0.00s`, `max=4.00s` |

```text
=== Unadmitted Workload Arrival Distribution Across 12 Equal Time Windows (total=1,080, span=12.0s) ===
  [   0.0s -    1.0s] |   250 ( 23.1%) | ████████████████████████████████
  [   1.0s -    2.0s] |    40 (  3.7%) | █████
  [   2.0s -    3.0s] |     0 (  0.0%) | 
  [   3.0s -    4.0s] |     0 (  0.0%) | 
  [   4.0s -    5.0s] |   250 ( 23.1%) | ████████████████████████████████
  [   5.0s -    6.0s] |    40 (  3.7%) | █████
  [   6.0s -    7.0s] |     0 (  0.0%) | 
  [   7.0s -    8.0s] |     0 (  0.0%) | 
  [   8.0s -    9.0s] |   250 ( 23.1%) | ████████████████████████████████
  [   9.0s -   10.0s] |     0 (  0.0%) | 
  [  10.0s -   11.0s] |     0 (  0.0%) | 
  [  11.0s -   12.0s] |   250 ( 23.1%) | ████████████████████████████████
```

---

## 7. Workload Priorities, Cluster Saturation, Per-LocalQueue Dissection & Duration Histograms

### 7.0 Three-Part Offered Cluster Saturation & Recommended `5m` / `10m` Replay Settings

For any resource $r \in \{\text{accelerators}, \text{cpu}, \text{memory}\}$ with physical cluster capacity $C_{\text{cluster}}(r)$ and total offered resource-seconds $W(r) = \sum_{w} \left(\text{size}_w(r) \times \text{duration}_w\right)$, let $T_{100\%}(r) = W(r) / C_{\text{cluster}}(r)$ denote the ideal `1.0x` full-cluster service time required at continuous `100%` utilization:

$$\text{Sat}_{\text{window}}(r) = \frac{W(r)}{T_{\text{window}} \times C_{\text{cluster}}(r)} \times 100\% = \frac{T_{100\%}(r)}{T_{\text{window}}} \times 100\%, \qquad N_{100\%}(T_{\text{target}}) = \frac{T_{100\%}(r^*)}{T_{\text{target}}}$$

#### Parts 1 & 2 — Offered Saturation: Active Arrival Window ($T_{\text{test}}$) vs. Default `6h` (`21,600s`) Duration

| Resource ($r$) | Physical Cluster Capacity $C_{\text{cluster}}(r)$ | Total Offered Work ($1.0\text{x}$) | Ideal `100%`-Util Service Time $T_{100\%}(r)$ | (1) Saturation over Arrival Window ($T_{\text{test}} = 12s$) | (2) Saturation over Default `6h` ($21{,}600\text{s}$) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **`google.com/tpu`** *(bottleneck)* | 1,280 | `10.49` `google.com/tpu`-h | `29.5s` | **245.83%** | **0.14%** |
| `cpu` | 5,120 cores | `10.49` core-h | `7.4s` | 61.46% | 0.03% |
| `memory` | 20,480 GiB (20.0 TiB) | `41.96` GiB-h | `7.4s` | 61.46% | 0.03% |

#### Part 3 — Recommended `testrunner.py` Settings for `100%` Saturation in `5m` (`300s`) & `10m` (`600s`)

| Target Wall-Clock Window ($T_{\text{target}}$) | Bottleneck Resource ($r^*$) | Recommended `--speedup` ($N_{100\%}$) | Recommended `--duration` | Compressed Arrival Span ($T_{\text{test}} / N_{100\%}$) | Execution & Drain Behavior |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **`5m` (`300s`)** | `google.com/tpu` | **`--speedup 0.098`** | **`--duration 5m`** | `122.0s` | All arrivals submit within `122.0s` (≤ `300s`); `--duration 5m` captures `100%` of arrivals and `100.0%` full-cluster work in `5m`. |
| **`10m` (`600s`)** | `google.com/tpu` | **`--speedup 0.049`** | **`--duration 10m`** | `244.1s` | All arrivals submit within `244.1s` (≤ `600s`); `--duration 10m` captures `100%` of arrivals and `100.0%` full-cluster work in `10m`. |

```bash
# Run at 100% offered google.com/tpu saturation in a 5-minute (300s) wall-clock window:
python3 deploy/scripts/testrunner.py --scenario testscenarios/self-tests/st52-scale-tas-mixed-slices-160n-1120w --speedup 0.098 --duration 5m

# Run at 100% offered google.com/tpu saturation in a 10-minute (600s) wall-clock window:
python3 deploy/scripts/testrunner.py --scenario testscenarios/self-tests/st52-scale-tas-mixed-slices-160n-1120w --speedup 0.049 --duration 10m
```

### 7.1 Workload Breakdown & Cluster Saturation by Priority Class

| Priority Class | Priority Value | Workloads | Share | Total Pods | Gang Size (`min / P50 / P90 / max`) | Duration (`P50 / AVG / P90 / P99`) | Offered `google.com/tpu`-Hours | `google.com/tpu` Saturation | `cpu` Saturation | `memory` Saturation |
| :--- | ---: | ---: | ---: | ---: | :---: | :---: | ---: | ---: | ---: | ---: |
| `<default>` | `0` | 1,080 | 100.0% | 1,160 | `1 / 1 / 1 / 2` | `8s / 8s / 8s / 9s` | 10.5 | **245.83%** | 61.46% | 61.46% |
| **ALL (Total)** | — | **1,080** | 100.0% | 1,160 | `1 / 1 / 1 / 2` | `8s / 8s / 8s / 9s` | 10.5 | **245.83%** | **61.46%** | **61.46%** |

### 7.2 Workload Dissection per `LocalQueue` (and Target `ClusterQueue`)

| LocalQueue (`ns/name`) | Target `ClusterQueue` | Workloads | Share | Total Pods | Duration (`P50 / AVG / P90 / P99`) | Offered `google.com/tpu`-h | Cluster `google.com/tpu` Saturation | Target CQ Nominal Quota Saturation | Priority Breakdown (Count) |
| :--- | :--- | ---: | ---: | ---: | :---: | ---: | ---: | ---: | :--- |
| `st-workloads/lq-tas-slices` | `cq-tas-slices` | **1,080** | 100.0% | 1,160 | `8s / 8s / 8s / 9s` | 10.5 | **245.83%** | **245.83%** | `<default>`:1080 |

#### Detailed Per-`LocalQueue` × Priority Dissection

<details>
<summary><b>LocalQueue <code>st-workloads/lq-tas-slices</code> → ClusterQueue <code>cq-tas-slices</code></b> (1,080 workloads)</summary>

| Priority Class | Priority Value | Workloads | Share of LQ | Total Pods | Duration (`P50 / AVG / P90 / P99`) | Cluster `google.com/tpu` Saturation | Cluster `cpu` Saturation |
| :--- | ---: | ---: | ---: | ---: | :---: | ---: | ---: |
| `<default>` | `0` | **1,080** | 100.0% | 1,160 | `8s / 8s / 8s / 9s` | 245.83% | 61.46% |

</details>

### 7.3 Workload Execution Duration Histograms (Aggregated & Per Priority)

#### Aggregated Duration Histogram (All Unadmitted Workloads)

```text
=== ALL PRIORITIES (Aggregated) | count=1,080 | min=8s, p50=8s (0.1m), avg=8.1s (0.1m), p90=8s (0.1m), p99=9s (0.1m), max=9s ===
  < 1m   (0-60s)       | 1,080 (100.0%) | ████████████████████████████████
  1-5m   (60-300s)     |     0 (  0.0%) | 
  5-10m  (300-600s)    |     0 (  0.0%) | 
  10-15m (600-900s)    |     0 (  0.0%) | 
  15-20m (900-1200s)   |     0 (  0.0%) | 
  20-30m (1200-1800s)  |     0 (  0.0%) | 
```

#### Per-Priority Duration Histograms

```text
=== Priority: <default> (value=0) | count=1,080 | min=8s, p50=8s (0.1m), avg=8.1s (0.1m), p90=8s (0.1m), p99=9s (0.1m), max=9s ===
  < 1m   (0-60s)       | 1,080 (100.0%) | ████████████████████████████████
  1-5m   (60-300s)     |     0 (  0.0%) | 
  5-10m  (300-600s)    |     0 (  0.0%) | 
  10-15m (600-900s)    |     0 (  0.0%) | 
  15-20m (900-1200s)   |     0 (  0.0%) | 
  20-30m (1200-1800s)  |     0 (  0.0%) | 
```
