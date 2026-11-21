# Test Scenario Summary: `st05-mixed-completed-and-active-at-end`

> **Auto-generated scenario analysis** from [`testscenarios/self-tests/st05-mixed-completed-and-active-at-end/infra.json`](./infra.json), [`testscenarios/self-tests/st05-mixed-completed-and-active-at-end/workflows.json`](./workflows.json), and [`testscenarios/self-tests/st05-mixed-completed-and-active-at-end/execution-time.json`](./execution-time.json) via [`deploy/scripts/generate_scenario_readme.py`](../../../deploy/scripts/generate_scenario_readme.py).

## 1. Scenario Overview

| Dimension | Summary |
| :--- | :--- |
| **Scenario Directory** | `testscenarios/self-tests/st05-mixed-completed-and-active-at-end` |
| **Raw Event Counts** | `infra.json`: **12** events · `workflows.json`: **12** events · `execution-time.json`: **24** events |
| **Cluster Capacity** | **8 nodes** · `128 cores` · `512 GiB` · `32` `google.com/tpu` (1 distinct node group) |
| **Topology Configuration** | **0** `Topology` CRD(s) (none) |
| **Initial In-Flight Pods at $t_0$** | **0** pods across **0** / 8 nodes (initial `google.com/tpu` utilization: **0.00%**) |
| **Kueue Objects** | **1** `ResourceFlavor`(s) · **0** `Cohort`(s) · **1** `ClusterQueue`(s) · **1** `LocalQueue`(s) · **0** `WorkloadPriorityClass`(es) |
| **Test Arrival Window ($T_{\text{test}}$)** | **20.0s** (`2026-06-01T00:00:00Z` $\rightarrow$ `2026-06-01T00:00:20Z`) |
| **Unadmitted Workloads** | **12** `NotAdmitted` arrivals (12 total pods; gang size min=1, p50=1, p90=1, max=1) + **0** pre-admitted at $t_0$ |
| **Execution Duration Quantiles** | `min=18s`, `P50=20s`, `AVG=53s`, `P90=120s (2.0m)`, `P99=120s (2.0m)`, `max=120s (2.0m)` |
| **1. Saturation (Arrival Window $T_{\text{test}}$)** | `google.com/tpu`: **395.00%** · `cpu`: **98.75%** · `memory`: **98.75%** *(over $T_{\text{test}} = 20s$)* |
| **2. Saturation (Reference `6h` / `21,600s` Window)** | `google.com/tpu`: **0.37%** · `cpu`: **0.09%** · `memory`: **0.09%** *(over $T_{6\text{h}} = 21{,}600\text{s}$)* |
| **3. Recommended `100%` Saturation Settings (`5m` & `10m`)** | **5m (`300s`)**: `--speedup 0.263 --duration 5m` · **10m (`600s`)**: `--speedup 0.132 --duration 10m` *(bottleneck: `google.com/tpu`, $T_{100\%} = 79.0s (1.3m)$)* |

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

No `kueue.x-k8s.io/Topology` CRD is defined in `infra.json` (Topology-Aware Scheduling is not explicitly configured via a `Topology` object).

---

## 4. Initial Running Pods & Initial Cluster Utilization at $t_0$

- **Initial Running Pods in `infra.json`**: **`0`** (the cluster starts completely empty at $t_0 = 2026-06-01T00:00:00Z$).
- **Initial Cluster Utilization**: **`0.00%`** across all resources (`cpu`, `memory`, `pods`, accelerators).

---

## 5. Kueue Setup: ResourceFlavors, Cohorts, ClusterQueues, Namespaces & LocalQueues

### 5.1 ResourceFlavors (`ResourceFlavor`) & Topology Binding

| ResourceFlavor Name | Bound `topologyName` | Node Selector Labels (`spec.nodeLabels`) | Node Taints / Tolerations |
| :--- | :--- | :--- | :--- |
| `st-tpu-flavor` | `*(none)*` | `cloud.google.com/gke-tpu-accelerator=tpu-v7x-st`, `type=kwok` | none |

### 5.2 Workload Priority Classes (`WorkloadPriorityClass`)

*(No explicit `WorkloadPriorityClass` objects defined in `infra.json`; all workloads run at default priority unless annotated.)*

### 5.3 Cohorts & ClusterQueues — Quotas, Preemption & Fair Sharing

#### ClusterQueue Quotas, Preemption & Fair-Sharing Configuration

| ClusterQueue | Cohort | Queueing Strategy | FS Weight | Flavors | Nominal `cpu` | Nominal `memory` | Nominal `google.com/tpu` | Borrowing / Lending Limits | Preemption Policy (`withinCQ` / `reclaim` / `borrow`) |
| :--- | :--- | :--- | :---: | :--- | ---: | ---: | ---: | :--- | :--- |
| `cq-split` | `*(standalone)*` | `BestEffortFIFO` | `1` | `st-tpu-flavor` | **128 cores** | **512 GiB** | **32** | unbounded (default) | `Never` / `Never` / `Never` |
| **Total Nominal Quota (CQs + Cohort)** | — | — | — | — | **128 cores** (100.0% of cluster) | **512 GiB** (100.0% of cluster) | **32** (100.0% of cluster) | — | — |

### 5.4 Namespaces & LocalQueues (`Namespace` & `LocalQueue`)

- **Namespaces (`1`)**: `st-workloads`

| Namespace | LocalQueue Name | Target `ClusterQueue` (`spec.clusterQueue`) | Target CQ Exists? | Target Cohort | Unadmitted Workloads Submitted |
| :--- | :--- | :--- | :---: | :--- | ---: |
| `st-workloads` | `lq-split` | `cq-split` | Yes | `*(standalone)*` | **12** |

---

## 6. Scenario Timeline & Arrival Dynamics

| Timeline Metric | Value |
| :--- | :--- |
| **Infrastructure Bootstrap Timestamp ($t_0$)** | `2026-06-01T00:00:00Z` |
| **First Unadmitted Workload Arrival ($t_{\text{first}}$)** | `2026-06-01T00:00:00Z` (offset `+0.00s` from $t_0$) |
| **Last Unadmitted Workload Arrival ($t_{\text{last}}$)** | `2026-06-01T00:00:20Z` (offset `+20.00s` from $t_0$) |
| **Active Test Arrival Duration ($T_{\text{test}} = t_{\text{last}} - t_{\text{first}}$)** | **20.0s** |
| **Unadmitted Workload Submission Count** | **12** workloads (plus **0** pre-admitted at $t_0$) |
| **Mean Arrival Rate ($\lambda$)** | **36.00 workloads/min** (**2160.0 workloads/hour**) |
| **Inter-Arrival Time ($\Delta t_i$) Distribution** | `min=0.00s`, `P50=0.00s`, `AVG=1.82s`, `P90=0.00s`, `P99=18.00s`, `max=20.00s` |

```text
=== Unadmitted Workload Arrival Distribution Across 12 Equal Time Windows (total=12, span=20.0s) ===
  [   0.0s -    1.7s] |     8 ( 66.7%) | ████████████████████████████████
  [   1.7s -    3.3s] |     0 (  0.0%) | 
  [   3.3s -    5.0s] |     0 (  0.0%) | 
  [   5.0s -    6.7s] |     0 (  0.0%) | 
  [   6.7s -    8.3s] |     0 (  0.0%) | 
  [   8.3s -   10.0s] |     0 (  0.0%) | 
  [  10.0s -   11.7s] |     0 (  0.0%) | 
  [  11.7s -   13.3s] |     0 (  0.0%) | 
  [  13.3s -   15.0s] |     0 (  0.0%) | 
  [  15.0s -   16.7s] |     0 (  0.0%) | 
  [  16.7s -   18.3s] |     0 (  0.0%) | 
  [  18.3s -   20.0s] |     4 ( 33.3%) | ████████████████
```

---

## 7. Workload Priorities, Cluster Saturation, Per-LocalQueue Dissection & Duration Histograms

### 7.0 Three-Part Offered Cluster Saturation & Recommended `5m` / `10m` Replay Settings

For any resource $r \in \{\text{accelerators}, \text{cpu}, \text{memory}\}$ with physical cluster capacity $C_{\text{cluster}}(r)$ and total offered resource-seconds $W(r) = \sum_{w} \left(\text{size}_w(r) \times \text{duration}_w\right)$, let $T_{100\%}(r) = W(r) / C_{\text{cluster}}(r)$ denote the ideal `1.0x` full-cluster service time required at continuous `100%` utilization:

$$\text{Sat}_{\text{window}}(r) = \frac{W(r)}{T_{\text{window}} \times C_{\text{cluster}}(r)} \times 100\% = \frac{T_{100\%}(r)}{T_{\text{window}}} \times 100\%, \qquad N_{100\%}(T_{\text{target}}) = \frac{T_{100\%}(r^*)}{T_{\text{target}}}$$

#### Parts 1 & 2 — Offered Saturation: Active Arrival Window ($T_{\text{test}}$) vs. Default `6h` (`21,600s`) Duration

| Resource ($r$) | Physical Cluster Capacity $C_{\text{cluster}}(r)$ | Total Offered Work ($1.0\text{x}$) | Ideal `100%`-Util Service Time $T_{100\%}(r)$ | (1) Saturation over Arrival Window ($T_{\text{test}} = 20s$) | (2) Saturation over Default `6h` ($21{,}600\text{s}$) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **`google.com/tpu`** *(bottleneck)* | 32 | `0.70` `google.com/tpu`-h | `79.0s (1.3m)` | **395.00%** | **0.37%** |
| `cpu` | 128 cores | `0.70` core-h | `19.8s` | 98.75% | 0.09% |
| `memory` | 512 GiB | `2.81` GiB-h | `19.8s` | 98.75% | 0.09% |

#### Part 3 — Recommended `testrunner.py` Settings for `100%` Saturation in `5m` (`300s`) & `10m` (`600s`)

| Target Wall-Clock Window ($T_{\text{target}}$) | Bottleneck Resource ($r^*$) | Recommended `--speedup` ($N_{100\%}$) | Recommended `--duration` | Compressed Arrival Span ($T_{\text{test}} / N_{100\%}$) | Execution & Drain Behavior |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **`5m` (`300s`)** | `google.com/tpu` | **`--speedup 0.263`** | **`--duration 5m`** | `75.9s` | All arrivals submit within `75.9s` (≤ `300s`); `--duration 5m` captures `100%` of arrivals and `100.0%` full-cluster work in `5m`. |
| **`10m` (`600s`)** | `google.com/tpu` | **`--speedup 0.132`** | **`--duration 10m`** | `151.9s` | All arrivals submit within `151.9s` (≤ `600s`); `--duration 10m` captures `100%` of arrivals and `100.0%` full-cluster work in `10m`. |

```bash
# Run at 100% offered google.com/tpu saturation in a 5-minute (300s) wall-clock window:
python3 deploy/scripts/testrunner.py --scenario testscenarios/self-tests/st05-mixed-completed-and-active-at-end --speedup 0.263 --duration 5m

# Run at 100% offered google.com/tpu saturation in a 10-minute (600s) wall-clock window:
python3 deploy/scripts/testrunner.py --scenario testscenarios/self-tests/st05-mixed-completed-and-active-at-end --speedup 0.132 --duration 10m
```

### 7.1 Workload Breakdown & Cluster Saturation by Priority Class

| Priority Class | Priority Value | Workloads | Share | Total Pods | Gang Size (`min / P50 / P90 / max`) | Duration (`P50 / AVG / P90 / P99`) | Offered `google.com/tpu`-Hours | `google.com/tpu` Saturation | `cpu` Saturation | `memory` Saturation |
| :--- | ---: | ---: | ---: | ---: | :---: | :---: | ---: | ---: | ---: | ---: |
| `<default>` | `0` | 12 | 100.0% | 12 | `1 / 1 / 1 / 1` | `20s / 53s / 120s / 120s` | 0.7 | **395.00%** | 98.75% | 98.75% |
| **ALL (Total)** | — | **12** | 100.0% | 12 | `1 / 1 / 1 / 1` | `20s / 53s / 120s / 120s` | 0.7 | **395.00%** | **98.75%** | **98.75%** |

### 7.2 Workload Dissection per `LocalQueue` (and Target `ClusterQueue`)

| LocalQueue (`ns/name`) | Target `ClusterQueue` | Workloads | Share | Total Pods | Duration (`P50 / AVG / P90 / P99`) | Offered `google.com/tpu`-h | Cluster `google.com/tpu` Saturation | Target CQ Nominal Quota Saturation | Priority Breakdown (Count) |
| :--- | :--- | ---: | ---: | ---: | :---: | ---: | ---: | ---: | :--- |
| `st-workloads/lq-split` | `cq-split` | **12** | 100.0% | 12 | `20s / 53s / 120s / 120s` | 0.7 | **395.00%** | **395.00%** | `<default>`:12 |

#### Detailed Per-`LocalQueue` × Priority Dissection

<details>
<summary><b>LocalQueue <code>st-workloads/lq-split</code> → ClusterQueue <code>cq-split</code></b> (12 workloads)</summary>

| Priority Class | Priority Value | Workloads | Share of LQ | Total Pods | Duration (`P50 / AVG / P90 / P99`) | Cluster `google.com/tpu` Saturation | Cluster `cpu` Saturation |
| :--- | ---: | ---: | ---: | ---: | :---: | ---: | ---: |
| `<default>` | `0` | **12** | 100.0% | 12 | `20s / 53s / 120s / 120s` | 395.00% | 98.75% |

</details>

### 7.3 Workload Execution Duration Histograms (Aggregated & Per Priority)

#### Aggregated Duration Histogram (All Unadmitted Workloads)

```text
=== ALL PRIORITIES (Aggregated) | count=12 | min=18s, p50=20s (0.3m), avg=52.7s (0.9m), p90=120s (2.0m), p99=120s (2.0m), max=120s ===
  < 1m   (0-60s)       |     8 ( 66.7%) | ████████████████████████████████
  1-5m   (60-300s)     |     4 ( 33.3%) | ████████████████
  5-10m  (300-600s)    |     0 (  0.0%) | 
  10-15m (600-900s)    |     0 (  0.0%) | 
  15-20m (900-1200s)   |     0 (  0.0%) | 
  20-30m (1200-1800s)  |     0 (  0.0%) | 
```

#### Per-Priority Duration Histograms

```text
=== Priority: <default> (value=0) | count=12 | min=18s, p50=20s (0.3m), avg=52.7s (0.9m), p90=120s (2.0m), p99=120s (2.0m), max=120s ===
  < 1m   (0-60s)       |     8 ( 66.7%) | ████████████████████████████████
  1-5m   (60-300s)     |     4 ( 33.3%) | ████████████████
  5-10m  (300-600s)    |     0 (  0.0%) | 
  10-15m (600-900s)    |     0 (  0.0%) | 
  15-20m (900-1200s)   |     0 (  0.0%) | 
  20-30m (1200-1800s)  |     0 (  0.0%) | 
```
