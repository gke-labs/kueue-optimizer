# Test Scenario Summary: `st30-inadmissible-oversized-workload-rejection`

> **Auto-generated scenario analysis** from [`testscenarios/self-tests/st30-inadmissible-oversized-workload-rejection/infra.json`](./infra.json), [`testscenarios/self-tests/st30-inadmissible-oversized-workload-rejection/workflows.json`](./workflows.json), and [`testscenarios/self-tests/st30-inadmissible-oversized-workload-rejection/execution-time.json`](./execution-time.json) via [`deploy/scripts/generate_scenario_readme.py`](../../../deploy/scripts/generate_scenario_readme.py).

## 1. Scenario Overview

| Dimension | Summary |
| :--- | :--- |
| **Scenario Directory** | `testscenarios/self-tests/st30-inadmissible-oversized-workload-rejection` |
| **Raw Event Counts** | `infra.json`: **12** events · `workflows.json`: **6** events · `execution-time.json`: **12** events |
| **Cluster Capacity** | **8 nodes** · `128 cores` · `512 GiB` · `32` `google.com/tpu` (1 distinct node group) |
| **Topology Configuration** | **0** `Topology` CRD(s) (none) |
| **Initial In-Flight Pods at $t_0$** | **0** pods across **0** / 8 nodes (initial `google.com/tpu` utilization: **0.00%**) |
| **Kueue Objects** | **1** `ResourceFlavor`(s) · **0** `Cohort`(s) · **1** `ClusterQueue`(s) · **1** `LocalQueue`(s) · **0** `WorkloadPriorityClass`(es) |
| **Test Arrival Window ($T_{\text{test}}$)** | **1.0s** (`2026-06-01T00:00:00Z` $\rightarrow$ `2026-06-01T00:00:01Z`) |
| **Unadmitted Workloads** | **6** `NotAdmitted` arrivals (36 total pods; gang size min=1, p50=1, p90=16, max=16) + **0** pre-admitted at $t_0$ |
| **Execution Duration Quantiles** | `min=35s`, `P50=35s`, `AVG=35s`, `P90=35s`, `P99=35s`, `max=35s` |
| **1. Saturation (Arrival Window $T_{\text{test}}$)** | `google.com/tpu`: **15750.00%** · `cpu`: **3937.50%** · `memory`: **3937.50%** *(over $T_{\text{test}} = 1.0s$)* |
| **2. Saturation (Reference `6h` / `21,600s` Window)** | `google.com/tpu`: **0.73%** · `cpu`: **0.18%** · `memory`: **0.18%** *(over $T_{6\text{h}} = 21{,}600\text{s}$)* |
| **3. Recommended `100%` Saturation Settings (`5m` & `10m`)** | **5m (`300s`)**: `--speedup 0.525 --duration 5m` · **10m (`600s`)**: `--speedup 0.263 --duration 10m` *(bottleneck: `google.com/tpu`, $T_{100\%} = 157.5s (2.6m)$)* |

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
| `cq-guard` | `*(standalone)*` | `BestEffortFIFO` | `1` | `st-tpu-flavor` | **128 cores** | **512 GiB** | **32** | unbounded (default) | `Never` / `Never` / `Never` |
| **Total Nominal Quota (CQs + Cohort)** | — | — | — | — | **128 cores** (100.0% of cluster) | **512 GiB** (100.0% of cluster) | **32** (100.0% of cluster) | — | — |

### 5.4 Namespaces & LocalQueues (`Namespace` & `LocalQueue`)

- **Namespaces (`1`)**: `st-workloads`

| Namespace | LocalQueue Name | Target `ClusterQueue` (`spec.clusterQueue`) | Target CQ Exists? | Target Cohort | Unadmitted Workloads Submitted |
| :--- | :--- | :--- | :---: | :--- | ---: |
| `st-workloads` | `lq-guard` | `cq-guard` | Yes | `*(standalone)*` | **6** |

---

## 6. Scenario Timeline & Arrival Dynamics

| Timeline Metric | Value |
| :--- | :--- |
| **Infrastructure Bootstrap Timestamp ($t_0$)** | `2026-06-01T00:00:00Z` |
| **First Unadmitted Workload Arrival ($t_{\text{first}}$)** | `2026-06-01T00:00:00Z` (offset `+0.00s` from $t_0$) |
| **Last Unadmitted Workload Arrival ($t_{\text{last}}$)** | `2026-06-01T00:00:01Z` (offset `+1.00s` from $t_0$) |
| **Active Test Arrival Duration ($T_{\text{test}} = t_{\text{last}} - t_{\text{first}}$)** | **1.0s** |
| **Unadmitted Workload Submission Count** | **6** workloads (plus **0** pre-admitted at $t_0$) |
| **Mean Arrival Rate ($\lambda$)** | **360.00 workloads/min** (**21600.0 workloads/hour**) |
| **Inter-Arrival Time ($\Delta t_i$) Distribution** | `min=0.00s`, `P50=0.00s`, `AVG=0.20s`, `P90=0.60s`, `P99=0.96s`, `max=1.00s` |

```text
=== Unadmitted Workload Arrival Distribution Across 12 Equal Time Windows (total=6, span=1.0s) ===
  [   0.0s -    0.1s] |     4 ( 66.7%) | ████████████████████████████████
  [   0.1s -    0.2s] |     0 (  0.0%) | 
  [   0.2s -    0.2s] |     0 (  0.0%) | 
  [   0.2s -    0.3s] |     0 (  0.0%) | 
  [   0.3s -    0.4s] |     0 (  0.0%) | 
  [   0.4s -    0.5s] |     0 (  0.0%) | 
  [   0.5s -    0.6s] |     0 (  0.0%) | 
  [   0.6s -    0.7s] |     0 (  0.0%) | 
  [   0.7s -    0.8s] |     0 (  0.0%) | 
  [   0.8s -    0.8s] |     0 (  0.0%) | 
  [   0.8s -    0.9s] |     0 (  0.0%) | 
  [   0.9s -    1.0s] |     2 ( 33.3%) | ████████████████
```

---

## 7. Workload Priorities, Cluster Saturation, Per-LocalQueue Dissection & Duration Histograms

### 7.0 Three-Part Offered Cluster Saturation & Recommended `5m` / `10m` Replay Settings

For any resource $r \in \{\text{accelerators}, \text{cpu}, \text{memory}\}$ with physical cluster capacity $C_{\text{cluster}}(r)$ and total offered resource-seconds $W(r) = \sum_{w} \left(\text{size}_w(r) \times \text{duration}_w\right)$, let $T_{100\%}(r) = W(r) / C_{\text{cluster}}(r)$ denote the ideal `1.0x` full-cluster service time required at continuous `100%` utilization:

$$\text{Sat}_{\text{window}}(r) = \frac{W(r)}{T_{\text{window}} \times C_{\text{cluster}}(r)} \times 100\% = \frac{T_{100\%}(r)}{T_{\text{window}}} \times 100\%, \qquad N_{100\%}(T_{\text{target}}) = \frac{T_{100\%}(r^*)}{T_{\text{target}}}$$

#### Parts 1 & 2 — Offered Saturation: Active Arrival Window ($T_{\text{test}}$) vs. Default `6h` (`21,600s`) Duration

| Resource ($r$) | Physical Cluster Capacity $C_{\text{cluster}}(r)$ | Total Offered Work ($1.0\text{x}$) | Ideal `100%`-Util Service Time $T_{100\%}(r)$ | (1) Saturation over Arrival Window ($T_{\text{test}} = 1.0s$) | (2) Saturation over Default `6h` ($21{,}600\text{s}$) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **`google.com/tpu`** *(bottleneck)* | 32 | `1.40` `google.com/tpu`-h | `157.5s (2.6m)` | **15750.00%** | **0.73%** |
| `cpu` | 128 cores | `1.40` core-h | `39.4s` | 3937.50% | 0.18% |
| `memory` | 512 GiB | `5.60` GiB-h | `39.4s` | 3937.50% | 0.18% |

#### Part 3 — Recommended `testrunner.py` Settings for `100%` Saturation in `5m` (`300s`) & `10m` (`600s`)

| Target Wall-Clock Window ($T_{\text{target}}$) | Bottleneck Resource ($r^*$) | Recommended `--speedup` ($N_{100\%}$) | Recommended `--duration` | Compressed Arrival Span ($T_{\text{test}} / N_{100\%}$) | Execution & Drain Behavior |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **`5m` (`300s`)** | `google.com/tpu` | **`--speedup 0.525`** | **`--duration 5m`** | `1.9s` | All arrivals submit within `1.9s` (≤ `300s`); `--duration 5m` captures `100%` of arrivals and `100.0%` full-cluster work in `5m`. |
| **`10m` (`600s`)** | `google.com/tpu` | **`--speedup 0.263`** | **`--duration 10m`** | `3.8s` | All arrivals submit within `3.8s` (≤ `600s`); `--duration 10m` captures `100%` of arrivals and `100.0%` full-cluster work in `10m`. |

```bash
# Run at 100% offered google.com/tpu saturation in a 5-minute (300s) wall-clock window:
python3 deploy/scripts/testrunner.py --scenario testscenarios/self-tests/st30-inadmissible-oversized-workload-rejection --speedup 0.525 --duration 5m

# Run at 100% offered google.com/tpu saturation in a 10-minute (600s) wall-clock window:
python3 deploy/scripts/testrunner.py --scenario testscenarios/self-tests/st30-inadmissible-oversized-workload-rejection --speedup 0.263 --duration 10m
```

### 7.1 Workload Breakdown & Cluster Saturation by Priority Class

| Priority Class | Priority Value | Workloads | Share | Total Pods | Gang Size (`min / P50 / P90 / max`) | Duration (`P50 / AVG / P90 / P99`) | Offered `google.com/tpu`-Hours | `google.com/tpu` Saturation | `cpu` Saturation | `memory` Saturation |
| :--- | ---: | ---: | ---: | ---: | :---: | :---: | ---: | ---: | ---: | ---: |
| `<default>` | `0` | 6 | 100.0% | 36 | `1 / 1 / 16 / 16` | `35s / 35s / 35s / 35s` | 1.4 | **15750.00%** | 3937.50% | 3937.50% |
| **ALL (Total)** | — | **6** | 100.0% | 36 | `1 / 1 / 16 / 16` | `35s / 35s / 35s / 35s` | 1.4 | **15750.00%** | **3937.50%** | **3937.50%** |

### 7.2 Workload Dissection per `LocalQueue` (and Target `ClusterQueue`)

| LocalQueue (`ns/name`) | Target `ClusterQueue` | Workloads | Share | Total Pods | Duration (`P50 / AVG / P90 / P99`) | Offered `google.com/tpu`-h | Cluster `google.com/tpu` Saturation | Target CQ Nominal Quota Saturation | Priority Breakdown (Count) |
| :--- | :--- | ---: | ---: | ---: | :---: | ---: | ---: | ---: | :--- |
| `st-workloads/lq-guard` | `cq-guard` | **6** | 100.0% | 36 | `35s / 35s / 35s / 35s` | 1.4 | **15750.00%** | **15750.00%** | `<default>`:6 |

#### Detailed Per-`LocalQueue` × Priority Dissection

<details>
<summary><b>LocalQueue <code>st-workloads/lq-guard</code> → ClusterQueue <code>cq-guard</code></b> (6 workloads)</summary>

| Priority Class | Priority Value | Workloads | Share of LQ | Total Pods | Duration (`P50 / AVG / P90 / P99`) | Cluster `google.com/tpu` Saturation | Cluster `cpu` Saturation |
| :--- | ---: | ---: | ---: | ---: | :---: | ---: | ---: |
| `<default>` | `0` | **6** | 100.0% | 36 | `35s / 35s / 35s / 35s` | 15750.00% | 3937.50% |

</details>

### 7.3 Workload Execution Duration Histograms (Aggregated & Per Priority)

#### Aggregated Duration Histogram (All Unadmitted Workloads)

```text
=== ALL PRIORITIES (Aggregated) | count=6 | min=35s, p50=35s (0.6m), avg=35.0s (0.6m), p90=35s (0.6m), p99=35s (0.6m), max=35s ===
  < 1m   (0-60s)       |     6 (100.0%) | ████████████████████████████████
  1-5m   (60-300s)     |     0 (  0.0%) | 
  5-10m  (300-600s)    |     0 (  0.0%) | 
  10-15m (600-900s)    |     0 (  0.0%) | 
  15-20m (900-1200s)   |     0 (  0.0%) | 
  20-30m (1200-1800s)  |     0 (  0.0%) | 
```

#### Per-Priority Duration Histograms

```text
=== Priority: <default> (value=0) | count=6 | min=35s, p50=35s (0.6m), avg=35.0s (0.6m), p90=35s (0.6m), p99=35s (0.6m), max=35s ===
  < 1m   (0-60s)       |     6 (100.0%) | ████████████████████████████████
  1-5m   (60-300s)     |     0 (  0.0%) | 
  5-10m  (300-600s)    |     0 (  0.0%) | 
  10-15m (600-900s)    |     0 (  0.0%) | 
  15-20m (900-1200s)   |     0 (  0.0%) | 
  20-30m (1200-1800s)  |     0 (  0.0%) | 
```
