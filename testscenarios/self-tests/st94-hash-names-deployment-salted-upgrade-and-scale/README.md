# Test Scenario Summary: `st94-hash-names-deployment-salted-upgrade-and-scale`

> **Auto-generated scenario analysis** from [`testscenarios/self-tests/st94-hash-names-deployment-salted-upgrade-and-scale/infra.json`](./infra.json), [`testscenarios/self-tests/st94-hash-names-deployment-salted-upgrade-and-scale/workflows.json`](./workflows.json), and [`testscenarios/self-tests/st94-hash-names-deployment-salted-upgrade-and-scale/execution-time.json`](./execution-time.json) via [`deploy/scripts/generate_scenario_readme.py`](../../../deploy/scripts/generate_scenario_readme.py).

## 1. Scenario Overview

| Dimension | Summary |
| :--- | :--- |
| **Scenario Directory** | `testscenarios/self-tests/st94-hash-names-deployment-salted-upgrade-and-scale` |
| **Raw Event Counts** | `infra.json`: **9** events · `workflows.json`: **5** events · `execution-time.json`: **2** events |
| **Cluster Capacity** | **4 nodes** · `64 cores` · `256 GiB` · `16` `google.com/tpu` (1 distinct node group) |
| **Topology Configuration** | **0** `Topology` CRD(s) (none) |
| **Initial In-Flight Pods at $t_0$** | **0** pods across **0** / 4 nodes (initial `google.com/tpu` utilization: **0.00%**) |
| **Kueue Objects** | **1** `ResourceFlavor`(s) · **0** `Cohort`(s) · **1** `ClusterQueue`(s) · **1** `LocalQueue`(s) · **1** `WorkloadPriorityClass`(es) |
| **Test Arrival Window ($T_{\text{test}}$)** | **16.0s** (`2026-06-01T00:00:00Z` $\rightarrow$ `2026-06-01T00:00:16Z`) |
| **Unadmitted Workloads** | **2** `NotAdmitted` arrivals (5 total pods; gang size min=1, p50=2, p90=3, max=4) + **0** pre-admitted at $t_0$ |
| **Execution Duration Quantiles** | `min=6.0s`, `P50=8.0s`, `AVG=8.0s`, `P90=9.6s`, `P99=10.0s`, `max=10s` |
| **1. Saturation (Arrival Window $T_{\text{test}}$)** | `google.com/tpu`: **40.62%** · `cpu`: **20.31%** · `memory`: **20.31%** *(over $T_{\text{test}} = 16s$)* |
| **2. Saturation (Reference `6h` / `21,600s` Window)** | `google.com/tpu`: **0.03%** · `cpu`: **0.02%** · `memory`: **0.02%** *(over $T_{6\text{h}} = 21{,}600\text{s}$)* |
| **3. Recommended `100%` Saturation Settings (`5m` & `10m`)** | **5m (`300s`)**: `--speedup 0.022 --duration 5m` · **10m (`600s`)**: `--speedup 0.011 --duration 10m` *(bottleneck: `google.com/tpu`, $T_{100\%} = 6.5s$)* |

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

| Priority Class Name | Priority `value` | API Version | Description |
| :--- | ---: | :--- | :--- |
| `p-std` | **`500`** | `kueue.x-k8s.io/v1beta2` | — |

### 5.3 Cohorts & ClusterQueues — Quotas, Preemption & Fair Sharing

#### ClusterQueue Quotas, Preemption & Fair-Sharing Configuration

| ClusterQueue | Cohort | Queueing Strategy | FS Weight | Flavors | Nominal `cpu` | Nominal `memory` | Nominal `google.com/tpu` | Borrowing / Lending Limits | Preemption Policy (`withinCQ` / `reclaim` / `borrow`) |
| :--- | :--- | :--- | :---: | :--- | ---: | ---: | ---: | :--- | :--- |
| `cq-hash-dep` | `*(standalone)*` | `StrictFIFO` | `1` | `st-tpu-flavor` | **32 cores** | **128 GiB** | **8** | unbounded (default) | `Never` / `Never` / `Never` |
| **Total Nominal Quota (CQs + Cohort)** | — | — | — | — | **32 cores** (50.0% of cluster) | **128 GiB** (50.0% of cluster) | **8** (50.0% of cluster) | — | — |

### 5.4 Namespaces & LocalQueues (`Namespace` & `LocalQueue`)

- **Namespaces (`1`)**: `st-workloads`

| Namespace | LocalQueue Name | Target `ClusterQueue` (`spec.clusterQueue`) | Target CQ Exists? | Target Cohort | Unadmitted Workloads Submitted |
| :--- | :--- | :--- | :---: | :--- | ---: |
| `st-workloads` | `lq-hash-dep` | `cq-hash-dep` | Yes | `*(standalone)*` | **2** |

---

## 6. Scenario Timeline & Arrival Dynamics

| Timeline Metric | Value |
| :--- | :--- |
| **Infrastructure Bootstrap Timestamp ($t_0$)** | `2026-06-01T00:00:00Z` |
| **First Unadmitted Workload Arrival ($t_{\text{first}}$)** | `2026-06-01T00:00:00Z` (offset `+0.00s` from $t_0$) |
| **Last Unadmitted Workload Arrival ($t_{\text{last}}$)** | `2026-06-01T00:00:16Z` (offset `+16.00s` from $t_0$) |
| **Active Test Arrival Duration ($T_{\text{test}} = t_{\text{last}} - t_{\text{first}}$)** | **16.0s** |
| **Unadmitted Workload Submission Count** | **2** workloads (plus **0** pre-admitted at $t_0$) |
| **Mean Arrival Rate ($\lambda$)** | **7.50 workloads/min** (**450.0 workloads/hour**) |
| **Inter-Arrival Time ($\Delta t_i$) Distribution** | `min=16.00s`, `P50=16.00s`, `AVG=16.00s`, `P90=16.00s`, `P99=16.00s`, `max=16.00s` |

```text
=== Unadmitted Workload Arrival Distribution Across 12 Equal Time Windows (total=2, span=16.0s) ===
  [   0.0s -    1.3s] |     1 ( 50.0%) | ████████████████████████████████
  [   1.3s -    2.7s] |     0 (  0.0%) | 
  [   2.7s -    4.0s] |     0 (  0.0%) | 
  [   4.0s -    5.3s] |     0 (  0.0%) | 
  [   5.3s -    6.7s] |     0 (  0.0%) | 
  [   6.7s -    8.0s] |     0 (  0.0%) | 
  [   8.0s -    9.3s] |     0 (  0.0%) | 
  [   9.3s -   10.7s] |     0 (  0.0%) | 
  [  10.7s -   12.0s] |     0 (  0.0%) | 
  [  12.0s -   13.3s] |     0 (  0.0%) | 
  [  13.3s -   14.7s] |     0 (  0.0%) | 
  [  14.7s -   16.0s] |     1 ( 50.0%) | ████████████████████████████████
```

---

## 7. Workload Priorities, Cluster Saturation, Per-LocalQueue Dissection & Duration Histograms

### 7.0 Three-Part Offered Cluster Saturation & Recommended `5m` / `10m` Replay Settings

For any resource $r \in \{\text{accelerators}, \text{cpu}, \text{memory}\}$ with physical cluster capacity $C_{\text{cluster}}(r)$ and total offered resource-seconds $W(r) = \sum_{w} \left(\text{size}_w(r) \times \text{duration}_w\right)$, let $T_{100\%}(r) = W(r) / C_{\text{cluster}}(r)$ denote the ideal `1.0x` full-cluster service time required at continuous `100%` utilization:

$$\text{Sat}_{\text{window}}(r) = \frac{W(r)}{T_{\text{window}} \times C_{\text{cluster}}(r)} \times 100\% = \frac{T_{100\%}(r)}{T_{\text{window}}} \times 100\%, \qquad N_{100\%}(T_{\text{target}}) = \frac{T_{100\%}(r^*)}{T_{\text{target}}}$$

#### Parts 1 & 2 — Offered Saturation: Active Arrival Window ($T_{\text{test}}$) vs. Default `6h` (`21,600s`) Duration

| Resource ($r$) | Physical Cluster Capacity $C_{\text{cluster}}(r)$ | Total Offered Work ($1.0\text{x}$) | Ideal `100%`-Util Service Time $T_{100\%}(r)$ | (1) Saturation over Arrival Window ($T_{\text{test}} = 16s$) | (2) Saturation over Default `6h` ($21{,}600\text{s}$) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **`google.com/tpu`** *(bottleneck)* | 16 | `0.03` `google.com/tpu`-h | `6.5s` | **40.62%** | **0.03%** |
| `cpu` | 64 cores | `0.06` core-h | `3.2s` | 20.31% | 0.02% |
| `memory` | 256 GiB | `0.23` GiB-h | `3.2s` | 20.31% | 0.02% |

#### Part 3 — Recommended `testrunner.py` Settings for `100%` Saturation in `5m` (`300s`) & `10m` (`600s`)

| Target Wall-Clock Window ($T_{\text{target}}$) | Bottleneck Resource ($r^*$) | Recommended `--speedup` ($N_{100\%}$) | Recommended `--duration` | Compressed Arrival Span ($T_{\text{test}} / N_{100\%}$) | Execution & Drain Behavior |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **`5m` (`300s`)** | `google.com/tpu` | **`--speedup 0.022`** | **`--duration 5m`** | `738.5s` | Offered work equals `100.0%` of `5m` (`300s`), while compressed arrivals span `738.5s` (since arrival-window saturation < `100%`); use `--duration 5m` for a hard `5m` wall-clock window or `--duration 1108s` to admit all late arrivals. |
| **`10m` (`600s`)** | `google.com/tpu` | **`--speedup 0.011`** | **`--duration 10m`** | `1476.9s` | Offered work equals `100.0%` of `10m` (`600s`), while compressed arrivals span `1476.9s` (since arrival-window saturation < `100%`); use `--duration 10m` for a hard `10m` wall-clock window or `--duration 2216s` to admit all late arrivals. |

```bash
# Run at 100% offered google.com/tpu saturation in a 5-minute (300s) wall-clock window:
python3 deploy/scripts/testrunner.py --scenario testscenarios/self-tests/st94-hash-names-deployment-salted-upgrade-and-scale --speedup 0.022 --duration 5m

# Run at 100% offered google.com/tpu saturation in a 10-minute (600s) wall-clock window:
python3 deploy/scripts/testrunner.py --scenario testscenarios/self-tests/st94-hash-names-deployment-salted-upgrade-and-scale --speedup 0.011 --duration 10m
```

### 7.1 Workload Breakdown & Cluster Saturation by Priority Class

| Priority Class | Priority Value | Workloads | Share | Total Pods | Gang Size (`min / P50 / P90 / max`) | Duration (`P50 / AVG / P90 / P99`) | Offered `google.com/tpu`-Hours | `google.com/tpu` Saturation | `cpu` Saturation | `memory` Saturation |
| :--- | ---: | ---: | ---: | ---: | :---: | :---: | ---: | ---: | ---: | ---: |
| `p-std` | `500` | 2 | 100.0% | 5 | `1 / 2 / 3 / 4` | `8s / 8s / 10s / 10s` | 0.0 | **40.62%** | 20.31% | 20.31% |
| **ALL (Total)** | — | **2** | 100.0% | 5 | `1 / 2 / 3 / 4` | `8s / 8s / 10s / 10s` | 0.0 | **40.62%** | **20.31%** | **20.31%** |

### 7.2 Workload Dissection per `LocalQueue` (and Target `ClusterQueue`)

| LocalQueue (`ns/name`) | Target `ClusterQueue` | Workloads | Share | Total Pods | Duration (`P50 / AVG / P90 / P99`) | Offered `google.com/tpu`-h | Cluster `google.com/tpu` Saturation | Target CQ Nominal Quota Saturation | Priority Breakdown (Count) |
| :--- | :--- | ---: | ---: | ---: | :---: | ---: | ---: | ---: | :--- |
| `st-workloads/lq-hash-dep` | `cq-hash-dep` | **2** | 100.0% | 5 | `8s / 8s / 10s / 10s` | 0.0 | **40.62%** | **81.25%** | `p-std`:2 |

#### Detailed Per-`LocalQueue` × Priority Dissection

<details>
<summary><b>LocalQueue <code>st-workloads/lq-hash-dep</code> → ClusterQueue <code>cq-hash-dep</code></b> (2 workloads)</summary>

| Priority Class | Priority Value | Workloads | Share of LQ | Total Pods | Duration (`P50 / AVG / P90 / P99`) | Cluster `google.com/tpu` Saturation | Cluster `cpu` Saturation |
| :--- | ---: | ---: | ---: | ---: | :---: | ---: | ---: |
| `p-std` | `500` | **2** | 100.0% | 5 | `8s / 8s / 10s / 10s` | 40.62% | 20.31% |

</details>

### 7.3 Workload Execution Duration Histograms (Aggregated & Per Priority)

#### Aggregated Duration Histogram (All Unadmitted Workloads)

```text
=== ALL PRIORITIES (Aggregated) | count=2 | min=6s, p50=8s (0.1m), avg=8.0s (0.1m), p90=10s (0.2m), p99=10s (0.2m), max=10s ===
  < 1m   (0-60s)       |     2 (100.0%) | ████████████████████████████████
  1-5m   (60-300s)     |     0 (  0.0%) | 
  5-10m  (300-600s)    |     0 (  0.0%) | 
  10-15m (600-900s)    |     0 (  0.0%) | 
  15-20m (900-1200s)   |     0 (  0.0%) | 
  20-30m (1200-1800s)  |     0 (  0.0%) | 
```

#### Per-Priority Duration Histograms

```text
=== Priority: p-std (value=500) | count=2 | min=6s, p50=8s (0.1m), avg=8.0s (0.1m), p90=10s (0.2m), p99=10s (0.2m), max=10s ===
  < 1m   (0-60s)       |     2 (100.0%) | ████████████████████████████████
  1-5m   (60-300s)     |     0 (  0.0%) | 
  5-10m  (300-600s)    |     0 (  0.0%) | 
  10-15m (600-900s)    |     0 (  0.0%) | 
  15-20m (900-1200s)   |     0 (  0.0%) | 
  20-30m (1200-1800s)  |     0 (  0.0%) | 
```
