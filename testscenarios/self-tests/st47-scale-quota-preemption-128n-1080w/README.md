# Test Scenario Summary: `st47-scale-quota-preemption-128n-1080w`

> **Auto-generated scenario analysis** from [`testscenarios/self-tests/st47-scale-quota-preemption-128n-1080w/infra.json`](./infra.json), [`testscenarios/self-tests/st47-scale-quota-preemption-128n-1080w/workflows.json`](./workflows.json), and [`testscenarios/self-tests/st47-scale-quota-preemption-128n-1080w/execution-time.json`](./execution-time.json) via [`deploy/scripts/generate_scenario_readme.py`](../../../deploy/scripts/generate_scenario_readme.py).

## 1. Scenario Overview

| Dimension | Summary |
| :--- | :--- |
| **Scenario Directory** | `testscenarios/self-tests/st47-scale-quota-preemption-128n-1080w` |
| **Raw Event Counts** | `infra.json`: **518** events · `workflows.json`: **1,080** events · `execution-time.json`: **2,160** events |
| **Cluster Capacity** | **128 nodes** · `4,096 cores` · `16,384 GiB (16.0 TiB)` · `1,024` `google.com/tpu` (1 distinct node group) |
| **Topology Configuration** | **0** `Topology` CRD(s) (none) |
| **Initial In-Flight Pods at $t_0$** | **192** pods across **128** / 128 nodes (initial `google.com/tpu` utilization: **100.00%**) |
| **Kueue Objects** | **1** `ResourceFlavor`(s) · **0** `Cohort`(s) · **1** `ClusterQueue`(s) · **1** `LocalQueue`(s) · **2** `WorkloadPriorityClass`(es) |
| **Test Arrival Window ($T_{\text{test}}$)** | **15.0s** (`2026-06-01T00:00:03Z` $\rightarrow$ `2026-06-01T00:00:18Z`) |
| **Unadmitted Workloads** | **888** `NotAdmitted` arrivals (888 total pods; gang size min=1, p50=1, p90=1, max=1) + **192** pre-admitted at $t_0$ |
| **Execution Duration Quantiles** | `min=8.0s`, `P50=8.0s`, `AVG=8.1s`, `P90=8.0s`, `P99=9.0s`, `max=9.0s` |
| **1. Saturation (Arrival Window $T_{\text{test}}$)** | `google.com/tpu`: **201.67%** · `cpu`: **50.42%** · `memory`: **50.42%** *(over $T_{\text{test}} = 15s$)* |
| **2. Saturation (Reference `6h` / `21,600s` Window)** | `google.com/tpu`: **0.14%** · `cpu`: **0.04%** · `memory`: **0.04%** *(over $T_{6\text{h}} = 21{,}600\text{s}$)* |
| **3. Recommended `100%` Saturation Settings (`5m` & `10m`)** | **5m (`300s`)**: `--speedup 0.101 --duration 5m` · **10m (`600s`)**: `--speedup 0.050 --duration 10m` *(bottleneck: `google.com/tpu`, $T_{100\%} = 30.2s$)* |

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

No `kueue.x-k8s.io/Topology` CRD is defined in `infra.json` (Topology-Aware Scheduling is not explicitly configured via a `Topology` object).

---

## 4. Initial Running Pods & Initial Cluster Utilization at $t_0$

At scenario start ($t_0 = 2026-06-01T00:00:00Z$), **`192`** running `Pod` objects are pre-placed across **`128`** of the **`128`** cluster nodes (`100.0%` of nodes occupied; `0` nodes completely free).
Additionally, `workflows.json` records **`192`** pre-existing `Admitted` `Job` warmup workflow object(s) at $t_0$.

### 4.1 Initial Cluster Utilization Breakdown

| Resource | Initial Pods Requested | Cluster Allocatable | Free Allocatable at $t_0$ | Initial Utilization (% Allocatable) | Initial Utilization (% Capacity) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| `cpu` | **1,024 cores** | 4,096 cores | 3,072 cores | **25.00%** | 25.00% |
| `memory` | **4,096 GiB** | 16,384 GiB (16.0 TiB) | 12,288 GiB (12.0 TiB) | **25.00%** | 25.00% |
| `pods` | **192** | 14,080 | 13,888 | **1.36%** | 1.36% |
| `google.com/tpu` | **1,024** | 1,024 | 0 | **100.00%** | 100.00% |

### 4.2 Initial Pod Remaining Lifetimes & Request Profiles

- **Pods with Finite Remaining Lifetime (`deletionTime - t0`)**: **192** pods — `min=20s`, `P50=20s`, `AVG=25s`, `P90=34s`, `P99=34s`, `max=34s`
- **Pods per Occupied Node (`128` active nodes)**: `min=1`, `P50=1`, `AVG=1.50`, `P90=2`, `max=2`

| Rank | Initial Pod Resource Request Profile | Pod Count | Share of Initial Pods |
| :---: | :--- | ---: | ---: |
| `1` | `cpu: 4`, `google.com/tpu: 4`, `memory: 16Gi` | **128** | 66.7% |
| `2` | `cpu: 8`, `google.com/tpu: 8`, `memory: 32Gi` | **64** | 33.3% |

---

## 5. Kueue Setup: ResourceFlavors, Cohorts, ClusterQueues, Namespaces & LocalQueues

### 5.1 ResourceFlavors (`ResourceFlavor`) & Topology Binding

| ResourceFlavor Name | Bound `topologyName` | Node Selector Labels (`spec.nodeLabels`) | Node Taints / Tolerations |
| :--- | :--- | :--- | :--- |
| `st-tpu-flavor` | `*(none)*` | `cloud.google.com/gke-tpu-accelerator=tpu-v7x-st`, `type=kwok` | none |

### 5.2 Workload Priority Classes (`WorkloadPriorityClass`)

| Priority Class Name | Priority `value` | API Version | Description |
| :--- | ---: | :--- | :--- |
| `p-high` | **`1000`** | `kueue.x-k8s.io/v1beta2` | — |
| `p-low` | **`100`** | `kueue.x-k8s.io/v1beta2` | — |

### 5.3 Cohorts & ClusterQueues — Quotas, Preemption & Fair Sharing

#### ClusterQueue Quotas, Preemption & Fair-Sharing Configuration

| ClusterQueue | Cohort | Queueing Strategy | FS Weight | Flavors | Nominal `cpu` | Nominal `memory` | Nominal `google.com/tpu` | Borrowing / Lending Limits | Preemption Policy (`withinCQ` / `reclaim` / `borrow`) |
| :--- | :--- | :--- | :---: | :--- | ---: | ---: | ---: | :--- | :--- |
| `cq-scale-preempt` | `*(standalone)*` | `BestEffortFIFO` | `1` | `st-tpu-flavor` | **4,096 cores** | **16,384 GiB (16.0 TiB)** | **1,024** | unbounded (default) | `LowerPriority` / `Never` / `Never` |
| **Total Nominal Quota (CQs + Cohort)** | — | — | — | — | **4,096 cores** (100.0% of cluster) | **16,384 GiB (16.0 TiB)** (100.0% of cluster) | **1,024** (100.0% of cluster) | — | — |

### 5.4 Namespaces & LocalQueues (`Namespace` & `LocalQueue`)

- **Namespaces (`1`)**: `st-workloads`

| Namespace | LocalQueue Name | Target `ClusterQueue` (`spec.clusterQueue`) | Target CQ Exists? | Target Cohort | Unadmitted Workloads Submitted |
| :--- | :--- | :--- | :---: | :--- | ---: |
| `st-workloads` | `lq-scale-preempt` | `cq-scale-preempt` | Yes | `*(standalone)*` | **888** |

---

## 6. Scenario Timeline & Arrival Dynamics

| Timeline Metric | Value |
| :--- | :--- |
| **Infrastructure Bootstrap Timestamp ($t_0$)** | `2026-06-01T00:00:00Z` |
| **First Unadmitted Workload Arrival ($t_{\text{first}}$)** | `2026-06-01T00:00:03Z` (offset `+3.00s` from $t_0$) |
| **Last Unadmitted Workload Arrival ($t_{\text{last}}$)** | `2026-06-01T00:00:18Z` (offset `+18.00s` from $t_0$) |
| **Active Test Arrival Duration ($T_{\text{test}} = t_{\text{last}} - t_{\text{first}}$)** | **15.0s** |
| **Unadmitted Workload Submission Count** | **888** workloads (plus **192** pre-admitted at $t_0$) |
| **Mean Arrival Rate ($\lambda$)** | **3552.00 workloads/min** (**213120.0 workloads/hour**) |
| **Inter-Arrival Time ($\Delta t_i$) Distribution** | `min=0.00s`, `P50=0.00s`, `AVG=0.02s`, `P90=0.00s`, `P99=0.00s`, `max=5.00s` |

```text
=== Unadmitted Workload Arrival Distribution Across 12 Equal Time Windows (total=888, span=15.0s) ===
  [   0.0s -    1.2s] |    64 (  7.2%) | ███████
  [   1.2s -    2.5s] |     0 (  0.0%) | 
  [   2.5s -    3.8s] |     0 (  0.0%) | 
  [   3.8s -    5.0s] |     0 (  0.0%) | 
  [   5.0s -    6.2s] |   280 ( 31.5%) | ████████████████████████████████
  [   6.2s -    7.5s] |     0 (  0.0%) | 
  [   7.5s -    8.8s] |     0 (  0.0%) | 
  [   8.8s -   10.0s] |     0 (  0.0%) | 
  [  10.0s -   11.2s] |   280 ( 31.5%) | ████████████████████████████████
  [  11.2s -   12.5s] |     0 (  0.0%) | 
  [  12.5s -   13.8s] |     0 (  0.0%) | 
  [  13.8s -   15.0s] |   264 ( 29.7%) | ██████████████████████████████
```

---

## 7. Workload Priorities, Cluster Saturation, Per-LocalQueue Dissection & Duration Histograms

### 7.0 Three-Part Offered Cluster Saturation & Recommended `5m` / `10m` Replay Settings

For any resource $r \in \{\text{accelerators}, \text{cpu}, \text{memory}\}$ with physical cluster capacity $C_{\text{cluster}}(r)$ and total offered resource-seconds $W(r) = \sum_{w} \left(\text{size}_w(r) \times \text{duration}_w\right)$, let $T_{100\%}(r) = W(r) / C_{\text{cluster}}(r)$ denote the ideal `1.0x` full-cluster service time required at continuous `100%` utilization:

$$\text{Sat}_{\text{window}}(r) = \frac{W(r)}{T_{\text{window}} \times C_{\text{cluster}}(r)} \times 100\% = \frac{T_{100\%}(r)}{T_{\text{window}}} \times 100\%, \qquad N_{100\%}(T_{\text{target}}) = \frac{T_{100\%}(r^*)}{T_{\text{target}}}$$

#### Parts 1 & 2 — Offered Saturation: Active Arrival Window ($T_{\text{test}}$) vs. Default `6h` (`21,600s`) Duration

| Resource ($r$) | Physical Cluster Capacity $C_{\text{cluster}}(r)$ | Total Offered Work ($1.0\text{x}$) | Ideal `100%`-Util Service Time $T_{100\%}(r)$ | (1) Saturation over Arrival Window ($T_{\text{test}} = 15s$) | (2) Saturation over Default `6h` ($21{,}600\text{s}$) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **`google.com/tpu`** *(bottleneck)* | 1,024 | `8.60` `google.com/tpu`-h | `30.2s` | **201.67%** | **0.14%** |
| `cpu` | 4,096 cores | `8.60` core-h | `7.6s` | 50.42% | 0.04% |
| `memory` | 16,384 GiB (16.0 TiB) | `34.42` GiB-h | `7.6s` | 50.42% | 0.04% |

#### Part 3 — Recommended `testrunner.py` Settings for `100%` Saturation in `5m` (`300s`) & `10m` (`600s`)

| Target Wall-Clock Window ($T_{\text{target}}$) | Bottleneck Resource ($r^*$) | Recommended `--speedup` ($N_{100\%}$) | Recommended `--duration` | Compressed Arrival Span ($T_{\text{test}} / N_{100\%}$) | Execution & Drain Behavior |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **`5m` (`300s`)** | `google.com/tpu` | **`--speedup 0.101`** | **`--duration 5m`** | `148.8s` | All arrivals submit within `148.8s` (≤ `300s`); `--duration 5m` captures `100%` of arrivals and `100.0%` full-cluster work in `5m`. |
| **`10m` (`600s`)** | `google.com/tpu` | **`--speedup 0.050`** | **`--duration 10m`** | `297.5s` | All arrivals submit within `297.5s` (≤ `600s`); `--duration 10m` captures `100%` of arrivals and `100.0%` full-cluster work in `10m`. |

```bash
# Run at 100% offered google.com/tpu saturation in a 5-minute (300s) wall-clock window:
python3 deploy/scripts/testrunner.py --scenario testscenarios/self-tests/st47-scale-quota-preemption-128n-1080w --speedup 0.101 --duration 5m

# Run at 100% offered google.com/tpu saturation in a 10-minute (600s) wall-clock window:
python3 deploy/scripts/testrunner.py --scenario testscenarios/self-tests/st47-scale-quota-preemption-128n-1080w --speedup 0.050 --duration 10m
```

### 7.1 Workload Breakdown & Cluster Saturation by Priority Class

| Priority Class | Priority Value | Workloads | Share | Total Pods | Gang Size (`min / P50 / P90 / max`) | Duration (`P50 / AVG / P90 / P99`) | Offered `google.com/tpu`-Hours | `google.com/tpu` Saturation | `cpu` Saturation | `memory` Saturation |
| :--- | ---: | ---: | ---: | ---: | :---: | :---: | ---: | ---: | ---: | ---: |
| `p-high` | `1000` | 344 | 38.7% | 344 | `1 / 1 / 1 / 1` | `8s / 8s / 9s / 9s` | 3.8 | **88.33%** | 22.08% | 22.08% |
| `p-low` | `100` | 544 | 61.3% | 544 | `1 / 1 / 1 / 1` | `8s / 8s / 8s / 8s` | 4.8 | **113.33%** | 28.33% | 28.33% |
| **ALL (Total)** | — | **888** | 100.0% | 888 | `1 / 1 / 1 / 1` | `8s / 8s / 8s / 9s` | 8.6 | **201.67%** | **50.42%** | **50.42%** |

### 7.2 Workload Dissection per `LocalQueue` (and Target `ClusterQueue`)

| LocalQueue (`ns/name`) | Target `ClusterQueue` | Workloads | Share | Total Pods | Duration (`P50 / AVG / P90 / P99`) | Offered `google.com/tpu`-h | Cluster `google.com/tpu` Saturation | Target CQ Nominal Quota Saturation | Priority Breakdown (Count) |
| :--- | :--- | ---: | ---: | ---: | :---: | ---: | ---: | ---: | :--- |
| `st-workloads/lq-scale-preempt` | `cq-scale-preempt` | **888** | 100.0% | 888 | `8s / 8s / 8s / 9s` | 8.6 | **201.67%** | **201.67%** | `p-high`:344, `p-low`:544 |

#### Detailed Per-`LocalQueue` × Priority Dissection

<details>
<summary><b>LocalQueue <code>st-workloads/lq-scale-preempt</code> → ClusterQueue <code>cq-scale-preempt</code></b> (888 workloads)</summary>

| Priority Class | Priority Value | Workloads | Share of LQ | Total Pods | Duration (`P50 / AVG / P90 / P99`) | Cluster `google.com/tpu` Saturation | Cluster `cpu` Saturation |
| :--- | ---: | ---: | ---: | ---: | :---: | ---: | ---: |
| `p-high` | `1000` | **344** | 38.7% | 344 | `8s / 8s / 9s / 9s` | 88.33% | 22.08% |
| `p-low` | `100` | **544** | 61.3% | 544 | `8s / 8s / 8s / 8s` | 113.33% | 28.33% |

</details>

### 7.3 Workload Execution Duration Histograms (Aggregated & Per Priority)

#### Aggregated Duration Histogram (All Unadmitted Workloads)

```text
=== ALL PRIORITIES (Aggregated) | count=888 | min=8s, p50=8s (0.1m), avg=8.1s (0.1m), p90=8s (0.1m), p99=9s (0.1m), max=9s ===
  < 1m   (0-60s)       |   888 (100.0%) | ████████████████████████████████
  1-5m   (60-300s)     |     0 (  0.0%) | 
  5-10m  (300-600s)    |     0 (  0.0%) | 
  10-15m (600-900s)    |     0 (  0.0%) | 
  15-20m (900-1200s)   |     0 (  0.0%) | 
  20-30m (1200-1800s)  |     0 (  0.0%) | 
```

#### Per-Priority Duration Histograms

```text
=== Priority: p-high (value=1000) | count=344 | min=8s, p50=8s (0.1m), avg=8.2s (0.1m), p90=9s (0.1m), p99=9s (0.1m), max=9s ===
  < 1m   (0-60s)       |   344 (100.0%) | ████████████████████████████████
  1-5m   (60-300s)     |     0 (  0.0%) | 
  5-10m  (300-600s)    |     0 (  0.0%) | 
  10-15m (600-900s)    |     0 (  0.0%) | 
  15-20m (900-1200s)   |     0 (  0.0%) | 
  20-30m (1200-1800s)  |     0 (  0.0%) | 

=== Priority: p-low (value=100) | count=544 | min=8s, p50=8s (0.1m), avg=8.0s (0.1m), p90=8s (0.1m), p99=8s (0.1m), max=8s ===
  < 1m   (0-60s)       |   544 (100.0%) | ████████████████████████████████
  1-5m   (60-300s)     |     0 (  0.0%) | 
  5-10m  (300-600s)    |     0 (  0.0%) | 
  10-15m (600-900s)    |     0 (  0.0%) | 
  15-20m (900-1200s)   |     0 (  0.0%) | 
  20-30m (1200-1800s)  |     0 (  0.0%) | 
```
