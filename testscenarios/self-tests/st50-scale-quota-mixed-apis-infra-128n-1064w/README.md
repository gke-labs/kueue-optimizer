# Test Scenario Summary: `st50-scale-quota-mixed-apis-infra-128n-1064w`

> **Auto-generated scenario analysis** from [`testscenarios/self-tests/st50-scale-quota-mixed-apis-infra-128n-1064w/infra.json`](./infra.json), [`testscenarios/self-tests/st50-scale-quota-mixed-apis-infra-128n-1064w/workflows.json`](./workflows.json), and [`testscenarios/self-tests/st50-scale-quota-mixed-apis-infra-128n-1064w/execution-time.json`](./execution-time.json) via [`deploy/scripts/generate_scenario_readme.py`](../../../deploy/scripts/generate_scenario_readme.py).

## 1. Scenario Overview

| Dimension | Summary |
| :--- | :--- |
| **Scenario Directory** | `testscenarios/self-tests/st50-scale-quota-mixed-apis-infra-128n-1064w` |
| **Raw Event Counts** | `infra.json`: **308** events · `workflows.json`: **1,080** events · `execution-time.json`: **2,112** events |
| **Cluster Capacity** | **128 nodes** · `4,096 cores` · `16,384 GiB (16.0 TiB)` · `1,024` `google.com/tpu` (1 distinct node group) |
| **Topology Configuration** | **0** `Topology` CRD(s) (none) |
| **Initial In-Flight Pods at $t_0$** | **96** pods across **80** / 128 nodes (initial `google.com/tpu` utilization: **31.25%**) |
| **Kueue Objects** | **1** `ResourceFlavor`(s) · **0** `Cohort`(s) · **1** `ClusterQueue`(s) · **1** `LocalQueue`(s) · **0** `WorkloadPriorityClass`(es) |
| **Test Arrival Window ($T_{\text{test}}$)** | **12.0s** (`2026-06-01T00:00:01Z` $\rightarrow$ `2026-06-01T00:00:13Z`) |
| **Unadmitted Workloads** | **1,000** `NotAdmitted` arrivals (1,000 total pods; gang size min=1, p50=1, p90=1, max=1) + **64** pre-admitted at $t_0$ |
| **Execution Duration Quantiles** | `min=8.0s`, `P50=8.0s`, `AVG=8.0s`, `P90=8.0s`, `P99=8.0s`, `max=8.0s` |
| **1. Saturation (Arrival Window $T_{\text{test}}$)** | `google.com/tpu`: **260.42%** · `cpu`: **65.10%** · `memory`: **65.10%** *(over $T_{\text{test}} = 12s$)* |
| **2. Saturation (Reference `6h` / `21,600s` Window)** | `google.com/tpu`: **0.14%** · `cpu`: **0.04%** · `memory`: **0.04%** *(over $T_{6\text{h}} = 21{,}600\text{s}$)* |
| **3. Recommended `100%` Saturation Settings (`5m` & `10m`)** | **5m (`300s`)**: `--speedup 0.104 --duration 5m` · **10m (`600s`)**: `--speedup 0.052 --duration 10m` *(bottleneck: `google.com/tpu`, $T_{100\%} = 31.2s$)* |

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

At scenario start ($t_0 = 2026-06-01T00:00:00Z$), **`96`** running `Pod` objects are pre-placed across **`80`** of the **`128`** cluster nodes (`62.5%` of nodes occupied; `48` nodes completely free).
Additionally, `workflows.json` records **`64`** pre-existing `Admitted` warmup workflow object(s) at $t_0$ (16 `Deployment`, 16 `Job`, 16 `JobSet`, 16 `PodGroup`).

### 4.1 Initial Cluster Utilization Breakdown

| Resource | Initial Pods Requested | Cluster Allocatable | Free Allocatable at $t_0$ | Initial Utilization (% Allocatable) | Initial Utilization (% Capacity) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| `cpu` | **320 cores** | 4,096 cores | 3,776 cores | **7.81%** | 7.81% |
| `memory` | **1,280 GiB** | 16,384 GiB (16.0 TiB) | 15,104 GiB | **7.81%** | 7.81% |
| `pods` | **96** | 14,080 | 13,984 | **0.68%** | 0.68% |
| `google.com/tpu` | **320** | 1,024 | 704 | **31.25%** | 31.25% |

### 4.2 Initial Pod Remaining Lifetimes & Request Profiles

- **Pods with Finite Remaining Lifetime (`deletionTime - t0`)**: **80** pods — `min=12s`, `P50=12s`, `AVG=12s`, `P90=14s`, `P99=14s`, `max=14s`
- **Pods with No Deletion Event (Run Entire Scenario / Infinite Duration)**: **16** pods
- **Pods per Occupied Node (`80` active nodes)**: `min=1`, `P50=1`, `AVG=1.20`, `P90=2`, `max=2`

| Rank | Initial Pod Resource Request Profile | Pod Count | Share of Initial Pods |
| :---: | :--- | ---: | ---: |
| `1` | `cpu: 4`, `google.com/tpu: 4`, `memory: 16Gi` | **64** | 66.7% |
| `2` | `cpu: 2`, `google.com/tpu: 2`, `memory: 8Gi` | **32** | 33.3% |

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
| `cq-scale-mixed` | `*(standalone)*` | `BestEffortFIFO` | `1` | `st-tpu-flavor` | **4,096 cores** | **16,384 GiB (16.0 TiB)** | **1,024** | unbounded (default) | `Never` / `Never` / `Never` |
| **Total Nominal Quota (CQs + Cohort)** | — | — | — | — | **4,096 cores** (100.0% of cluster) | **16,384 GiB (16.0 TiB)** (100.0% of cluster) | **1,024** (100.0% of cluster) | — | — |

### 5.4 Namespaces & LocalQueues (`Namespace` & `LocalQueue`)

- **Namespaces (`1`)**: `st-workloads`

| Namespace | LocalQueue Name | Target `ClusterQueue` (`spec.clusterQueue`) | Target CQ Exists? | Target Cohort | Unadmitted Workloads Submitted |
| :--- | :--- | :--- | :---: | :--- | ---: |
| `st-workloads` | `lq-scale-mixed` | `cq-scale-mixed` | Yes | `*(standalone)*` | **1,000** |

---

## 6. Scenario Timeline & Arrival Dynamics

| Timeline Metric | Value |
| :--- | :--- |
| **Infrastructure Bootstrap Timestamp ($t_0$)** | `2026-06-01T00:00:00Z` |
| **First Unadmitted Workload Arrival ($t_{\text{first}}$)** | `2026-06-01T00:00:01Z` (offset `+1.00s` from $t_0$) |
| **Last Unadmitted Workload Arrival ($t_{\text{last}}$)** | `2026-06-01T00:00:13Z` (offset `+13.00s` from $t_0$) |
| **Active Test Arrival Duration ($T_{\text{test}} = t_{\text{last}} - t_{\text{first}}$)** | **12.0s** |
| **Unadmitted Workload Submission Count** | **1,000** workloads (plus **64** pre-admitted at $t_0$) |
| **Mean Arrival Rate ($\lambda$)** | **5000.00 workloads/min** (**300000.0 workloads/hour**) |
| **Inter-Arrival Time ($\Delta t_i$) Distribution** | `min=0.00s`, `P50=0.00s`, `AVG=0.01s`, `P90=0.00s`, `P99=0.00s`, `max=2.00s` |

```text
=== Unadmitted Workload Arrival Distribution Across 12 Equal Time Windows (total=1,000, span=12.0s) ===
  [   0.0s -    1.0s] |   200 ( 20.0%) | ████████████████████████████████
  [   1.0s -    2.0s] |     0 (  0.0%) | 
  [   2.0s -    3.0s] |    50 (  5.0%) | ████████
  [   3.0s -    4.0s] |     0 (  0.0%) | 
  [   4.0s -    5.0s] |   200 ( 20.0%) | ████████████████████████████████
  [   5.0s -    6.0s] |    50 (  5.0%) | ████████
  [   6.0s -    7.0s] |     0 (  0.0%) | 
  [   7.0s -    8.0s] |    50 (  5.0%) | ████████
  [   8.0s -    9.0s] |   200 ( 20.0%) | ████████████████████████████████
  [   9.0s -   10.0s] |     0 (  0.0%) | 
  [  10.0s -   11.0s] |    50 (  5.0%) | ████████
  [  11.0s -   12.0s] |   200 ( 20.0%) | ████████████████████████████████
```

---

## 7. Workload Priorities, Cluster Saturation, Per-LocalQueue Dissection & Duration Histograms

### 7.0 Three-Part Offered Cluster Saturation & Recommended `5m` / `10m` Replay Settings

For any resource $r \in \{\text{accelerators}, \text{cpu}, \text{memory}\}$ with physical cluster capacity $C_{\text{cluster}}(r)$ and total offered resource-seconds $W(r) = \sum_{w} \left(\text{size}_w(r) \times \text{duration}_w\right)$, let $T_{100\%}(r) = W(r) / C_{\text{cluster}}(r)$ denote the ideal `1.0x` full-cluster service time required at continuous `100%` utilization:

$$\text{Sat}_{\text{window}}(r) = \frac{W(r)}{T_{\text{window}} \times C_{\text{cluster}}(r)} \times 100\% = \frac{T_{100\%}(r)}{T_{\text{window}}} \times 100\%, \qquad N_{100\%}(T_{\text{target}}) = \frac{T_{100\%}(r^*)}{T_{\text{target}}}$$

#### Parts 1 & 2 — Offered Saturation: Active Arrival Window ($T_{\text{test}}$) vs. Default `6h` (`21,600s`) Duration

| Resource ($r$) | Physical Cluster Capacity $C_{\text{cluster}}(r)$ | Total Offered Work ($1.0\text{x}$) | Ideal `100%`-Util Service Time $T_{100\%}(r)$ | (1) Saturation over Arrival Window ($T_{\text{test}} = 12s$) | (2) Saturation over Default `6h` ($21{,}600\text{s}$) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **`google.com/tpu`** *(bottleneck)* | 1,024 | `8.89` `google.com/tpu`-h | `31.2s` | **260.42%** | **0.14%** |
| `cpu` | 4,096 cores | `8.89` core-h | `7.8s` | 65.10% | 0.04% |
| `memory` | 16,384 GiB (16.0 TiB) | `35.56` GiB-h | `7.8s` | 65.10% | 0.04% |

#### Part 3 — Recommended `testrunner.py` Settings for `100%` Saturation in `5m` (`300s`) & `10m` (`600s`)

| Target Wall-Clock Window ($T_{\text{target}}$) | Bottleneck Resource ($r^*$) | Recommended `--speedup` ($N_{100\%}$) | Recommended `--duration` | Compressed Arrival Span ($T_{\text{test}} / N_{100\%}$) | Execution & Drain Behavior |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **`5m` (`300s`)** | `google.com/tpu` | **`--speedup 0.104`** | **`--duration 5m`** | `115.2s` | All arrivals submit within `115.2s` (≤ `300s`); `--duration 5m` captures `100%` of arrivals and `100.0%` full-cluster work in `5m`. |
| **`10m` (`600s`)** | `google.com/tpu` | **`--speedup 0.052`** | **`--duration 10m`** | `230.4s` | All arrivals submit within `230.4s` (≤ `600s`); `--duration 10m` captures `100%` of arrivals and `100.0%` full-cluster work in `10m`. |

```bash
# Run at 100% offered google.com/tpu saturation in a 5-minute (300s) wall-clock window:
python3 deploy/scripts/testrunner.py --scenario testscenarios/self-tests/st50-scale-quota-mixed-apis-infra-128n-1064w --speedup 0.104 --duration 5m

# Run at 100% offered google.com/tpu saturation in a 10-minute (600s) wall-clock window:
python3 deploy/scripts/testrunner.py --scenario testscenarios/self-tests/st50-scale-quota-mixed-apis-infra-128n-1064w --speedup 0.052 --duration 10m
```

### 7.1 Workload Breakdown & Cluster Saturation by Priority Class

| Priority Class | Priority Value | Workloads | Share | Total Pods | Gang Size (`min / P50 / P90 / max`) | Duration (`P50 / AVG / P90 / P99`) | Offered `google.com/tpu`-Hours | `google.com/tpu` Saturation | `cpu` Saturation | `memory` Saturation |
| :--- | ---: | ---: | ---: | ---: | :---: | :---: | ---: | ---: | ---: | ---: |
| `<default>` | `0` | 1,000 | 100.0% | 1,000 | `1 / 1 / 1 / 1` | `8s / 8s / 8s / 8s` | 8.9 | **260.42%** | 65.10% | 65.10% |
| **ALL (Total)** | — | **1,000** | 100.0% | 1,000 | `1 / 1 / 1 / 1` | `8s / 8s / 8s / 8s` | 8.9 | **260.42%** | **65.10%** | **65.10%** |

### 7.2 Workload Dissection per `LocalQueue` (and Target `ClusterQueue`)

| LocalQueue (`ns/name`) | Target `ClusterQueue` | Workloads | Share | Total Pods | Duration (`P50 / AVG / P90 / P99`) | Offered `google.com/tpu`-h | Cluster `google.com/tpu` Saturation | Target CQ Nominal Quota Saturation | Priority Breakdown (Count) |
| :--- | :--- | ---: | ---: | ---: | :---: | ---: | ---: | ---: | :--- |
| `st-workloads/lq-scale-mixed` | `cq-scale-mixed` | **1,000** | 100.0% | 1,000 | `8s / 8s / 8s / 8s` | 8.9 | **260.42%** | **260.42%** | `<default>`:1000 |

#### Detailed Per-`LocalQueue` × Priority Dissection

<details>
<summary><b>LocalQueue <code>st-workloads/lq-scale-mixed</code> → ClusterQueue <code>cq-scale-mixed</code></b> (1,000 workloads)</summary>

| Priority Class | Priority Value | Workloads | Share of LQ | Total Pods | Duration (`P50 / AVG / P90 / P99`) | Cluster `google.com/tpu` Saturation | Cluster `cpu` Saturation |
| :--- | ---: | ---: | ---: | ---: | :---: | ---: | ---: |
| `<default>` | `0` | **1,000** | 100.0% | 1,000 | `8s / 8s / 8s / 8s` | 260.42% | 65.10% |

</details>

### 7.3 Workload Execution Duration Histograms (Aggregated & Per Priority)

#### Aggregated Duration Histogram (All Unadmitted Workloads)

```text
=== ALL PRIORITIES (Aggregated) | count=1,000 | min=8s, p50=8s (0.1m), avg=8.0s (0.1m), p90=8s (0.1m), p99=8s (0.1m), max=8s ===
  < 1m   (0-60s)       | 1,000 (100.0%) | ████████████████████████████████
  1-5m   (60-300s)     |     0 (  0.0%) | 
  5-10m  (300-600s)    |     0 (  0.0%) | 
  10-15m (600-900s)    |     0 (  0.0%) | 
  15-20m (900-1200s)   |     0 (  0.0%) | 
  20-30m (1200-1800s)  |     0 (  0.0%) | 
```

#### Per-Priority Duration Histograms

```text
=== Priority: <default> (value=0) | count=1,000 | min=8s, p50=8s (0.1m), avg=8.0s (0.1m), p90=8s (0.1m), p99=8s (0.1m), max=8s ===
  < 1m   (0-60s)       | 1,000 (100.0%) | ████████████████████████████████
  1-5m   (60-300s)     |     0 (  0.0%) | 
  5-10m  (300-600s)    |     0 (  0.0%) | 
  10-15m (600-900s)    |     0 (  0.0%) | 
  15-20m (900-1200s)   |     0 (  0.0%) | 
  20-30m (1200-1800s)  |     0 (  0.0%) | 
```
