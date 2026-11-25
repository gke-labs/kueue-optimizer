# Test Scenario Summary: `st48-scale-quota-cohort-reclaim-144n-1116w`

> **Auto-generated scenario analysis** from [`testscenarios/self-tests/st48-scale-quota-cohort-reclaim-144n-1116w/infra.json`](./infra.json), [`testscenarios/self-tests/st48-scale-quota-cohort-reclaim-144n-1116w/workflows.json`](./workflows.json), and [`testscenarios/self-tests/st48-scale-quota-cohort-reclaim-144n-1116w/execution-time.json`](./execution-time.json) via [`deploy/scripts/generate_scenario_readme.py`](../../../deploy/scripts/generate_scenario_readme.py).

## 1. Scenario Overview

| Dimension | Summary |
| :--- | :--- |
| **Scenario Directory** | `testscenarios/self-tests/st48-scale-quota-cohort-reclaim-144n-1116w` |
| **Raw Event Counts** | `infra.json`: **439** events · `workflows.json`: **1,116** events · `execution-time.json`: **2,232** events |
| **Cluster Capacity** | **144 nodes** · `4,608 cores` · `18,432 GiB (18.0 TiB)` · `1,152` `google.com/tpu` (1 distinct node group) |
| **Topology Configuration** | **0** `Topology` CRD(s) (none) |
| **Initial In-Flight Pods at $t_0$** | **144** pods across **144** / 144 nodes (initial `google.com/tpu` utilization: **100.00%**) |
| **Kueue Objects** | **1** `ResourceFlavor`(s) · **1** `Cohort`(s) · **2** `ClusterQueue`(s) · **2** `LocalQueue`(s) · **0** `WorkloadPriorityClass`(es) |
| **Test Arrival Window ($T_{\text{test}}$)** | **17.0s** (`2026-06-01T00:00:03Z` $\rightarrow$ `2026-06-01T00:00:20Z`) |
| **Unadmitted Workloads** | **972** `NotAdmitted` arrivals (972 total pods; gang size min=1, p50=1, p90=1, max=1) + **144** pre-admitted at $t_0$ |
| **Execution Duration Quantiles** | `min=8.0s`, `P50=8.0s`, `AVG=8.1s`, `P90=8.0s`, `P99=10s`, `max=10s` |
| **1. Saturation (Arrival Window $T_{\text{test}}$)** | `google.com/tpu`: **176.47%** · `cpu`: **44.12%** · `memory`: **44.12%** *(over $T_{\text{test}} = 17s$)* |
| **2. Saturation (Reference `6h` / `21,600s` Window)** | `google.com/tpu`: **0.14%** · `cpu`: **0.03%** · `memory`: **0.03%** *(over $T_{6\text{h}} = 21{,}600\text{s}$)* |
| **3. Recommended `100%` Saturation Settings (`5m` & `10m`)** | **5m (`300s`)**: `--speedup 0.100 --duration 5m` · **10m (`600s`)**: `--speedup 0.050 --duration 10m` *(bottleneck: `google.com/tpu`, $T_{100\%} = 30.0s$)* |

---

## 2. Cluster Nodes & Distinct Node Groups

The cluster contains **144** `v1/Node` objects partitioned into **1** distinct hardware/label configuration group(s).

### 2.1 Aggregate Physical Cluster Capacity & Allocatable Resources

| Resource | Total Capacity | Total Allocatable | Per-Node Average (Capacity) |
| :--- | ---: | ---: | ---: |
| `nodes` | **144** | **144** | `1` |
| `cpu` | **4,608 cores** | **4,608 cores** | 32 cores |
| `memory` | **18,432 GiB (18.0 TiB)** | **18,432 GiB (18.0 TiB)** | 128 GiB |
| `pods` | **15,840** | **15,840** | 110 |
| `google.com/tpu` | **1,152** | **1,152** | 8 |

### 2.2 Summary of Distinct Node Groups

| Group | Node Count | Share | Sample Node Name | `cpu` / Node | `memory` / Node | `pods` / Node | `google.com/tpu` / Node | Key Identifying Labels |
| :--- | ---: | ---: | :--- | ---: | ---: | ---: | ---: | :--- |
| **Group 1: accel=`tpu-v7x-st`** | **144** | 100.0% | `st-node-000` | `32` | `128Gi` | `110` | 8 | `cloud.google.com/gke-tpu-accelerator=tpu-v7x-st`, `kubernetes.io/role=agent`, `kueue-optimizer.gke.io/node-role=agent`, `type=kwok` |

### 2.3 Sample Node Manifest (YAML) from Each Distinct Group

#### Group 1: accel=`tpu-v7x-st` — Sample Node `st-node-000` (144 nodes in group)

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

At scenario start ($t_0 = 2026-06-01T00:00:00Z$), **`144`** running `Pod` objects are pre-placed across **`144`** of the **`144`** cluster nodes (`100.0%` of nodes occupied; `0` nodes completely free).
Additionally, `workflows.json` records **`144`** pre-existing `Admitted` `Job` warmup workflow object(s) at $t_0$.

### 4.1 Initial Cluster Utilization Breakdown

| Resource | Initial Pods Requested | Cluster Allocatable | Free Allocatable at $t_0$ | Initial Utilization (% Allocatable) | Initial Utilization (% Capacity) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| `cpu` | **1,152 cores** | 4,608 cores | 3,456 cores | **25.00%** | 25.00% |
| `memory` | **4,608 GiB** | 18,432 GiB (18.0 TiB) | 13,824 GiB (13.5 TiB) | **25.00%** | 25.00% |
| `pods` | **144** | 15,840 | 15,696 | **0.91%** | 0.91% |
| `google.com/tpu` | **1,152** | 1,152 | 0 | **100.00%** | 100.00% |

### 4.2 Initial Pod Remaining Lifetimes & Request Profiles

- **Pods with Finite Remaining Lifetime (`deletionTime - t0`)**: **144** pods — `min=28s`, `P50=28s`, `AVG=28s`, `P90=28s`, `P99=28s`, `max=28s`
- **Pods per Occupied Node (`144` active nodes)**: `min=1`, `P50=1`, `AVG=1.00`, `P90=1`, `max=1`

| Rank | Initial Pod Resource Request Profile | Pod Count | Share of Initial Pods |
| :---: | :--- | ---: | ---: |
| `1` | `cpu: 8`, `google.com/tpu: 8`, `memory: 32Gi` | **144** | 100.0% |

---

## 5. Kueue Setup: ResourceFlavors, Cohorts, ClusterQueues, Namespaces & LocalQueues

### 5.1 ResourceFlavors (`ResourceFlavor`) & Topology Binding

| ResourceFlavor Name | Bound `topologyName` | Node Selector Labels (`spec.nodeLabels`) | Node Taints / Tolerations |
| :--- | :--- | :--- | :--- |
| `st-tpu-flavor` | `*(none)*` | `cloud.google.com/gke-tpu-accelerator=tpu-v7x-st`, `type=kwok` | none |

### 5.2 Workload Priority Classes (`WorkloadPriorityClass`)

*(No explicit `WorkloadPriorityClass` objects defined in `infra.json`; all workloads run at default priority unless annotated.)*

### 5.3 Cohorts & ClusterQueues — Quotas, Preemption & Fair Sharing

#### Cohort Configuration

| Cohort Name | Explicit CRD in `infra.json` | Parent Cohort | Fair-Sharing Weight | Member ClusterQueues | Direct Cohort Remainder Quota |
| :--- | :---: | :--- | :---: | :---: | :--- |
| `cohort-scale-quota` | Yes | `*(root)*` | `1 (default)` | **2** (`cq-scale-borrower`, `cq-scale-lender`) | `cpu`: 0 cores, `memory`: 0 GiB, `google.com/tpu`: 0 |

#### ClusterQueue Quotas, Preemption & Fair-Sharing Configuration

| ClusterQueue | Cohort | Queueing Strategy | FS Weight | Flavors | Nominal `cpu` | Nominal `memory` | Nominal `google.com/tpu` | Borrowing / Lending Limits | Preemption Policy (`withinCQ` / `reclaim` / `borrow`) |
| :--- | :--- | :--- | :---: | :--- | ---: | ---: | ---: | :--- | :--- |
| `cq-scale-borrower` | `cohort-scale-quota` | `BestEffortFIFO` | `1` | `st-tpu-flavor` | **2,304 cores** | **9,216 GiB** | **576** | unbounded (default) | `Never` / `Any` / `Never` |
| `cq-scale-lender` | `cohort-scale-quota` | `BestEffortFIFO` | `1` | `st-tpu-flavor` | **2,304 cores** | **9,216 GiB** | **576** | unbounded (default) | `Never` / `Any` / `Never` |
| **Total Nominal Quota (CQs + Cohort)** | — | — | — | — | **4,608 cores** (100.0% of cluster) | **18,432 GiB (18.0 TiB)** (100.0% of cluster) | **1,152** (100.0% of cluster) | — | — |

### 5.4 Namespaces & LocalQueues (`Namespace` & `LocalQueue`)

- **Namespaces (`1`)**: `st-workloads`

| Namespace | LocalQueue Name | Target `ClusterQueue` (`spec.clusterQueue`) | Target CQ Exists? | Target Cohort | Unadmitted Workloads Submitted |
| :--- | :--- | :--- | :---: | :--- | ---: |
| `st-workloads` | `lq-scale-borrower` | `cq-scale-borrower` | Yes | `cohort-scale-quota` | **450** |
| `st-workloads` | `lq-scale-lender` | `cq-scale-lender` | Yes | `cohort-scale-quota` | **522** |

---

## 6. Scenario Timeline & Arrival Dynamics

| Timeline Metric | Value |
| :--- | :--- |
| **Infrastructure Bootstrap Timestamp ($t_0$)** | `2026-06-01T00:00:00Z` |
| **First Unadmitted Workload Arrival ($t_{\text{first}}$)** | `2026-06-01T00:00:03Z` (offset `+3.00s` from $t_0$) |
| **Last Unadmitted Workload Arrival ($t_{\text{last}}$)** | `2026-06-01T00:00:20Z` (offset `+20.00s` from $t_0$) |
| **Active Test Arrival Duration ($T_{\text{test}} = t_{\text{last}} - t_{\text{first}}$)** | **17.0s** |
| **Unadmitted Workload Submission Count** | **972** workloads (plus **144** pre-admitted at $t_0$) |
| **Mean Arrival Rate ($\lambda$)** | **3430.59 workloads/min** (**205835.3 workloads/hour**) |
| **Inter-Arrival Time ($\Delta t_i$) Distribution** | `min=0.00s`, `P50=0.00s`, `AVG=0.02s`, `P90=0.00s`, `P99=0.00s`, `max=7.00s` |

```text
=== Unadmitted Workload Arrival Distribution Across 12 Equal Time Windows (total=972, span=17.0s) ===
  [   0.0s -    1.4s] |    72 (  7.4%) | ████████
  [   1.4s -    2.8s] |     0 (  0.0%) | 
  [   2.8s -    4.2s] |     0 (  0.0%) | 
  [   4.2s -    5.7s] |     0 (  0.0%) | 
  [   5.7s -    7.1s] |   300 ( 30.9%) | ████████████████████████████████
  [   7.1s -    8.5s] |     0 (  0.0%) | 
  [   8.5s -    9.9s] |     0 (  0.0%) | 
  [   9.9s -   11.3s] |     0 (  0.0%) | 
  [  11.3s -   12.8s] |   300 ( 30.9%) | ████████████████████████████████
  [  12.8s -   14.2s] |     0 (  0.0%) | 
  [  14.2s -   15.6s] |     0 (  0.0%) | 
  [  15.6s -   17.0s] |   300 ( 30.9%) | ████████████████████████████████
```

---

## 7. Workload Priorities, Cluster Saturation, Per-LocalQueue Dissection & Duration Histograms

### 7.0 Three-Part Offered Cluster Saturation & Recommended `5m` / `10m` Replay Settings

For any resource $r \in \{\text{accelerators}, \text{cpu}, \text{memory}\}$ with physical cluster capacity $C_{\text{cluster}}(r)$ and total offered resource-seconds $W(r) = \sum_{w} \left(\text{size}_w(r) \times \text{duration}_w\right)$, let $T_{100\%}(r) = W(r) / C_{\text{cluster}}(r)$ denote the ideal `1.0x` full-cluster service time required at continuous `100%` utilization:

$$\text{Sat}_{\text{window}}(r) = \frac{W(r)}{T_{\text{window}} \times C_{\text{cluster}}(r)} \times 100\% = \frac{T_{100\%}(r)}{T_{\text{window}}} \times 100\%, \qquad N_{100\%}(T_{\text{target}}) = \frac{T_{100\%}(r^*)}{T_{\text{target}}}$$

#### Parts 1 & 2 — Offered Saturation: Active Arrival Window ($T_{\text{test}}$) vs. Default `6h` (`21,600s`) Duration

| Resource ($r$) | Physical Cluster Capacity $C_{\text{cluster}}(r)$ | Total Offered Work ($1.0\text{x}$) | Ideal `100%`-Util Service Time $T_{100\%}(r)$ | (1) Saturation over Arrival Window ($T_{\text{test}} = 17s$) | (2) Saturation over Default `6h` ($21{,}600\text{s}$) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **`google.com/tpu`** *(bottleneck)* | 1,152 | `9.60` `google.com/tpu`-h | `30.0s` | **176.47%** | **0.14%** |
| `cpu` | 4,608 cores | `9.60` core-h | `7.5s` | 44.12% | 0.03% |
| `memory` | 18,432 GiB (18.0 TiB) | `38.40` GiB-h | `7.5s` | 44.12% | 0.03% |

#### Part 3 — Recommended `testrunner.py` Settings for `100%` Saturation in `5m` (`300s`) & `10m` (`600s`)

| Target Wall-Clock Window ($T_{\text{target}}$) | Bottleneck Resource ($r^*$) | Recommended `--speedup` ($N_{100\%}$) | Recommended `--duration` | Compressed Arrival Span ($T_{\text{test}} / N_{100\%}$) | Execution & Drain Behavior |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **`5m` (`300s`)** | `google.com/tpu` | **`--speedup 0.100`** | **`--duration 5m`** | `170.0s` | All arrivals submit within `170.0s` (≤ `300s`); `--duration 5m` captures `100%` of arrivals and `100.0%` full-cluster work in `5m`. |
| **`10m` (`600s`)** | `google.com/tpu` | **`--speedup 0.050`** | **`--duration 10m`** | `340.0s` | All arrivals submit within `340.0s` (≤ `600s`); `--duration 10m` captures `100%` of arrivals and `100.0%` full-cluster work in `10m`. |

```bash
# Run at 100% offered google.com/tpu saturation in a 5-minute (300s) wall-clock window:
python3 deploy/scripts/testrunner.py --scenario testscenarios/self-tests/st48-scale-quota-cohort-reclaim-144n-1116w --speedup 0.100 --duration 5m

# Run at 100% offered google.com/tpu saturation in a 10-minute (600s) wall-clock window:
python3 deploy/scripts/testrunner.py --scenario testscenarios/self-tests/st48-scale-quota-cohort-reclaim-144n-1116w --speedup 0.050 --duration 10m
```

### 7.1 Workload Breakdown & Cluster Saturation by Priority Class

| Priority Class | Priority Value | Workloads | Share | Total Pods | Gang Size (`min / P50 / P90 / max`) | Duration (`P50 / AVG / P90 / P99`) | Offered `google.com/tpu`-Hours | `google.com/tpu` Saturation | `cpu` Saturation | `memory` Saturation |
| :--- | ---: | ---: | ---: | ---: | :---: | :---: | ---: | ---: | ---: | ---: |
| `<default>` | `0` | 972 | 100.0% | 972 | `1 / 1 / 1 / 1` | `8s / 8s / 8s / 10s` | 9.6 | **176.47%** | 44.12% | 44.12% |
| **ALL (Total)** | — | **972** | 100.0% | 972 | `1 / 1 / 1 / 1` | `8s / 8s / 8s / 10s` | 9.6 | **176.47%** | **44.12%** | **44.12%** |

### 7.2 Workload Dissection per `LocalQueue` (and Target `ClusterQueue`)

| LocalQueue (`ns/name`) | Target `ClusterQueue` | Workloads | Share | Total Pods | Duration (`P50 / AVG / P90 / P99`) | Offered `google.com/tpu`-h | Cluster `google.com/tpu` Saturation | Target CQ Nominal Quota Saturation | Priority Breakdown (Count) |
| :--- | :--- | ---: | ---: | ---: | :---: | ---: | ---: | ---: | :--- |
| `st-workloads/lq-scale-lender` | `cq-scale-lender` | **522** | 53.7% | 522 | `8s / 8s / 10s / 10s` | 5.6 | **102.94%** | **205.88%** | `<default>`:522 |
| `st-workloads/lq-scale-borrower` | `cq-scale-borrower` | **450** | 46.3% | 450 | `8s / 8s / 8s / 8s` | 4.0 | **73.53%** | **147.06%** | `<default>`:450 |

#### Detailed Per-`LocalQueue` × Priority Dissection

<details>
<summary><b>LocalQueue <code>st-workloads/lq-scale-lender</code> → ClusterQueue <code>cq-scale-lender</code></b> (522 workloads)</summary>

| Priority Class | Priority Value | Workloads | Share of LQ | Total Pods | Duration (`P50 / AVG / P90 / P99`) | Cluster `google.com/tpu` Saturation | Cluster `cpu` Saturation |
| :--- | ---: | ---: | ---: | ---: | :---: | ---: | ---: |
| `<default>` | `0` | **522** | 100.0% | 522 | `8s / 8s / 10s / 10s` | 102.94% | 25.74% |

</details>

<details>
<summary><b>LocalQueue <code>st-workloads/lq-scale-borrower</code> → ClusterQueue <code>cq-scale-borrower</code></b> (450 workloads)</summary>

| Priority Class | Priority Value | Workloads | Share of LQ | Total Pods | Duration (`P50 / AVG / P90 / P99`) | Cluster `google.com/tpu` Saturation | Cluster `cpu` Saturation |
| :--- | ---: | ---: | ---: | ---: | :---: | ---: | ---: |
| `<default>` | `0` | **450** | 100.0% | 450 | `8s / 8s / 8s / 8s` | 73.53% | 18.38% |

</details>

### 7.3 Workload Execution Duration Histograms (Aggregated & Per Priority)

#### Aggregated Duration Histogram (All Unadmitted Workloads)

```text
=== ALL PRIORITIES (Aggregated) | count=972 | min=8s, p50=8s (0.1m), avg=8.1s (0.1m), p90=8s (0.1m), p99=10s (0.2m), max=10s ===
  < 1m   (0-60s)       |   972 (100.0%) | ████████████████████████████████
  1-5m   (60-300s)     |     0 (  0.0%) | 
  5-10m  (300-600s)    |     0 (  0.0%) | 
  10-15m (600-900s)    |     0 (  0.0%) | 
  15-20m (900-1200s)   |     0 (  0.0%) | 
  20-30m (1200-1800s)  |     0 (  0.0%) | 
```

#### Per-Priority Duration Histograms

```text
=== Priority: <default> (value=0) | count=972 | min=8s, p50=8s (0.1m), avg=8.1s (0.1m), p90=8s (0.1m), p99=10s (0.2m), max=10s ===
  < 1m   (0-60s)       |   972 (100.0%) | ████████████████████████████████
  1-5m   (60-300s)     |     0 (  0.0%) | 
  5-10m  (300-600s)    |     0 (  0.0%) | 
  10-15m (600-900s)    |     0 (  0.0%) | 
  15-20m (900-1200s)   |     0 (  0.0%) | 
  20-30m (1200-1800s)  |     0 (  0.0%) | 
```
