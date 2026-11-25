# Test Scenario Summary: `st49-scale-quota-fair-sharing-160n-1160w`

> **Auto-generated scenario analysis** from [`testscenarios/self-tests/st49-scale-quota-fair-sharing-160n-1160w/infra.json`](./infra.json), [`testscenarios/self-tests/st49-scale-quota-fair-sharing-160n-1160w/workflows.json`](./workflows.json), and [`testscenarios/self-tests/st49-scale-quota-fair-sharing-160n-1160w/execution-time.json`](./execution-time.json) via [`deploy/scripts/generate_scenario_readme.py`](../../../deploy/scripts/generate_scenario_readme.py).

## 1. Scenario Overview

| Dimension | Summary |
| :--- | :--- |
| **Scenario Directory** | `testscenarios/self-tests/st49-scale-quota-fair-sharing-160n-1160w` |
| **Raw Event Counts** | `infra.json`: **491** events · `workflows.json`: **1,160** events · `execution-time.json`: **2,320** events |
| **Cluster Capacity** | **160 nodes** · `5,120 cores` · `20,480 GiB (20.0 TiB)` · `1,280` `google.com/tpu` (1 distinct node group) |
| **Topology Configuration** | **0** `Topology` CRD(s) (none) |
| **Initial In-Flight Pods at $t_0$** | **160** pods across **160** / 160 nodes (initial `google.com/tpu` utilization: **100.00%**) |
| **Kueue Objects** | **1** `ResourceFlavor`(s) · **1** `Cohort`(s) · **4** `ClusterQueue`(s) · **4** `LocalQueue`(s) · **0** `WorkloadPriorityClass`(es) |
| **Test Arrival Window ($T_{\text{test}}$)** | **18.0s** (`2026-06-01T00:00:03Z` $\rightarrow$ `2026-06-01T00:00:21Z`) |
| **Unadmitted Workloads** | **1,000** `NotAdmitted` arrivals (1,000 total pods; gang size min=1, p50=1, p90=1, max=1) + **160** pre-admitted at $t_0$ |
| **Execution Duration Quantiles** | `min=8.0s`, `P50=8.0s`, `AVG=8.2s`, `P90=10s`, `P99=10s`, `max=10s` |
| **1. Saturation (Arrival Window $T_{\text{test}}$)** | `google.com/tpu`: **163.89%** · `cpu`: **40.97%** · `memory`: **40.97%** *(over $T_{\text{test}} = 18s$)* |
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

At scenario start ($t_0 = 2026-06-01T00:00:00Z$), **`160`** running `Pod` objects are pre-placed across **`160`** of the **`160`** cluster nodes (`100.0%` of nodes occupied; `0` nodes completely free).
Additionally, `workflows.json` records **`160`** pre-existing `Admitted` `Job` warmup workflow object(s) at $t_0$.

### 4.1 Initial Cluster Utilization Breakdown

| Resource | Initial Pods Requested | Cluster Allocatable | Free Allocatable at $t_0$ | Initial Utilization (% Allocatable) | Initial Utilization (% Capacity) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| `cpu` | **1,280 cores** | 5,120 cores | 3,840 cores | **25.00%** | 25.00% |
| `memory` | **5,120 GiB** | 20,480 GiB (20.0 TiB) | 15,360 GiB (15.0 TiB) | **25.00%** | 25.00% |
| `pods` | **160** | 17,600 | 17,440 | **0.91%** | 0.91% |
| `google.com/tpu` | **1,280** | 1,280 | 0 | **100.00%** | 100.00% |

### 4.2 Initial Pod Remaining Lifetimes & Request Profiles

- **Pods with Finite Remaining Lifetime (`deletionTime - t0`)**: **160** pods — `min=28s`, `P50=28s`, `AVG=28s`, `P90=28s`, `P99=28s`, `max=28s`
- **Pods per Occupied Node (`160` active nodes)**: `min=1`, `P50=1`, `AVG=1.00`, `P90=1`, `max=1`

| Rank | Initial Pod Resource Request Profile | Pod Count | Share of Initial Pods |
| :---: | :--- | ---: | ---: |
| `1` | `cpu: 8`, `google.com/tpu: 8`, `memory: 32Gi` | **160** | 100.0% |

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
| `cohort-scale-fs` | Yes | `*(root)*` | `1 (default)` | **4** (`cq-fs-q1`, `cq-fs-q2`, `cq-fs-q3`, `cq-fs-q4`) | `cpu`: 5,120 cores, `memory`: 20,480 GiB (20.0 TiB), `google.com/tpu`: 1,280 |

#### ClusterQueue Quotas, Preemption & Fair-Sharing Configuration

| ClusterQueue | Cohort | Queueing Strategy | FS Weight | Flavors | Nominal `cpu` | Nominal `memory` | Nominal `google.com/tpu` | Borrowing / Lending Limits | Preemption Policy (`withinCQ` / `reclaim` / `borrow`) |
| :--- | :--- | :--- | :---: | :--- | ---: | ---: | ---: | :--- | :--- |
| `cq-fs-q1` | `cohort-scale-fs` | `BestEffortFIFO` | `1` | `st-tpu-flavor` | **0 cores** | **0 GiB** | **0** | unbounded (default) | `Never` / `Any` / `Never` |
| `cq-fs-q2` | `cohort-scale-fs` | `BestEffortFIFO` | `1` | `st-tpu-flavor` | **0 cores** | **0 GiB** | **0** | unbounded (default) | `Never` / `Any` / `Never` |
| `cq-fs-q3` | `cohort-scale-fs` | `BestEffortFIFO` | `1` | `st-tpu-flavor` | **0 cores** | **0 GiB** | **0** | unbounded (default) | `Never` / `Any` / `Never` |
| `cq-fs-q4` | `cohort-scale-fs` | `BestEffortFIFO` | `1` | `st-tpu-flavor` | **0 cores** | **0 GiB** | **0** | unbounded (default) | `Never` / `Any` / `Never` |
| **Total Nominal Quota (CQs + Cohort)** | — | — | — | — | **5,120 cores** (100.0% of cluster) | **20,480 GiB (20.0 TiB)** (100.0% of cluster) | **1,280** (100.0% of cluster) | — | — |

### 5.4 Namespaces & LocalQueues (`Namespace` & `LocalQueue`)

- **Namespaces (`1`)**: `st-workloads`

| Namespace | LocalQueue Name | Target `ClusterQueue` (`spec.clusterQueue`) | Target CQ Exists? | Target Cohort | Unadmitted Workloads Submitted |
| :--- | :--- | :--- | :---: | :--- | ---: |
| `st-workloads` | `lq-fs-q1` | `cq-fs-q1` | Yes | `cohort-scale-fs` | **220** |
| `st-workloads` | `lq-fs-q2` | `cq-fs-q2` | Yes | `cohort-scale-fs` | **260** |
| `st-workloads` | `lq-fs-q3` | `cq-fs-q3` | Yes | `cohort-scale-fs` | **260** |
| `st-workloads` | `lq-fs-q4` | `cq-fs-q4` | Yes | `cohort-scale-fs` | **260** |

---

## 6. Scenario Timeline & Arrival Dynamics

| Timeline Metric | Value |
| :--- | :--- |
| **Infrastructure Bootstrap Timestamp ($t_0$)** | `2026-06-01T00:00:00Z` |
| **First Unadmitted Workload Arrival ($t_{\text{first}}$)** | `2026-06-01T00:00:03Z` (offset `+3.00s` from $t_0$) |
| **Last Unadmitted Workload Arrival ($t_{\text{last}}$)** | `2026-06-01T00:00:21Z` (offset `+21.00s` from $t_0$) |
| **Active Test Arrival Duration ($T_{\text{test}} = t_{\text{last}} - t_{\text{first}}$)** | **18.0s** |
| **Unadmitted Workload Submission Count** | **1,000** workloads (plus **160** pre-admitted at $t_0$) |
| **Mean Arrival Rate ($\lambda$)** | **3333.33 workloads/min** (**200000.0 workloads/hour**) |
| **Inter-Arrival Time ($\Delta t_i$) Distribution** | `min=0.00s`, `P50=0.00s`, `AVG=0.02s`, `P90=0.00s`, `P99=0.00s`, `max=8.00s` |

```text
=== Unadmitted Workload Arrival Distribution Across 12 Equal Time Windows (total=1,000, span=18.0s) ===
  [   0.0s -    1.5s] |   120 ( 12.0%) | ████████████
  [   1.5s -    3.0s] |     0 (  0.0%) | 
  [   3.0s -    4.5s] |     0 (  0.0%) | 
  [   4.5s -    6.0s] |     0 (  0.0%) | 
  [   6.0s -    7.5s] |     0 (  0.0%) | 
  [   7.5s -    9.0s] |   320 ( 32.0%) | ████████████████████████████████
  [   9.0s -   10.5s] |     0 (  0.0%) | 
  [  10.5s -   12.0s] |     0 (  0.0%) | 
  [  12.0s -   13.5s] |   280 ( 28.0%) | ████████████████████████████
  [  13.5s -   15.0s] |     0 (  0.0%) | 
  [  15.0s -   16.5s] |     0 (  0.0%) | 
  [  16.5s -   18.0s] |   280 ( 28.0%) | ████████████████████████████
```

---

## 7. Workload Priorities, Cluster Saturation, Per-LocalQueue Dissection & Duration Histograms

### 7.0 Three-Part Offered Cluster Saturation & Recommended `5m` / `10m` Replay Settings

For any resource $r \in \{\text{accelerators}, \text{cpu}, \text{memory}\}$ with physical cluster capacity $C_{\text{cluster}}(r)$ and total offered resource-seconds $W(r) = \sum_{w} \left(\text{size}_w(r) \times \text{duration}_w\right)$, let $T_{100\%}(r) = W(r) / C_{\text{cluster}}(r)$ denote the ideal `1.0x` full-cluster service time required at continuous `100%` utilization:

$$\text{Sat}_{\text{window}}(r) = \frac{W(r)}{T_{\text{window}} \times C_{\text{cluster}}(r)} \times 100\% = \frac{T_{100\%}(r)}{T_{\text{window}}} \times 100\%, \qquad N_{100\%}(T_{\text{target}}) = \frac{T_{100\%}(r^*)}{T_{\text{target}}}$$

#### Parts 1 & 2 — Offered Saturation: Active Arrival Window ($T_{\text{test}}$) vs. Default `6h` (`21,600s`) Duration

| Resource ($r$) | Physical Cluster Capacity $C_{\text{cluster}}(r)$ | Total Offered Work ($1.0\text{x}$) | Ideal `100%`-Util Service Time $T_{100\%}(r)$ | (1) Saturation over Arrival Window ($T_{\text{test}} = 18s$) | (2) Saturation over Default `6h` ($21{,}600\text{s}$) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **`google.com/tpu`** *(bottleneck)* | 1,280 | `10.49` `google.com/tpu`-h | `29.5s` | **163.89%** | **0.14%** |
| `cpu` | 5,120 cores | `10.49` core-h | `7.4s` | 40.97% | 0.03% |
| `memory` | 20,480 GiB (20.0 TiB) | `41.96` GiB-h | `7.4s` | 40.97% | 0.03% |

#### Part 3 — Recommended `testrunner.py` Settings for `100%` Saturation in `5m` (`300s`) & `10m` (`600s`)

| Target Wall-Clock Window ($T_{\text{target}}$) | Bottleneck Resource ($r^*$) | Recommended `--speedup` ($N_{100\%}$) | Recommended `--duration` | Compressed Arrival Span ($T_{\text{test}} / N_{100\%}$) | Execution & Drain Behavior |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **`5m` (`300s`)** | `google.com/tpu` | **`--speedup 0.098`** | **`--duration 5m`** | `183.1s` | All arrivals submit within `183.1s` (≤ `300s`); `--duration 5m` captures `100%` of arrivals and `100.0%` full-cluster work in `5m`. |
| **`10m` (`600s`)** | `google.com/tpu` | **`--speedup 0.049`** | **`--duration 10m`** | `366.1s` | All arrivals submit within `366.1s` (≤ `600s`); `--duration 10m` captures `100%` of arrivals and `100.0%` full-cluster work in `10m`. |

```bash
# Run at 100% offered google.com/tpu saturation in a 5-minute (300s) wall-clock window:
python3 deploy/scripts/testrunner.py --scenario testscenarios/self-tests/st49-scale-quota-fair-sharing-160n-1160w --speedup 0.098 --duration 5m

# Run at 100% offered google.com/tpu saturation in a 10-minute (600s) wall-clock window:
python3 deploy/scripts/testrunner.py --scenario testscenarios/self-tests/st49-scale-quota-fair-sharing-160n-1160w --speedup 0.049 --duration 10m
```

### 7.1 Workload Breakdown & Cluster Saturation by Priority Class

| Priority Class | Priority Value | Workloads | Share | Total Pods | Gang Size (`min / P50 / P90 / max`) | Duration (`P50 / AVG / P90 / P99`) | Offered `google.com/tpu`-Hours | `google.com/tpu` Saturation | `cpu` Saturation | `memory` Saturation |
| :--- | ---: | ---: | ---: | ---: | :---: | :---: | ---: | ---: | ---: | ---: |
| `<default>` | `0` | 1,000 | 100.0% | 1,000 | `1 / 1 / 1 / 1` | `8s / 8s / 10s / 10s` | 10.5 | **163.89%** | 40.97% | 40.97% |
| **ALL (Total)** | — | **1,000** | 100.0% | 1,000 | `1 / 1 / 1 / 1` | `8s / 8s / 10s / 10s` | 10.5 | **163.89%** | **40.97%** | **40.97%** |

### 7.2 Workload Dissection per `LocalQueue` (and Target `ClusterQueue`)

| LocalQueue (`ns/name`) | Target `ClusterQueue` | Workloads | Share | Total Pods | Duration (`P50 / AVG / P90 / P99`) | Offered `google.com/tpu`-h | Cluster `google.com/tpu` Saturation | Target CQ Nominal Quota Saturation | Priority Breakdown (Count) |
| :--- | :--- | ---: | ---: | ---: | :---: | ---: | ---: | ---: | :--- |
| `st-workloads/lq-fs-q2` | `cq-fs-q2` | **260** | 26.0% | 260 | `8s / 8s / 10s / 10s` | 2.8 | **44.44%** | n/a | `<default>`:260 |
| `st-workloads/lq-fs-q3` | `cq-fs-q3` | **260** | 26.0% | 260 | `8s / 8s / 10s / 10s` | 2.8 | **44.44%** | n/a | `<default>`:260 |
| `st-workloads/lq-fs-q4` | `cq-fs-q4` | **260** | 26.0% | 260 | `8s / 8s / 10s / 10s` | 2.8 | **44.44%** | n/a | `<default>`:260 |
| `st-workloads/lq-fs-q1` | `cq-fs-q1` | **220** | 22.0% | 220 | `8s / 8s / 8s / 8s` | 2.0 | **30.56%** | n/a | `<default>`:220 |

#### Detailed Per-`LocalQueue` × Priority Dissection

<details>
<summary><b>LocalQueue <code>st-workloads/lq-fs-q2</code> → ClusterQueue <code>cq-fs-q2</code></b> (260 workloads)</summary>

| Priority Class | Priority Value | Workloads | Share of LQ | Total Pods | Duration (`P50 / AVG / P90 / P99`) | Cluster `google.com/tpu` Saturation | Cluster `cpu` Saturation |
| :--- | ---: | ---: | ---: | ---: | :---: | ---: | ---: |
| `<default>` | `0` | **260** | 100.0% | 260 | `8s / 8s / 10s / 10s` | 44.44% | 11.11% |

</details>

<details>
<summary><b>LocalQueue <code>st-workloads/lq-fs-q3</code> → ClusterQueue <code>cq-fs-q3</code></b> (260 workloads)</summary>

| Priority Class | Priority Value | Workloads | Share of LQ | Total Pods | Duration (`P50 / AVG / P90 / P99`) | Cluster `google.com/tpu` Saturation | Cluster `cpu` Saturation |
| :--- | ---: | ---: | ---: | ---: | :---: | ---: | ---: |
| `<default>` | `0` | **260** | 100.0% | 260 | `8s / 8s / 10s / 10s` | 44.44% | 11.11% |

</details>

<details>
<summary><b>LocalQueue <code>st-workloads/lq-fs-q4</code> → ClusterQueue <code>cq-fs-q4</code></b> (260 workloads)</summary>

| Priority Class | Priority Value | Workloads | Share of LQ | Total Pods | Duration (`P50 / AVG / P90 / P99`) | Cluster `google.com/tpu` Saturation | Cluster `cpu` Saturation |
| :--- | ---: | ---: | ---: | ---: | :---: | ---: | ---: |
| `<default>` | `0` | **260** | 100.0% | 260 | `8s / 8s / 10s / 10s` | 44.44% | 11.11% |

</details>

<details>
<summary><b>LocalQueue <code>st-workloads/lq-fs-q1</code> → ClusterQueue <code>cq-fs-q1</code></b> (220 workloads)</summary>

| Priority Class | Priority Value | Workloads | Share of LQ | Total Pods | Duration (`P50 / AVG / P90 / P99`) | Cluster `google.com/tpu` Saturation | Cluster `cpu` Saturation |
| :--- | ---: | ---: | ---: | ---: | :---: | ---: | ---: |
| `<default>` | `0` | **220** | 100.0% | 220 | `8s / 8s / 8s / 8s` | 30.56% | 7.64% |

</details>

### 7.3 Workload Execution Duration Histograms (Aggregated & Per Priority)

#### Aggregated Duration Histogram (All Unadmitted Workloads)

```text
=== ALL PRIORITIES (Aggregated) | count=1,000 | min=8s, p50=8s (0.1m), avg=8.2s (0.1m), p90=10s (0.2m), p99=10s (0.2m), max=10s ===
  < 1m   (0-60s)       | 1,000 (100.0%) | ████████████████████████████████
  1-5m   (60-300s)     |     0 (  0.0%) | 
  5-10m  (300-600s)    |     0 (  0.0%) | 
  10-15m (600-900s)    |     0 (  0.0%) | 
  15-20m (900-1200s)   |     0 (  0.0%) | 
  20-30m (1200-1800s)  |     0 (  0.0%) | 
```

#### Per-Priority Duration Histograms

```text
=== Priority: <default> (value=0) | count=1,000 | min=8s, p50=8s (0.1m), avg=8.2s (0.1m), p90=10s (0.2m), p99=10s (0.2m), max=10s ===
  < 1m   (0-60s)       | 1,000 (100.0%) | ████████████████████████████████
  1-5m   (60-300s)     |     0 (  0.0%) | 
  5-10m  (300-600s)    |     0 (  0.0%) | 
  10-15m (600-900s)    |     0 (  0.0%) | 
  15-20m (900-1200s)   |     0 (  0.0%) | 
  20-30m (1200-1800s)  |     0 (  0.0%) | 
```
