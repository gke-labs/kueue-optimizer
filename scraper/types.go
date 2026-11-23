// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"
)

// EventType represents the lifecycle event type recorded by the scraper.
type EventType string

const (
	EventNew     EventType = "New"
	EventUpdate  EventType = "Update"
	EventDeleted EventType = "Deleted"
)

// ExecutionEventType represents the event type recorded in execution-time.json.
type ExecutionEventType string

const (
	ExecutionEventAdmitted ExecutionEventType = "Admitted"
	ExecutionEventPartial  ExecutionEventType = "PartialExecution"
	ExecutionEventFull     ExecutionEventType = "FullExecution"
)

// WorkloadState represents the Kueue admission state of a workload (Job, JobSet, etc.).
type WorkloadState string

const (
	WorkloadStateNotAdmitted         WorkloadState = "NotAdmitted"
	WorkloadStateAdmissionInProgress WorkloadState = "AdmissionInProgress"
	WorkloadStateAdmitted            WorkloadState = "Admitted"
)

// DefaultPauseImage is the replacement container image used when stripping
// business-specific container commands and images from workflow specifications.
const DefaultPauseImage = "registry.k8s.io/pause:3.10"

// DefaultKueueNamespace and DefaultKueueConfigMapName identify the standard
// ConfigMap holding Kueue's controller_manager_config.yaml Configuration document.
const (
	DefaultKueueNamespace     = "kueue-system"
	DefaultKueueConfigMapName = "kueue-manager-config"
)

// Config holds runtime configuration for the Kubernetes and Kueue load test scraper.
type Config struct {
	Kubeconfig         string
	Interval           time.Duration
	Duration           time.Duration
	HashNames          bool
	PauseImage         string
	ExtraHashLabels    []string
	HashSalt           []byte
	InfraFile          string
	WorkflowsFile      string
	ExecutionTimeFile  string
	KueueNamespace     string
	KueueConfigMapName string
}

// InfraEvent represents a single event stored in infra.json.
// Event metadata (timestamp and eventType) is separated from the object payload.
type InfraEvent struct {
	Timestamp time.Time `json:"timestamp"`
	EventType EventType `json:"eventType"`
	Payload   any       `json:"payload"`
}

// ScrapedPodPayload contains strictly the allowed fields for scheduled and running pods:
// placement, resource request, scheduling time, first notice time, deletion time, and
// optional sanitized Kueue workload/owner linkage metadata for startup pod-to-quota sync
// (plus apiVersion, kind, name, and namespace to identify the pod object).
// No container information, args, commands, secrets, volumes, annotations, or labels are stored.
type ScrapedPodPayload struct {
	APIVersion      string              `json:"apiVersion"`
	Kind            string              `json:"kind"`
	Name            string              `json:"name"`
	Namespace       string              `json:"namespace"`
	Placement       string              `json:"placement"`
	ResourceRequest corev1.ResourceList `json:"resourceRequest"`
	SchedulingTime  *metav1.Time        `json:"schedulingTime,omitempty"`
	FirstNoticeTime metav1.Time         `json:"firstNoticeTime"`
	DeletionTime    *metav1.Time        `json:"deletionTime,omitempty"`
	WorkloadName    string              `json:"workloadName,omitempty"`
	OwnerKind       string              `json:"ownerKind,omitempty"`
	OwnerName       string              `json:"ownerName,omitempty"`
	PodSetName      string              `json:"podSetName,omitempty"`
	IsTAS           bool                `json:"isTAS,omitempty"`
}

// WorkflowEvent represents a single event stored in workflows.json for Jobs, JobSets,
// LeaderWorkerSets, Deployments, RayJobs, RayClusters, and standalone Kueue Pods.
//
// Note: The term "Workflow" (and workflows.json) is used throughout the scraper to refer
// to these user-submitted Kubernetes compute objects so they remain distinct from Kueue's
// internal Workload CRD (kueue.x-k8s.io/v1beta2), which is tracked separately for admission
// and execution timings in execution-time.json.
//
// Event metadata (timestamp, eventType, and workloadState) is separated from the workflow payload.
type WorkflowEvent struct {
	Timestamp     time.Time       `json:"timestamp"`
	EventType     EventType       `json:"eventType"`
	WorkloadState WorkloadState   `json:"workloadState"`
	Payload       WorkflowPayload `json:"payload"`
}

// WorkflowPayload holds the recorded workflow object along with its creation time and LocalQueue name.
type WorkflowPayload struct {
	ObjectKind     string      `json:"objectKind"`
	Name           string      `json:"name"`
	Namespace      string      `json:"namespace"`
	CreationTime   metav1.Time `json:"creationTime"`
	LocalQueueName string      `json:"localQueueName"`
	Object         any         `json:"object"`
}

// ExecutionTimeEvent represents an admission, partial execution (preempted), or
// full execution (finished without preemption) record in execution-time.json.
// Event metadata (timestamp, eventType, and workloadState) is separated from the execution time payload.
type ExecutionTimeEvent struct {
	Timestamp     time.Time            `json:"timestamp"`
	EventType     ExecutionEventType   `json:"eventType"`
	WorkloadState WorkloadState        `json:"workloadState"`
	Payload       ExecutionTimePayload `json:"payload"`
}

// Interruption reason constants recorded on PartialExecution events in execution-time.json.
const (
	InterruptionReasonPreempted        = "Preempted"
	InterruptionReasonNodeFailure      = "NodeFailure"
	InterruptionReasonPodsReadyTimeout = "PodsReadyTimeout"
	InterruptionReasonEvicted          = "Evicted"
	InterruptionReasonFailed           = "Failed"
	InterruptionReasonDeleted          = "Deleted"
	InterruptionReasonScrapeEnded      = "ScrapeEnded"
)

// ExecutionTimePayload holds the Kueue Workload admission, partial execution, or full execution details.
type ExecutionTimePayload struct {
	ExecutionType             string                          `json:"executionType,omitempty"`
	WorkloadName              string                          `json:"workloadName"`
	Namespace                 string                          `json:"namespace"`
	OwnerKind                 string                          `json:"ownerKind,omitempty"`
	OwnerName                 string                          `json:"ownerName,omitempty"`
	OwnerAPIVersion           string                          `json:"ownerAPIVersion,omitempty"`
	LocalQueueName            string                          `json:"localQueueName,omitempty"`
	ClusterQueueName          string                          `json:"clusterQueueName,omitempty"`
	PriorityClassName         string                          `json:"priorityClassName,omitempty"`
	Priority                  *int32                          `json:"priority,omitempty"`
	PodSetAssignments         []kueuev1beta2.PodSetAssignment `json:"podSetAssignments,omitempty"`
	UnhealthyNodes            []string                        `json:"unhealthyNodes,omitempty"`
	CreationTime              metav1.Time                     `json:"creationTime"`
	AdmittedTime              metav1.Time                     `json:"admittedTime"`
	EndTime                   *metav1.Time                    `json:"endTime,omitempty"`
	ExecutionDurationSeconds  *float64                        `json:"executionDurationSeconds,omitempty"`
	ExecutionDuration         string                          `json:"executionDuration,omitempty"`
	Preempted                 bool                            `json:"preempted"`
	PreemptionCount           int                             `json:"preemptionCount,omitempty"`
	InterruptionReason        string                          `json:"interruptionReason,omitempty"`
	InterruptionCount         int                             `json:"interruptionCount,omitempty"`
	TrueDurationSeconds       *float64                        `json:"trueDurationSeconds,omitempty"`
	MissingDurationMultiplier *float64                        `json:"missingDurationMultiplier,omitempty"`
	Reason                    string                          `json:"reason,omitempty"`
	Message                   string                          `json:"message,omitempty"`
}

// JSONEventLog writes events in JSON Lines (NDJSON) format on disk — one compact,
// single-line JSON object per line (`{"timestamp":...,"eventType":...,"payload":{...}}\n`)
// so large files can be parsed line-by-line without loading the entire file into memory.
type JSONEventLog struct {
	mu    sync.Mutex
	path  string
	file  *os.File
	count int
}

// NewJSONEventLog creates or truncates the target JSON Lines file.
func NewJSONEventLog(path string) (*JSONEventLog, error) {
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create directory %s: %w", dir, err)
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("sync %s: %w", path, err)
	}
	return &JSONEventLog{
		path: path,
		file: f,
	}, nil
}

// Append marshals event as a single-line compact JSON object and appends it as one line to the file.
func (l *JSONEventLog) Append(event any) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file == nil {
		return fmt.Errorf("event log %s is closed", l.path)
	}

	raw, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event for %s: %w", l.path, err)
	}
	raw = append(raw, '\n')
	if _, err := l.file.Write(raw); err != nil {
		return fmt.Errorf("append event to %s: %w", l.path, err)
	}
	l.count++
	return nil
}

// Sync flushes buffered kernel file data to stable storage.
func (l *JSONEventLog) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file == nil {
		return nil
	}
	return l.file.Sync()
}

// Count returns the total number of events written so far.
func (l *JSONEventLog) Count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.count
}

// Close flushes and closes the underlying file.
func (l *JSONEventLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file == nil {
		return nil
	}
	_ = l.file.Sync()
	err := l.file.Close()
	l.file = nil
	return err
}

// PrintStartupBanner prints the exact scraping configuration, omitted fields,
// and SHA-256 name hashing behavior at startup.
func PrintStartupBanner(w io.Writer, cfg Config) {
	var b strings.Builder
	b.WriteString("================================================================================\n")
	b.WriteString("Kubernetes & Kueue Load Test Cluster Scraper\n")
	b.WriteString("================================================================================\n")
	b.WriteString(fmt.Sprintf("Periodic Scrape Interval : %s\n", cfg.Interval))
	if cfg.Duration > 0 {
		b.WriteString(fmt.Sprintf("Run Duration Limit       : %s\n", cfg.Duration))
	} else {
		b.WriteString("Run Duration Limit       : until interrupted (SIGINT/SIGTERM)\n")
	}
	b.WriteString(fmt.Sprintf("Pause Container Image    : %s\n", cfg.PauseImage))
	if len(cfg.ExtraHashLabels) > 0 {
		b.WriteString(fmt.Sprintf("Extra Hashed Labels      : %s\n", strings.Join(cfg.ExtraHashLabels, ", ")))
	}
	b.WriteString("\n")
	b.WriteString("Output JSON Event Logs:\n")
	b.WriteString(fmt.Sprintf("  1. %-22s : Nodes, Namespaces, RuntimeClasses, ClusterQueues, Cohorts, LocalQueues, ResourceFlavors, Topologies, WorkloadPriorityClasses, and Scheduled/Running Pods\n", cfg.InfraFile))
	b.WriteString(fmt.Sprintf("  2. %-22s : Jobs, JobSets, LeaderWorkerSets, Kueue-managed Deployments, RayJobs, RayClusters, and standalone Kueue-managed Pods\n", cfg.WorkflowsFile))
	b.WriteString(fmt.Sprintf("  3. %-22s : Kueue Workload admission, partial execution (preempted), and full execution (completed without preemption)\n", cfg.ExecutionTimeFile))
	b.WriteString("\n")
	b.WriteString("Objects Being Scraped:\n")
	b.WriteString("  - Nodes (v1/Node)                                        : Sanitized scheduling-relevant spec & status (periodic)\n")
	b.WriteString("  - Namespaces (v1/Namespace)                              : Sanitized object information without status (periodic)\n")
	b.WriteString("  - RuntimeClasses (node.k8s.io/v1)                        : Sanitized object information without status (periodic)\n")
	b.WriteString("  - ClusterQueues (kueue.x-k8s.io/v1beta2)                 : Sanitized object information without status (periodic)\n")
	b.WriteString("  - Cohorts (kueue.x-k8s.io/v1beta2)                       : Sanitized object information without status (periodic)\n")
	b.WriteString("  - LocalQueues (kueue.x-k8s.io/v1beta2)                   : Sanitized object information without status (periodic)\n")
	b.WriteString("  - ResourceFlavors (kueue.x-k8s.io/v1beta2)               : Sanitized object information without status (periodic)\n")
	b.WriteString("  - Topologies (kueue.x-k8s.io/v1beta2)                    : Sanitized object information without status (periodic)\n")
	b.WriteString("  - WorkloadPriorityClasses (kueue.x-k8s.io/v1beta2)       : Sanitized object information without status (periodic)\n")
	b.WriteString("  - Scheduled & Running Pods (v1/Pod)                      : Placement (nodeName), resource request, scheduling time, first notice time, and deletion time via watch\n")
	b.WriteString("  - Workflow Creations/Updates (via watch)                 : Jobs, JobSets, LeaderWorkerSets, Kueue-managed Deployments, RayJobs, RayClusters, and standalone Kueue-managed Pods (recording creation time and LocalQueue name)\n")
	b.WriteString("  - Kueue Workloads (kueue.x-k8s.io/v1beta2, watch)        : Admission timestamp, partial execution (on preemption), and full execution (on completion without preemption)\n")
	b.WriteString("\n")
	b.WriteString("Information Omitted / Sanitized:\n")
	b.WriteString("  - Nodes (in infra.json):\n")
	b.WriteString("      * Retained spec/status: spec.{taints, unschedulable} and status.{capacity, allocatable, conditions, phase}\n")
	b.WriteString("      * Omitted: status.{images, addresses, nodeInfo, volumesInUse, volumesAttached, daemonEndpoints, config, runtimeHandlers, features}, spec.{providerID, podCIDR, podCIDRs, configSource}, metadata.managedFields, cloud/cluster-identifying labels, and disallowed/sensitive annotations (sensitive/--extra-hash-labels label values hashed when --hash-names is enabled)\n")
	b.WriteString("  - Namespaces, RuntimeClasses, ClusterQueues, Cohorts, LocalQueues, ResourceFlavors, Topologies, WorkloadPriorityClasses:\n")
	b.WriteString("      * Omitted: status subresource, WorkloadPriorityClass description, metadata.managedFields, and disallowed/sensitive annotations (sensitive/--extra-hash-labels label values hashed when --hash-names is enabled)\n")
	b.WriteString("  - Scheduled & Running Pods (in infra.json):\n")
	b.WriteString("      * Omitted: ALL fields except placement, resource request, scheduling time, first notice time, and deletion time\n")
	b.WriteString("      * Specifically omitted: container specs/statuses, images, execution arguments, commands, secrets, volumes, annotations, and labels\n")
	b.WriteString("  - Workflow Objects (Jobs, JobSets, LeaderWorkerSets, Deployments, RayJobs, RayClusters, standalone Kueue Pods in workflows.json):\n")
	b.WriteString(fmt.Sprintf("      * Command line / container image : Replaced with pause container (%s) and command cleared\n", cfg.PauseImage))
	b.WriteString("      * Command line arguments         : Omitted (args, Ray entrypoint/runtimeEnvYAML/rayStartParams cleared)\n")
	b.WriteString("      * Probes & lifecycle hooks       : Omitted (livenessProbe, readinessProbe, startupProbe, lifecycle)\n")
	b.WriteString("      * Pod identity & network names   : Omitted (serviceAccountName, serviceAccount, hostname, hostnameOverride, subdomain, hostAliases, dnsConfig, JobSet network.subdomain)\n")
	b.WriteString("      * Volumes                        : Omitted (volumes, volumeMounts, volumeDevices, volumeClaimTemplates, volumeClaimPolicies)\n")
	b.WriteString("      * Secrets                        : Omitted (imagePullSecrets, secret volumes/env)\n")
	b.WriteString("      * Environment variables          : Omitted (env, envFrom)\n")
	b.WriteString("      * Annotations                    : Omitted unless domain is kubernetes.io, k8s.io, x-k8s.io, or kueue-optimizer.gke.io (or their subdomains, excluding sensitive keys like kubectl.kubernetes.io/last-applied-configuration)\n")
	b.WriteString("      * Labels                         : Values hashed with per-run HMAC-SHA256 when --hash-names is enabled if key matches sensitive user label list (owner, team, customer, ...), workload-name labels, or --extra-hash-labels (across metadata, selectors, and pod/node affinities)\n")
	b.WriteString("      * Status & managedFields         : Omitted (stored as submitted to the cluster)\n")
	b.WriteString("\n")
	b.WriteString("SHA-256 Name Replacement & Label Hashing (per-run HMAC-SHA256):\n")
	b.WriteString(fmt.Sprintf("  - Enabled                            : %t\n", cfg.HashNames))
	b.WriteString("  - Objects with SHA-256 hashed names  : Workloads, Pods, Jobs, JobSets, LeaderWorkerSets, Deployments, RayJobs, RayClusters\n")
	b.WriteString("  - Objects NEVER hashed (plain names) : Namespaces, Nodes, ClusterQueues, Cohorts, LocalQueues, ResourceFlavors, Topologies, WorkloadPriorityClasses, RuntimeClasses\n")
	b.WriteString("  - Hashed label values (when enabled) : Sensitive organizational labels (owner, team, customer, ...), workload-referencing labels, and --extra-hash-labels\n")
	b.WriteString("================================================================================\n")
	_, _ = io.WriteString(w, b.String())
}
