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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"
	"strings"
	"time"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"
	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	leaderworkersetv1 "sigs.k8s.io/lws/api/leaderworkerset/v1"
)

const (
	KueueQueueLabel             = "kueue.x-k8s.io/queue-name"
	KueueManagedLabel           = "kueue.x-k8s.io/managed"
	KueuePrebuiltWorkloadLabel  = "kueue.x-k8s.io/prebuilt-workload-name"
	KueueJobOwnerNameAnnotation = "kueue.x-k8s.io/job-owner-name"
	KueueJobOwnerGVKAnnotation  = "kueue.x-k8s.io/job-owner-gvk"
	KueuePodGroupNameLabel      = "kueue.x-k8s.io/pod-group-name"
	KueueSuspendedByParentAnnot = "kueue.x-k8s.io/pod-suspending-parent"
	KueueAdmissionGateName      = "kueue.x-k8s.io/admission"
	KueueTopologyGateName       = "kueue.x-k8s.io/topology"
	KueueWorkloadAnnot          = "kueue.x-k8s.io/workload"
	KueueWorkloadSliceNameAnnot = "kueue.x-k8s.io/workload-slice-name"
)

// nonHashedKinds lists object kinds whose names must NEVER be hashed even when --hash-names is enabled.
var nonHashedKinds = map[string]bool{
	"Namespace":      true,
	"Node":           true,
	"ClusterQueue":   true,
	"LocalQueue":     true,
	"ResourceFlavor": true,
	"Topology":       true,
}

// labelKeysHoldingWorkloadNames lists label keys whose values represent workload/job/pod/cluster names
// that should be hashed when SHA-256 name replacement is enabled.
var labelKeysHoldingWorkloadNames = map[string]bool{
	"job-name":                         true,
	"batch.kubernetes.io/job-name":     true,
	"jobset.sigs.k8s.io/jobset-name":   true,
	"leaderworkerset.sigs.k8s.io/name": true,
	"ray.io/cluster":                   true,
	"app.kubernetes.io/instance":       true,
	KueuePrebuiltWorkloadLabel:         true,
	KueuePodGroupNameLabel:             true,
}

// annotationKeysHoldingWorkloadNames lists annotation keys whose values represent workload/job/pod names
// that should be hashed when SHA-256 name replacement is enabled.
var annotationKeysHoldingWorkloadNames = map[string]bool{
	KueueJobOwnerNameAnnotation:      true,
	KueuePrebuiltWorkloadLabel:       true,
	KueuePodGroupNameLabel:           true,
	KueueWorkloadAnnot:               true,
	KueueWorkloadSliceNameAnnot:      true,
	"jobset.sigs.k8s.io/jobset-name": true,
}

// parentWorkflowKinds lists Kubernetes/CRD kinds for the workflow APIs tracked separately from standalone Pods.
var parentWorkflowKinds = map[string]bool{
	"Job":             true,
	"JobSet":          true,
	"LeaderWorkerSet": true,
	"Deployment":      true,
	"ReplicaSet":      true,
	"StatefulSet":     true,
	"RayJob":          true,
	"RayCluster":      true,
}

// HashName returns the hex-encoded SHA-256 digest of name when enabled is true and name is non-empty.
func HashName(name string, enabled bool) string {
	if !enabled || name == "" {
		return name
	}
	sum := sha256.Sum256([]byte(name))
	return hex.EncodeToString(sum[:])
}

// HashNameForKind hashes name only if the object kind is a workload/pod/job/jobset/etc.
// Names of namespaces, nodes, queues, resource flavors, and topologies are never hashed.
func HashNameForKind(kind, name string, enabled bool) string {
	if !enabled || name == "" || nonHashedKinds[kind] {
		return name
	}
	return HashName(name, true)
}

// allowedAnnotationDomains lists the official DNS domains (matched exactly or as a dot-separated
// subdomain suffix) whose annotations are retained on sanitized workflow objects.
var allowedAnnotationDomains = []string{
	"kubernetes.io",
	"k8s.io",
	"x-k8s.io",
	"kueue-optimizer.gke.io",
}

// deniedAnnotationKeys lists specific annotation keys within allowed domains that must always
// be stripped because they can embed unsanitized manifests, CLI commands, container specs,
// cloud project/volume identifiers, or internal node IP addresses.
var deniedAnnotationKeys = []string{
	strings.ToLower(corev1.LastAppliedConfigAnnotation), // kubectl.kubernetes.io/last-applied-configuration
	"kubernetes.io/change-cause",
	"pod.alpha.kubernetes.io/init-containers",
	"pod.beta.kubernetes.io/init-containers",
	"pod.alpha.kubernetes.io/init-container-statuses",
	"pod.beta.kubernetes.io/init-container-statuses",
	"csi.volume.kubernetes.io/nodeid",
	"alpha.kubernetes.io/provided-node-ip",
}

// deniedNodeLabelKeys lists cloud-provider and cluster-identifying Node label keys (all lowercase)
// that can leak cluster names, cloud project/subscription IDs, VM instance IDs, or node service
// accounts, while preserving scheduling, hardware, and topology labels needed by ResourceFlavor,
// Topology, and Pod nodeSelector/nodeAffinity.
var deniedNodeLabelKeys = []string{
	"cloud.google.com/gke-cluster-name",
	"cloud.google.com/gke-boot-disk",
	"cloud.google.com/project",
	"project.cloud.google.com/id",
	"iam.gke.io/node-service-account",
	"topology.gke.io/cluster",
	"alpha.eksctl.io/cluster-name",
	"alpha.eksctl.io/instance-id",
	"eks.amazonaws.com/nodegroup-image",
	"kubernetes.azure.com/cluster",
	"kubernetes.azure.com/subscription-id",
}

// ShouldKeepAnnotation returns true iff key is a domain-prefixed annotation ("<domain>/<name>")
// whose domain is kubernetes.io, k8s.io, x-k8s.io, or kueue-optimizer.gke.io (or a subdomain thereof),
// and key is not on the sensitive annotation denylist (e.g. kubectl.kubernetes.io/last-applied-configuration).
func ShouldKeepAnnotation(key string) bool {
	lower := strings.ToLower(key)
	if slices.Contains(deniedAnnotationKeys, lower) {
		return false
	}
	domain, name, hasSlash := strings.Cut(lower, "/")
	if !hasSlash || domain == "" || name == "" || strings.Contains(name, "/") {
		return false
	}
	for _, allowed := range allowedAnnotationDomains {
		if domain == allowed || strings.HasSuffix(domain, "."+allowed) {
			return true
		}
	}
	return false
}

// FilterAnnotations retains only allowed Kubernetes, SIG (x-k8s.io / k8s.io), and kueue-optimizer
// domain annotations (excluding sensitive keys like kubectl.kubernetes.io/last-applied-configuration),
// and hashes any annotation value that holds a workload/job/pod name when hashNames is true.
func FilterAnnotations(annotations map[string]string, hashNames bool) map[string]string {
	if len(annotations) == 0 {
		return nil
	}
	filtered := make(map[string]string)
	for k, v := range annotations {
		if !ShouldKeepAnnotation(k) {
			continue
		}
		if hashNames && annotationKeysHoldingWorkloadNames[strings.ToLower(k)] && v != "" {
			v = HashName(v, true)
		}
		filtered[k] = v
	}
	if len(filtered) == 0 {
		return nil
	}
	return filtered
}

// FilterLabels copies labels while omitting any key present in deniedKeys (entries in deniedKeys must be lowercase).
func FilterLabels(labels map[string]string, deniedKeys []string) map[string]string {
	if len(labels) == 0 {
		return nil
	}
	out := make(map[string]string, len(labels))
	for k, v := range labels {
		if slices.Contains(deniedKeys, strings.ToLower(k)) {
			continue
		}
		out[k] = v
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// SanitizeLabels copies labels and hashes values that store workload/job/pod names when hashNames is true.
// Queue names (kueue.x-k8s.io/queue-name) are never hashed.
func SanitizeLabels(labels map[string]string, hashNames bool) map[string]string {
	if len(labels) == 0 {
		return nil
	}
	out := make(map[string]string, len(labels))
	for k, v := range labels {
		if hashNames && labelKeysHoldingWorkloadNames[k] && v != "" {
			v = HashName(v, true)
		}
		out[k] = v
	}
	return out
}

// SanitizeOwnerReferences copies owner references and hashes the owner name if hashNames is true
// and the owner is not a non-hashed infrastructure resource.
func SanitizeOwnerReferences(refs []metav1.OwnerReference, hashNames bool) []metav1.OwnerReference {
	if len(refs) == 0 {
		return nil
	}
	out := make([]metav1.OwnerReference, len(refs))
	for i, r := range refs {
		out[i] = r
		if hashNames && !nonHashedKinds[r.Kind] {
			out[i].Name = HashName(r.Name, true)
		}
	}
	return out
}

// SanitizeWorkflowObjectMeta sanitizes top-level ObjectMeta on workflow objects.
func SanitizeWorkflowObjectMeta(meta *metav1.ObjectMeta, hashNames bool) {
	meta.ManagedFields = nil
	if hashNames {
		meta.Name = HashName(meta.Name, true)
		if meta.GenerateName != "" {
			meta.GenerateName = HashName(meta.GenerateName, true)
		}
	}
	meta.Annotations = FilterAnnotations(meta.Annotations, hashNames)
	meta.Labels = SanitizeLabels(meta.Labels, hashNames)
	meta.OwnerReferences = SanitizeOwnerReferences(meta.OwnerReferences, hashNames)
}

// SanitizePodSpec strips business-specific fields from a PodSpec:
//   - Replaces container images with pauseImage and clears Command and Args
//   - Removes container probes (LivenessProbe, ReadinessProbe, StartupProbe) and Lifecycle hooks
//   - Clears ServiceAccountName (and DeprecatedServiceAccount), Hostname, HostnameOverride, and Subdomain
//   - Removes Volumes, VolumeMounts, and VolumeDevices
//   - Removes Secrets (ImagePullSecrets, secret env/volumes)
//   - Removes Environment variables (Env and EnvFrom)
func SanitizePodSpec(spec *corev1.PodSpec, pauseImage string) {
	if spec == nil {
		return
	}
	if pauseImage == "" {
		pauseImage = DefaultPauseImage
	}
	spec.Volumes = nil
	spec.ImagePullSecrets = nil
	spec.ServiceAccountName = ""
	spec.DeprecatedServiceAccount = ""
	spec.Hostname = ""
	spec.HostnameOverride = nil
	spec.Subdomain = ""

	for i := range spec.Containers {
		sanitizeContainer(&spec.Containers[i], pauseImage)
	}
	for i := range spec.InitContainers {
		sanitizeContainer(&spec.InitContainers[i], pauseImage)
	}
	for i := range spec.EphemeralContainers {
		sanitizeEphemeralContainer(&spec.EphemeralContainers[i], pauseImage)
	}
}

func sanitizeContainer(c *corev1.Container, pauseImage string) {
	c.Image = pauseImage
	c.Command = nil
	c.Args = nil
	c.Env = nil
	c.EnvFrom = nil
	c.VolumeMounts = nil
	c.VolumeDevices = nil
	c.LivenessProbe = nil
	c.ReadinessProbe = nil
	c.StartupProbe = nil
	c.Lifecycle = nil
}

func sanitizeEphemeralContainer(c *corev1.EphemeralContainer, pauseImage string) {
	c.Image = pauseImage
	c.Command = nil
	c.Args = nil
	c.Env = nil
	c.EnvFrom = nil
	c.VolumeMounts = nil
	c.VolumeDevices = nil
	c.LivenessProbe = nil
	c.ReadinessProbe = nil
	c.StartupProbe = nil
	c.Lifecycle = nil
}

// SanitizePodTemplateSpec sanitizes both the template ObjectMeta and PodSpec.
func SanitizePodTemplateSpec(tpl *corev1.PodTemplateSpec, pauseImage string, hashNames bool) {
	if tpl == nil {
		return
	}
	tpl.ObjectMeta.ManagedFields = nil
	if hashNames {
		if tpl.ObjectMeta.Name != "" {
			tpl.ObjectMeta.Name = HashName(tpl.ObjectMeta.Name, true)
		}
		if tpl.ObjectMeta.GenerateName != "" {
			tpl.ObjectMeta.GenerateName = HashName(tpl.ObjectMeta.GenerateName, true)
		}
	}
	tpl.ObjectMeta.Annotations = FilterAnnotations(tpl.ObjectMeta.Annotations, hashNames)
	tpl.ObjectMeta.Labels = SanitizeLabels(tpl.ObjectMeta.Labels, hashNames)
	tpl.ObjectMeta.OwnerReferences = SanitizeOwnerReferences(tpl.ObjectMeta.OwnerReferences, hashNames)
	SanitizePodSpec(&tpl.Spec, pauseImage)
}

func toMapWithoutStatus(obj any) map[string]any {
	raw, err := json.Marshal(obj)
	if err != nil {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil
	}
	delete(m, "status")
	return m
}

// ExtractLocalQueueName returns the Kueue LocalQueue name from labels or annotations on obj.
func ExtractLocalQueueName(obj metav1.Object) string {
	if obj == nil {
		return ""
	}
	if labels := obj.GetLabels(); labels != nil {
		if q := labels[KueueQueueLabel]; q != "" {
			return q
		}
	}
	if ann := obj.GetAnnotations(); ann != nil {
		if q := ann[KueueQueueLabel]; q != "" {
			return q
		}
	}
	return ""
}

// IsKueueManagedPod returns true if the Pod is marked to be scheduled with Kueue.
func IsKueueManagedPod(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	if ExtractLocalQueueName(pod) != "" {
		return true
	}
	if pod.Labels != nil && pod.Labels[KueueManagedLabel] == "true" {
		return true
	}
	for _, g := range pod.Spec.SchedulingGates {
		if g.Name == KueueAdmissionGateName || g.Name == KueueTopologyGateName {
			return true
		}
	}
	return false
}

// IsStandaloneKueuePod returns true if the Pod is marked to be scheduled with Kueue
// AND does not belong to any of the higher-level workflow APIs (Job, JobSet,
// LeaderWorkerSet, Deployment, RayJob, RayCluster).
func IsStandaloneKueuePod(pod *corev1.Pod) bool {
	if !IsKueueManagedPod(pod) {
		return false
	}
	for _, ref := range pod.OwnerReferences {
		if parentWorkflowKinds[ref.Kind] {
			return false
		}
	}
	if pod.Annotations != nil && pod.Annotations[KueueSuspendedByParentAnnot] != "" {
		return false
	}
	if pod.Labels != nil {
		if pod.Labels["job-name"] != "" ||
			pod.Labels["batch.kubernetes.io/job-name"] != "" ||
			pod.Labels["jobset.sigs.k8s.io/jobset-name"] != "" ||
			pod.Labels["leaderworkerset.sigs.k8s.io/name"] != "" ||
			pod.Labels["ray.io/cluster"] != "" {
			return false
		}
	}
	return true
}

// IsScheduledAndRunningPod returns true if the pod has been scheduled onto a node
// and is in Running (or scheduled active) state.
func IsScheduledAndRunningPod(pod *corev1.Pod) bool {
	if pod == nil || pod.Spec.NodeName == "" {
		return false
	}
	return pod.Status.Phase == corev1.PodRunning
}

// ComputePodResourceRequest calculates the effective pod resource requests:
// max(pod.Spec.Resources.Requests, max(initContainer.Requests), sum(container.Requests)) + pod.Spec.Overhead.
func ComputePodResourceRequest(pod *corev1.Pod) corev1.ResourceList {
	reqs := corev1.ResourceList{}
	if pod == nil {
		return reqs
	}

	// Sum regular container requests.
	for _, c := range pod.Spec.Containers {
		for rName, qty := range c.Resources.Requests {
			cur := reqs[rName]
			cur.Add(qty)
			reqs[rName] = cur
		}
	}

	// Account for init containers (sidecar init containers with RestartPolicy=Always add to running requests;
	// standard init containers take the max per resource).
	var sidecarReqs corev1.ResourceList
	for _, ic := range pod.Spec.InitContainers {
		if ic.RestartPolicy != nil && *ic.RestartPolicy == corev1.ContainerRestartPolicyAlways {
			if sidecarReqs == nil {
				sidecarReqs = corev1.ResourceList{}
			}
			for rName, qty := range ic.Resources.Requests {
				cur := sidecarReqs[rName]
				cur.Add(qty)
				sidecarReqs[rName] = cur
			}
		} else {
			for rName, qty := range ic.Resources.Requests {
				if cur, ok := reqs[rName]; !ok || qty.Cmp(cur) > 0 {
					reqs[rName] = qty.DeepCopy()
				}
			}
		}
	}
	for rName, qty := range sidecarReqs {
		cur := reqs[rName]
		cur.Add(qty)
		reqs[rName] = cur
	}

	// If pod-level resources are specified, take the max with container sum.
	if pod.Spec.Resources != nil {
		for rName, qty := range pod.Spec.Resources.Requests {
			if cur, ok := reqs[rName]; !ok || qty.Cmp(cur) > 0 {
				reqs[rName] = qty.DeepCopy()
			}
		}
	}

	// Add Pod Overhead if present.
	for rName, qty := range pod.Spec.Overhead {
		if !qty.IsZero() {
			cur := reqs[rName]
			cur.Add(qty)
			reqs[rName] = cur
		}
	}

	// Normalize quantities to clean canonical string representations.
	normalized := make(corev1.ResourceList, len(reqs))
	for rName, qty := range reqs {
		parsed := resource.MustParse(qty.String())
		normalized[rName] = parsed
	}
	return normalized
}

// ExtractPodSchedulingTime extracts the timestamp when the Pod was scheduled (from the PodScheduled condition),
// falling back to Status.StartTime or CreationTimestamp if the condition is absent.
func ExtractPodSchedulingTime(pod *corev1.Pod) *metav1.Time {
	if pod == nil {
		return nil
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodScheduled && cond.Status == corev1.ConditionTrue && !cond.LastTransitionTime.IsZero() {
			t := metav1.NewTime(cond.LastTransitionTime.Time.UTC())
			return &t
		}
	}
	if pod.Status.StartTime != nil && !pod.Status.StartTime.IsZero() {
		t := metav1.NewTime(pod.Status.StartTime.Time.UTC())
		return &t
	}
	if !pod.CreationTimestamp.IsZero() {
		t := metav1.NewTime(pod.CreationTimestamp.Time.UTC())
		return &t
	}
	return nil
}

// BuildScrapedPodPayload constructs the strictly-scoped ScrapedPodPayload for infra.json.
func BuildScrapedPodPayload(pod *corev1.Pod, firstNoticeTime time.Time, deletionTime *time.Time, hashNames bool) ScrapedPodPayload {
	var delTime *metav1.Time
	if deletionTime != nil && !deletionTime.IsZero() {
		t := metav1.NewTime(deletionTime.UTC())
		delTime = &t
	}
	return ScrapedPodPayload{
		APIVersion:      "v1",
		Kind:            "Pod",
		Name:            HashName(pod.Name, hashNames),
		Namespace:       pod.Namespace,
		Placement:       pod.Spec.NodeName,
		ResourceRequest: ComputePodResourceRequest(pod),
		SchedulingTime:  ExtractPodSchedulingTime(pod),
		FirstNoticeTime: metav1.NewTime(firstNoticeTime.UTC()),
		DeletionTime:    delTime,
	}
}

func sanitizeInfraObjectMeta(meta *metav1.ObjectMeta) {
	meta.ManagedFields = nil
	meta.Annotations = FilterAnnotations(meta.Annotations, false)
}

// SanitizeNode prepares a Node for infra.json, retaining only scheduling-relevant spec fields
// (taints, unschedulable) and status fields (capacity, allocatable, conditions, phase), filtering
// annotations, and stripping cloud/cluster-identifying node labels.
func SanitizeNode(node *corev1.Node) *corev1.Node {
	if node == nil {
		return nil
	}
	cp := node.DeepCopy()
	cp.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Node"}
	sanitizeInfraObjectMeta(&cp.ObjectMeta)
	cp.Labels = FilterLabels(cp.Labels, deniedNodeLabelKeys)
	cp.Spec = corev1.NodeSpec{
		Taints:        cp.Spec.Taints,
		Unschedulable: cp.Spec.Unschedulable,
	}
	cp.Status = corev1.NodeStatus{
		Capacity:    cp.Status.Capacity,
		Allocatable: cp.Status.Allocatable,
		Conditions:  cp.Status.Conditions,
		Phase:       cp.Status.Phase,
	}
	return cp
}

// SanitizeNamespace prepares a Namespace for infra.json (filtering annotations and omitting status).
func SanitizeNamespace(ns *corev1.Namespace) map[string]any {
	if ns == nil {
		return nil
	}
	cp := ns.DeepCopy()
	cp.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"}
	sanitizeInfraObjectMeta(&cp.ObjectMeta)
	return toMapWithoutStatus(cp)
}

// SanitizeClusterQueue prepares a ClusterQueue for infra.json (filtering annotations and omitting status).
func SanitizeClusterQueue(cq *kueuev1beta2.ClusterQueue) map[string]any {
	if cq == nil {
		return nil
	}
	cp := cq.DeepCopy()
	cp.TypeMeta = metav1.TypeMeta{APIVersion: kueuev1beta2.SchemeGroupVersion.String(), Kind: "ClusterQueue"}
	sanitizeInfraObjectMeta(&cp.ObjectMeta)
	return toMapWithoutStatus(cp)
}

// SanitizeLocalQueue prepares a LocalQueue for infra.json (filtering annotations and omitting status).
func SanitizeLocalQueue(lq *kueuev1beta2.LocalQueue) map[string]any {
	if lq == nil {
		return nil
	}
	cp := lq.DeepCopy()
	cp.TypeMeta = metav1.TypeMeta{APIVersion: kueuev1beta2.SchemeGroupVersion.String(), Kind: "LocalQueue"}
	sanitizeInfraObjectMeta(&cp.ObjectMeta)
	return toMapWithoutStatus(cp)
}

// SanitizeResourceFlavor prepares a ResourceFlavor for infra.json (filtering annotations and omitting status).
func SanitizeResourceFlavor(rf *kueuev1beta2.ResourceFlavor) map[string]any {
	if rf == nil {
		return nil
	}
	cp := rf.DeepCopy()
	cp.TypeMeta = metav1.TypeMeta{APIVersion: kueuev1beta2.SchemeGroupVersion.String(), Kind: "ResourceFlavor"}
	sanitizeInfraObjectMeta(&cp.ObjectMeta)
	return toMapWithoutStatus(cp)
}

// SanitizeTopology prepares a Topology for infra.json (filtering annotations and omitting status).
func SanitizeTopology(topo *kueuev1beta2.Topology) map[string]any {
	if topo == nil {
		return nil
	}
	cp := topo.DeepCopy()
	cp.TypeMeta = metav1.TypeMeta{APIVersion: kueuev1beta2.SchemeGroupVersion.String(), Kind: "Topology"}
	sanitizeInfraObjectMeta(&cp.ObjectMeta)
	return toMapWithoutStatus(cp)
}

// SanitizeJob sanitizes a batch/v1 Job for workflows.json.
func SanitizeJob(job *batchv1.Job, pauseImage string, hashNames bool) map[string]any {
	cp := job.DeepCopy()
	cp.TypeMeta = metav1.TypeMeta{APIVersion: "batch/v1", Kind: "Job"}
	SanitizeWorkflowObjectMeta(&cp.ObjectMeta, hashNames)
	SanitizePodTemplateSpec(&cp.Spec.Template, pauseImage, hashNames)
	return toMapWithoutStatus(cp)
}

// SanitizeJobSet sanitizes a jobset.x-k8s.io/v1alpha2 JobSet for workflows.json.
func SanitizeJobSet(js *jobsetv1alpha2.JobSet, pauseImage string, hashNames bool) map[string]any {
	cp := js.DeepCopy()
	cp.TypeMeta = metav1.TypeMeta{APIVersion: jobsetv1alpha2.GroupVersion.String(), Kind: "JobSet"}
	SanitizeWorkflowObjectMeta(&cp.ObjectMeta, hashNames)
	cp.Spec.VolumeClaimPolicies = nil
	for i := range cp.Spec.ReplicatedJobs {
		rj := &cp.Spec.ReplicatedJobs[i]
		rj.Template.ObjectMeta.ManagedFields = nil
		rj.Template.ObjectMeta.Annotations = FilterAnnotations(rj.Template.ObjectMeta.Annotations, hashNames)
		rj.Template.ObjectMeta.Labels = SanitizeLabels(rj.Template.ObjectMeta.Labels, hashNames)
		SanitizePodTemplateSpec(&rj.Template.Spec.Template, pauseImage, hashNames)
	}
	return toMapWithoutStatus(cp)
}

// SanitizeLeaderWorkerSet sanitizes a leaderworkerset.x-k8s.io/v1 LeaderWorkerSet for workflows.json.
func SanitizeLeaderWorkerSet(lws *leaderworkersetv1.LeaderWorkerSet, pauseImage string, hashNames bool) map[string]any {
	cp := lws.DeepCopy()
	cp.TypeMeta = metav1.TypeMeta{APIVersion: leaderworkersetv1.GroupVersion.String(), Kind: "LeaderWorkerSet"}
	SanitizeWorkflowObjectMeta(&cp.ObjectMeta, hashNames)
	cp.Spec.LeaderWorkerTemplate.VolumeClaimTemplates = nil
	if cp.Spec.LeaderWorkerTemplate.LeaderTemplate != nil {
		SanitizePodTemplateSpec(cp.Spec.LeaderWorkerTemplate.LeaderTemplate, pauseImage, hashNames)
	}
	SanitizePodTemplateSpec(&cp.Spec.LeaderWorkerTemplate.WorkerTemplate, pauseImage, hashNames)
	return toMapWithoutStatus(cp)
}

// SanitizeDeployment sanitizes an apps/v1 Deployment for workflows.json.
func SanitizeDeployment(deploy *appsv1.Deployment, pauseImage string, hashNames bool) map[string]any {
	cp := deploy.DeepCopy()
	cp.TypeMeta = metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"}
	SanitizeWorkflowObjectMeta(&cp.ObjectMeta, hashNames)
	SanitizePodTemplateSpec(&cp.Spec.Template, pauseImage, hashNames)
	return toMapWithoutStatus(cp)
}

func sanitizeRayClusterSpec(spec *rayv1.RayClusterSpec, pauseImage string, hashNames bool) {
	if spec == nil {
		return
	}
	spec.HeadServiceAnnotations = FilterAnnotations(spec.HeadServiceAnnotations, hashNames)
	spec.HeadGroupSpec.RayStartParams = nil
	if spec.HeadGroupSpec.HeadService != nil {
		SanitizeWorkflowObjectMeta(&spec.HeadGroupSpec.HeadService.ObjectMeta, hashNames)
	}
	SanitizePodTemplateSpec(&spec.HeadGroupSpec.Template, pauseImage, hashNames)
	for i := range spec.WorkerGroupSpecs {
		w := &spec.WorkerGroupSpecs[i]
		w.RayStartParams = nil
		SanitizePodTemplateSpec(&w.Template, pauseImage, hashNames)
	}
}

// SanitizeRayJob sanitizes a ray.io/v1 RayJob for workflows.json.
func SanitizeRayJob(rj *rayv1.RayJob, pauseImage string, hashNames bool) map[string]any {
	cp := rj.DeepCopy()
	cp.TypeMeta = metav1.TypeMeta{APIVersion: rayv1.GroupVersion.String(), Kind: "RayJob"}
	SanitizeWorkflowObjectMeta(&cp.ObjectMeta, hashNames)
	cp.Spec.Entrypoint = ""
	cp.Spec.RuntimeEnvYAML = ""
	cp.Spec.Metadata = nil
	if cp.Spec.SubmitterPodTemplate != nil {
		SanitizePodTemplateSpec(cp.Spec.SubmitterPodTemplate, pauseImage, hashNames)
	}
	sanitizeRayClusterSpec(cp.Spec.RayClusterSpec, pauseImage, hashNames)
	return toMapWithoutStatus(cp)
}

// SanitizeRayCluster sanitizes a ray.io/v1 RayCluster for workflows.json.
func SanitizeRayCluster(rc *rayv1.RayCluster, pauseImage string, hashNames bool) map[string]any {
	cp := rc.DeepCopy()
	cp.TypeMeta = metav1.TypeMeta{APIVersion: rayv1.GroupVersion.String(), Kind: "RayCluster"}
	SanitizeWorkflowObjectMeta(&cp.ObjectMeta, hashNames)
	sanitizeRayClusterSpec(&cp.Spec, pauseImage, hashNames)
	return toMapWithoutStatus(cp)
}

// SanitizeStandaloneKueuePod sanitizes a standalone Kueue-managed v1/Pod for workflows.json.
func SanitizeStandaloneKueuePod(pod *corev1.Pod, pauseImage string, hashNames bool) map[string]any {
	cp := pod.DeepCopy()
	cp.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}
	SanitizeWorkflowObjectMeta(&cp.ObjectMeta, hashNames)
	SanitizePodSpec(&cp.Spec, pauseImage)
	return toMapWithoutStatus(cp)
}
