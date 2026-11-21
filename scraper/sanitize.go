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
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log"
	"slices"
	"strings"
	"time"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	resourcehelper "k8s.io/component-helpers/resource"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"
	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	leaderworkersetv1 "sigs.k8s.io/lws/api/leaderworkerset/v1"
)

const (
	KueueQueueLabel                       = "kueue.x-k8s.io/queue-name"
	KueueManagedLabel                     = "kueue.x-k8s.io/managed"
	KueuePriorityClassLabel               = "kueue.x-k8s.io/priority-class"
	KueuePrebuiltWorkloadLabel            = "kueue.x-k8s.io/prebuilt-workload-name"
	KueueJobOwnerNameAnnotation           = "kueue.x-k8s.io/job-owner-name"
	KueueJobOwnerGVKAnnotation            = "kueue.x-k8s.io/job-owner-gvk"
	KueuePodGroupNameLabel                = "kueue.x-k8s.io/pod-group-name"
	KueueSuspendedByParentAnnot           = "kueue.x-k8s.io/pod-suspending-parent"
	KueueAdmissionGateName                = "kueue.x-k8s.io/admission"
	KueueTopologyGateName                 = "kueue.x-k8s.io/topology"
	KueueWorkloadAnnot                    = "kueue.x-k8s.io/workload"
	KueueWorkloadSliceNameAnnot           = "kueue.x-k8s.io/workload-slice-name"
	KueueWorkloadSliceReplacementForAnnot = "kueue.x-k8s.io/workload-slice-replacement-for"
)

// nonHashedKinds lists object kinds whose names must NEVER be hashed even when --hash-names is enabled.
var nonHashedKinds = map[string]bool{
	"Namespace":             true,
	"Node":                  true,
	"ClusterQueue":          true,
	"Cohort":                true,
	"LocalQueue":            true,
	"ResourceFlavor":        true,
	"Topology":              true,
	"WorkloadPriorityClass": true,
	"RuntimeClass":          true,
}

// labelKeysHoldingWorkloadNames lists label keys (all lowercase) whose values represent
// workload/job/pod/cluster/deployment names that should be hashed when SHA-256 name replacement is enabled.
var labelKeysHoldingWorkloadNames = map[string]bool{
	"app":                                     true,
	"app.kubernetes.io/name":                  true,
	"app.kubernetes.io/instance":              true,
	"job-name":                                true,
	"batch.kubernetes.io/job-name":            true,
	"jobset.sigs.k8s.io/jobset-name":          true,
	"jobset.sigs.k8s.io/coordinator":          true,
	"alpha.jobset.sigs.k8s.io/namespaced-job": true,
	"leaderworkerset.sigs.k8s.io/name":        true,
	"leaderworkerset.sigs.k8s.io/leader-name": true,
	"ray.io/cluster":                          true,
	KueuePrebuiltWorkloadLabel:                true,
	KueuePodGroupNameLabel:                    true,
}

// annotationKeysHoldingWorkloadNames lists annotation keys (all lowercase) whose values represent
// workload/job/pod names that should be hashed when SHA-256 name replacement is enabled.
var annotationKeysHoldingWorkloadNames = map[string]bool{
	KueueJobOwnerNameAnnotation:                  true,
	KueuePrebuiltWorkloadLabel:                   true,
	KueuePodGroupNameLabel:                       true,
	KueueWorkloadAnnot:                           true,
	KueueWorkloadSliceNameAnnot:                  true,
	KueueWorkloadSliceReplacementForAnnot:        true,
	"jobset.sigs.k8s.io/jobset-name":             true,
	"jobset.sigs.k8s.io/coordinator":             true,
	"alpha.jobset.sigs.k8s.io/namespaced-job":    true,
	"leaderworkerset.sigs.k8s.io/leader-name":    true,
	"leaderworkerset.sigs.k8s.io/leader-address": true,
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
	"kubernetes.io/description",
	"kubernetes.io/service-account.name",
	"kubernetes.io/service-account.uid",
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

// hashedLabelKeys lists default user/business metadata label keys (all lowercase) whose values are
// hashed across both infrastructure and workflow objects, templates, and selectors when hashing is enabled.
// Users can extend this list via --extra-hash-labels.
var hashedLabelKeys = []string{
	"owner",
	"team",
	"customer",
	"client",
	"user",
	"username",
	"author",
	"creator",
	"maintainer",
	"contact",
	"email",
	"department",
	"org",
	"organization",
	"cost-center",
	"cost_center",
	"costcenter",
	"billing",
	"project",
	"account",
}

// protectedLabelKeys lists Kueue control-plane label keys (all lowercase) whose values must never
// be hashed even if matched by user-supplied --extra-hash-labels patterns.
var protectedLabelKeys = map[string]bool{
	KueueQueueLabel:         true,
	KueueManagedLabel:       true,
	KueuePriorityClassLabel: true,
}

func isAllowedDomain(domain string) bool {
	if domain == "" || strings.HasPrefix(domain, ".") {
		return false
	}
	for _, allowed := range allowedAnnotationDomains {
		if domain == allowed || strings.HasSuffix(domain, "."+allowed) {
			return true
		}
	}
	return false
}

// defaultProcessSalt is a 256-bit ephemeral CSPRNG key used as a fail-safe if an uninitialized
// hmacSensitiveDataHasher is ever invoked without an explicit per-Scraper salt.
var defaultProcessSalt = NewRandomHashSalt()

// NewRandomHashSalt generates a fresh 32-byte (256-bit) cryptographically random HMAC key from the OS CSPRNG.
func NewRandomHashSalt() []byte {
	salt := make([]byte, 32)
	if _, err := rand.Read(salt); err != nil {
		panic("crypto/rand.Read failed: " + err.Error())
	}
	return salt
}

// SensitiveDataHasher anonymizes workload/pod/job names and sensitive or workload-referencing
// label values across scraped infrastructure, workflow, and execution-time records.
type SensitiveDataHasher interface {
	HashName(val string) string
	HashNameForKind(kind, name string) string
	ShouldHashLabel(key string) bool
	ShouldHashWorkloadLabel(key string) bool
}

// NoopHasher implements SensitiveDataHasher as a pass-through no-op when hashing (--hash-names) is disabled.
type NoopHasher struct{}

// NewNoopHasher returns a SensitiveDataHasher that leaves all names and label values in plain text.
func NewNoopHasher() SensitiveDataHasher {
	return NoopHasher{}
}

func (NoopHasher) HashName(val string) string {
	return val
}

func (NoopHasher) HashNameForKind(_, name string) string {
	return name
}

func (NoopHasher) ShouldHashLabel(_ string) bool {
	return false
}

func (NoopHasher) ShouldHashWorkloadLabel(_ string) bool {
	return false
}

// hmacSensitiveDataHasher deterministically hashes resource names and sensitive label values using
// per-run HMAC-SHA256 so low-entropy values (such as short team, project, customer, or job names)
// cannot be dictionary-guessed while preserving 1-to-1 matching across all scraped objects, names,
// and label selectors within a capture session.
type hmacSensitiveDataHasher struct {
	extraExactLabels   map[string]bool
	extraLabelPatterns []string
	salt               []byte
}

// NewSensitiveDataHasher constructs a per-run HMAC-SHA256 SensitiveDataHasher with normalized
// --extra-hash-labels exact keys and "<domain>/*" domain prefixes and a 256-bit key
// (generated from OS CSPRNG when salt is empty).
func NewSensitiveDataHasher(extraLabels []string, salt []byte) SensitiveDataHasher {
	var (
		exactLabels   map[string]bool
		labelPatterns []string
	)
	for _, raw := range extraLabels {
		entry := strings.ToLower(strings.TrimSpace(raw))
		if entry == "" {
			continue
		}
		if prefix, ok := strings.CutSuffix(entry, "/*"); ok {
			if prefix != "" && !slices.Contains(labelPatterns, prefix) {
				labelPatterns = append(labelPatterns, prefix)
			}
			continue
		}
		if exactLabels == nil {
			exactLabels = make(map[string]bool)
		}
		exactLabels[entry] = true
	}
	var key []byte
	if len(salt) > 0 {
		key = slices.Clone(salt)
	} else {
		key = NewRandomHashSalt()
	}
	return hmacSensitiveDataHasher{
		extraExactLabels:   exactLabels,
		extraLabelPatterns: labelPatterns,
		salt:               key,
	}
}

func (h hmacSensitiveDataHasher) effectiveSalt() []byte {
	if len(h.salt) > 0 {
		return h.salt
	}
	return defaultProcessSalt
}

func (h hmacSensitiveDataHasher) hmacSum(val string) [32]byte {
	mac := hmac.New(sha256.New, h.effectiveSalt())
	_, _ = mac.Write([]byte(val))
	var sum [32]byte
	copy(sum[:], mac.Sum(nil))
	return sum
}

// HashName returns a deterministic, Kubernetes-valid HMAC-SHA256 digest ("h-" + 32 hex chars,
// 34 chars total <= 63-char Kubernetes DNS/label limit) used consistently for both resource names
// and label values.
func (h hmacSensitiveDataHasher) HashName(val string) string {
	if val == "" {
		return ""
	}
	sum := h.hmacSum(val)
	return "h-" + hex.EncodeToString(sum[:16])
}

// HashNameForKind hashes name only if kind is a workload/pod/job/jobset/etc.
// Names of namespaces, nodes, queues, cohorts, resource flavors, topologies, workload priority classes, and runtime classes are never hashed.
func (h hmacSensitiveDataHasher) HashNameForKind(kind, name string) string {
	if name == "" || nonHashedKinds[kind] {
		return name
	}
	return h.HashName(name)
}

func (h hmacSensitiveDataHasher) shouldHashLabel(lowerKey string) bool {
	if h.extraExactLabels[lowerKey] || slices.Contains(hashedLabelKeys, lowerKey) {
		return true
	}
	domain, name, hasSlash := strings.Cut(lowerKey, "/")
	if !hasSlash {
		return false
	}
	if slices.Contains(hashedLabelKeys, name) {
		return true
	}
	for _, prefix := range h.extraLabelPatterns {
		if domain == prefix || strings.HasSuffix(domain, "."+prefix) {
			return true
		}
	}
	return false
}

func (h hmacSensitiveDataHasher) ShouldHashLabel(key string) bool {
	lowerKey := strings.ToLower(key)
	if protectedLabelKeys[lowerKey] {
		return false
	}
	return h.shouldHashLabel(lowerKey)
}

func (h hmacSensitiveDataHasher) ShouldHashWorkloadLabel(key string) bool {
	lowerKey := strings.ToLower(key)
	if protectedLabelKeys[lowerKey] {
		return false
	}
	if labelKeysHoldingWorkloadNames[lowerKey] {
		return true
	}
	return h.ShouldHashLabel(lowerKey)
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
	return isAllowedDomain(domain)
}

// FilterAnnotations retains only allowed Kubernetes, SIG (x-k8s.io / k8s.io), and kueue-optimizer
// domain annotations (excluding sensitive keys like kubectl.kubernetes.io/last-applied-configuration),
// and hashes any annotation value that holds a workload/job/pod name via hasher.
func FilterAnnotations(annotations map[string]string, hasher SensitiveDataHasher) map[string]string {
	if len(annotations) == 0 {
		return nil
	}
	filtered := make(map[string]string)
	for k, v := range annotations {
		if !ShouldKeepAnnotation(k) {
			continue
		}
		if annotationKeysHoldingWorkloadNames[strings.ToLower(k)] && v != "" {
			v = hasher.HashName(v)
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

func sanitizeLabelsFunc(labels map[string]string, shouldHash func(string) bool, hasher SensitiveDataHasher) map[string]string {
	if len(labels) == 0 {
		return nil
	}
	out := make(map[string]string, len(labels))
	for k, v := range labels {
		if shouldHash(k) {
			out[k] = hasher.HashName(v)
			continue
		}
		out[k] = v
	}
	return out
}

// SanitizeLabels hashes sensitive label values (hashedLabelKeys plus any --extra-hash-labels patterns) and
// workload/job/pod/deployment-referencing label values via hasher while keeping all label keys intact.
// Queue names (kueue.x-k8s.io/queue-name) are never hashed.
func SanitizeLabels(labels map[string]string, hasher SensitiveDataHasher) map[string]string {
	return sanitizeLabelsFunc(labels, hasher.ShouldHashWorkloadLabel, hasher)
}

func sanitizeLabelSelectorFunc(sel *metav1.LabelSelector, shouldHash func(string) bool, hasher SensitiveDataHasher) *metav1.LabelSelector {
	if sel == nil {
		return nil
	}
	cp := sel.DeepCopy()
	cp.MatchLabels = sanitizeLabelsFunc(cp.MatchLabels, shouldHash, hasher)
	for i := range cp.MatchExpressions {
		expr := &cp.MatchExpressions[i]
		if shouldHash(expr.Key) {
			for j, v := range expr.Values {
				expr.Values[j] = hasher.HashName(v)
			}
		}
	}
	return cp
}

// SanitizeLabelSelector hashes MatchLabels and MatchExpressions values on a LabelSelector via hasher so
// sensitive and workload-referencing label selectors remain non-empty and consistent.
func SanitizeLabelSelector(sel *metav1.LabelSelector, hasher SensitiveDataHasher) *metav1.LabelSelector {
	return sanitizeLabelSelectorFunc(sel, hasher.ShouldHashWorkloadLabel, hasher)
}

// SanitizeInfraLabels hashes sensitive label values (hashedLabelKeys plus --extra-hash-labels) on infrastructure objects via hasher.
func SanitizeInfraLabels(labels map[string]string, hasher SensitiveDataHasher) map[string]string {
	return sanitizeLabelsFunc(labels, hasher.ShouldHashLabel, hasher)
}

// SanitizeInfraLabelSelector hashes sensitive MatchLabels and MatchExpressions values on an infrastructure
// LabelSelector (such as ClusterQueue.spec.namespaceSelector or PodAffinityTerm.namespaceSelector) via hasher.
func SanitizeInfraLabelSelector(sel *metav1.LabelSelector, hasher SensitiveDataHasher) *metav1.LabelSelector {
	return sanitizeLabelSelectorFunc(sel, hasher.ShouldHashLabel, hasher)
}

// SanitizeNodeLabels strips cloud/cluster-identifying node labels (deniedNodeLabelKeys) and hashes
// sensitive label values (hashedLabelKeys plus --extra-hash-labels) via hasher so Node, ResourceFlavor, and Pod nodeSelector stay consistent.
func SanitizeNodeLabels(labels map[string]string, hasher SensitiveDataHasher) map[string]string {
	return SanitizeInfraLabels(FilterLabels(labels, deniedNodeLabelKeys), hasher)
}

func sanitizeSlice[T any](items []T, hasher SensitiveDataHasher, sanitize func(T, SensitiveDataHasher) *T) []T {
	if len(items) == 0 {
		return nil
	}
	out := make([]T, 0, len(items))
	for _, item := range items {
		if clean := sanitize(item, hasher); clean != nil {
			out = append(out, *clean)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func sanitizeNodeSelectorTerm(term corev1.NodeSelectorTerm, hasher SensitiveDataHasher) *corev1.NodeSelectorTerm {
	out := corev1.NodeSelectorTerm{
		MatchFields: slices.Clone(term.MatchFields),
	}
	if len(term.MatchExpressions) > 0 {
		exprs := make([]corev1.NodeSelectorRequirement, 0, len(term.MatchExpressions))
		for _, expr := range term.MatchExpressions {
			lowerKey := strings.ToLower(expr.Key)
			if slices.Contains(deniedNodeLabelKeys, lowerKey) {
				continue
			}
			cleanExpr := expr
			if hasher.ShouldHashLabel(expr.Key) && len(expr.Values) > 0 {
				vals := make([]string, len(expr.Values))
				for i, v := range expr.Values {
					vals[i] = hasher.HashName(v)
				}
				cleanExpr.Values = vals
			} else if len(expr.Values) > 0 {
				cleanExpr.Values = slices.Clone(expr.Values)
			}
			exprs = append(exprs, cleanExpr)
		}
		if len(exprs) > 0 {
			out.MatchExpressions = exprs
		}
	}
	if len(out.MatchExpressions) == 0 && len(out.MatchFields) == 0 {
		return nil
	}
	return &out
}

func sanitizePreferredSchedulingTerm(pref corev1.PreferredSchedulingTerm, hasher SensitiveDataHasher) *corev1.PreferredSchedulingTerm {
	clean := sanitizeNodeSelectorTerm(pref.Preference, hasher)
	if clean == nil {
		return nil
	}
	return &corev1.PreferredSchedulingTerm{
		Weight:     pref.Weight,
		Preference: *clean,
	}
}

func sanitizePodAffinityTerm(term corev1.PodAffinityTerm, hasher SensitiveDataHasher) *corev1.PodAffinityTerm {
	out := term.DeepCopy()
	out.LabelSelector = SanitizeLabelSelector(out.LabelSelector, hasher)
	out.NamespaceSelector = SanitizeInfraLabelSelector(out.NamespaceSelector, hasher)
	return out
}

func sanitizeWeightedPodAffinityTerm(term corev1.WeightedPodAffinityTerm, hasher SensitiveDataHasher) *corev1.WeightedPodAffinityTerm {
	out := term
	out.PodAffinityTerm = *sanitizePodAffinityTerm(term.PodAffinityTerm, hasher)
	return &out
}

// SanitizeAffinity sanitizes NodeAffinity, PodAffinity, and PodAntiAffinity terms consistently with
// Node, Namespace, and Pod label sanitization.
func SanitizeAffinity(aff *corev1.Affinity, hasher SensitiveDataHasher) *corev1.Affinity {
	if aff == nil {
		return nil
	}
	cp := aff.DeepCopy()
	if cp.NodeAffinity != nil {
		if req := cp.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution; req != nil {
			req.NodeSelectorTerms = sanitizeSlice(req.NodeSelectorTerms, hasher, sanitizeNodeSelectorTerm)
			if len(req.NodeSelectorTerms) == 0 {
				cp.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = nil
			}
		}
		cp.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution = sanitizeSlice(
			cp.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution, hasher, sanitizePreferredSchedulingTerm,
		)
		if cp.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil &&
			len(cp.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution) == 0 {
			cp.NodeAffinity = nil
		}
	}
	if cp.PodAffinity != nil {
		cp.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution = sanitizeSlice(
			cp.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution, hasher, sanitizePodAffinityTerm,
		)
		cp.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution = sanitizeSlice(
			cp.PodAffinity.PreferredDuringSchedulingIgnoredDuringExecution, hasher, sanitizeWeightedPodAffinityTerm,
		)
	}
	if cp.PodAntiAffinity != nil {
		cp.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = sanitizeSlice(
			cp.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution, hasher, sanitizePodAffinityTerm,
		)
		cp.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution = sanitizeSlice(
			cp.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution, hasher, sanitizeWeightedPodAffinityTerm,
		)
	}
	if cp.NodeAffinity == nil && cp.PodAffinity == nil && cp.PodAntiAffinity == nil {
		return nil
	}
	return cp
}

// SanitizeOwnerReferences copies owner references and hashes non-infrastructure owner names via hasher.
func SanitizeOwnerReferences(refs []metav1.OwnerReference, hasher SensitiveDataHasher) []metav1.OwnerReference {
	if len(refs) == 0 {
		return nil
	}
	out := make([]metav1.OwnerReference, len(refs))
	for i, r := range refs {
		out[i] = r
		out[i].Name = hasher.HashNameForKind(r.Kind, r.Name)
	}
	return out
}

// SanitizeWorkflowObjectMeta sanitizes top-level ObjectMeta on workflow objects.
func SanitizeWorkflowObjectMeta(meta *metav1.ObjectMeta, hasher SensitiveDataHasher) {
	meta.ManagedFields = nil
	meta.Name = hasher.HashName(meta.Name)
	if meta.GenerateName != "" {
		meta.GenerateName = hasher.HashName(meta.GenerateName)
	}
	meta.Annotations = FilterAnnotations(meta.Annotations, hasher)
	meta.Labels = SanitizeLabels(meta.Labels, hasher)
	meta.OwnerReferences = SanitizeOwnerReferences(meta.OwnerReferences, hasher)
}

// SanitizePodSpec strips business-specific fields and hashes sensitive label selectors in a PodSpec:
//   - Replaces container images with pauseImage and clears Command and Args
//   - Removes container probes (LivenessProbe, ReadinessProbe, StartupProbe) and Lifecycle hooks
//   - Clears ServiceAccountName (and DeprecatedServiceAccount), Hostname, HostnameOverride, Subdomain, HostAliases, and DNSConfig
//   - Removes Volumes, VolumeMounts, and VolumeDevices
//   - Removes Secrets (ImagePullSecrets, secret env/volumes)
//   - Removes Environment variables (Env and EnvFrom)
//   - Sanitizes NodeSelector, Affinity (NodeAffinity, PodAffinity, PodAntiAffinity), and TopologySpreadConstraints
func SanitizePodSpec(spec *corev1.PodSpec, pauseImage string, hasher SensitiveDataHasher) {
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
	spec.HostAliases = nil
	spec.DNSConfig = nil
	spec.NodeSelector = SanitizeNodeLabels(spec.NodeSelector, hasher)
	spec.Affinity = SanitizeAffinity(spec.Affinity, hasher)
	for i := range spec.TopologySpreadConstraints {
		tsc := &spec.TopologySpreadConstraints[i]
		tsc.LabelSelector = SanitizeLabelSelector(tsc.LabelSelector, hasher)
	}

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
func SanitizePodTemplateSpec(tpl *corev1.PodTemplateSpec, pauseImage string, hasher SensitiveDataHasher) {
	if tpl == nil {
		return
	}
	tpl.ObjectMeta.ManagedFields = nil
	if tpl.ObjectMeta.Name != "" {
		tpl.ObjectMeta.Name = hasher.HashName(tpl.ObjectMeta.Name)
	}
	if tpl.ObjectMeta.GenerateName != "" {
		tpl.ObjectMeta.GenerateName = hasher.HashName(tpl.ObjectMeta.GenerateName)
	}
	tpl.ObjectMeta.Annotations = FilterAnnotations(tpl.ObjectMeta.Annotations, hasher)
	tpl.ObjectMeta.Labels = SanitizeLabels(tpl.ObjectMeta.Labels, hasher)
	tpl.ObjectMeta.OwnerReferences = SanitizeOwnerReferences(tpl.ObjectMeta.OwnerReferences, hasher)
	SanitizePodSpec(&tpl.Spec, pauseImage, hasher)
}

func toMapWithoutStatus(obj any) map[string]any {
	raw, err := json.Marshal(obj)
	if err != nil {
		log.Printf("WARN failed to marshal sanitized object %T: %v", obj, err)
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		log.Printf("WARN failed to unmarshal sanitized object %T: %v", obj, err)
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

// ExtractDeploymentLocalQueueName returns the Kueue LocalQueue name from a Deployment's
// metadata or its PodTemplateSpec metadata.
func ExtractDeploymentLocalQueueName(deploy *appsv1.Deployment) string {
	if deploy == nil {
		return ""
	}
	if lq := ExtractLocalQueueName(deploy); lq != "" {
		return lq
	}
	return ExtractLocalQueueName(&deploy.Spec.Template.ObjectMeta)
}

// IsKueueManagedDeployment returns true if the Deployment (or its PodTemplateSpec)
// is marked to be scheduled with Kueue regardless of namespace.
func IsKueueManagedDeployment(deploy *appsv1.Deployment) bool {
	if deploy == nil {
		return false
	}
	if ExtractDeploymentLocalQueueName(deploy) != "" {
		return true
	}
	if (deploy.Labels != nil && deploy.Labels[KueueManagedLabel] == "true") ||
		(deploy.Spec.Template.Labels != nil && deploy.Spec.Template.Labels[KueueManagedLabel] == "true") {
		return true
	}
	return false
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

// ComputePodResourceRequest returns the effective resource requests of the pod spec,
// computed with resource.PodRequests, the helper kube-scheduler uses. Because a sidecar
// (an init container with restartPolicy Always) keeps running once started, it adds to
// the containers and to the init containers listed after it, but not to those before it.
func ComputePodResourceRequest(pod *corev1.Pod) corev1.ResourceList {
	if pod == nil {
		return corev1.ResourceList{}
	}
	reqs := resourcehelper.PodRequests(pod, resourcehelper.PodResourcesOptions{ExcludeOverhead: true})

	// Add the overhead here so zero entries are skipped. reqs may hold the pod's own
	// pod-level quantities, so add to a copy rather than to them.
	for rName, qty := range pod.Spec.Overhead {
		if !qty.IsZero() {
			cur := reqs[rName].DeepCopy()
			cur.Add(qty)
			reqs[rName] = cur
		}
	}

	// Normalize quantities to clean canonical string representations.
	normalized := make(corev1.ResourceList, len(reqs))
	for rName, qty := range reqs {
		normalized[rName] = resource.MustParse(qty.String())
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
func BuildScrapedPodPayload(pod *corev1.Pod, firstNoticeTime time.Time, deletionTime *time.Time, hasher SensitiveDataHasher) ScrapedPodPayload {
	var delTime *metav1.Time
	if deletionTime != nil && !deletionTime.IsZero() {
		t := metav1.NewTime(deletionTime.UTC())
		delTime = &t
	}
	return ScrapedPodPayload{
		APIVersion:      "v1",
		Kind:            "Pod",
		Name:            hasher.HashName(pod.Name),
		Namespace:       pod.Namespace,
		Placement:       pod.Spec.NodeName,
		ResourceRequest: ComputePodResourceRequest(pod),
		SchedulingTime:  ExtractPodSchedulingTime(pod),
		FirstNoticeTime: metav1.NewTime(firstNoticeTime.UTC()),
		DeletionTime:    delTime,
	}
}

func sanitizeInfraObjectMeta(meta *metav1.ObjectMeta, hasher SensitiveDataHasher) {
	meta.ManagedFields = nil
	meta.Annotations = FilterAnnotations(meta.Annotations, hasher)
	meta.Labels = SanitizeInfraLabels(meta.Labels, hasher)
}

// SanitizeNode prepares a Node for infra.json, retaining only scheduling-relevant spec fields
// (taints, unschedulable) and status fields (capacity, allocatable, conditions, phase), stripping
// zero-valued nodeInfo and daemonEndpoints from status, filtering annotations, stripping
// cloud/cluster-identifying node labels, and hashing sensitive/--extra-hash-labels values via hasher.
func SanitizeNode(node *corev1.Node, hasher SensitiveDataHasher) map[string]any {
	if node == nil {
		return nil
	}
	cp := node.DeepCopy()
	cp.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Node"}
	cp.ManagedFields = nil
	cp.Annotations = FilterAnnotations(cp.Annotations, hasher)
	cp.Labels = SanitizeNodeLabels(cp.Labels, hasher)
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
	raw, err := json.Marshal(cp)
	if err != nil {
		log.Printf("WARN failed to marshal sanitized Node %q: %v", cp.Name, err)
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		log.Printf("WARN failed to unmarshal sanitized Node %q: %v", cp.Name, err)
		return nil
	}
	if status, ok := m["status"].(map[string]any); ok {
		delete(status, "nodeInfo")
		delete(status, "daemonEndpoints")
	}
	return m
}

// SanitizeNamespace prepares a Namespace for infra.json (filtering annotations, hashing sensitive/--extra-hash-labels values via hasher, and omitting status).
func SanitizeNamespace(ns *corev1.Namespace, hasher SensitiveDataHasher) map[string]any {
	if ns == nil {
		return nil
	}
	cp := ns.DeepCopy()
	cp.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"}
	sanitizeInfraObjectMeta(&cp.ObjectMeta, hasher)
	return toMapWithoutStatus(cp)
}

// SanitizeClusterQueue prepares a ClusterQueue for infra.json (filtering annotations, hashing sensitive/--extra-hash-labels
// values including spec.namespaceSelector via hasher, and omitting status).
func SanitizeClusterQueue(cq *kueuev1beta2.ClusterQueue, hasher SensitiveDataHasher) map[string]any {
	if cq == nil {
		return nil
	}
	cp := cq.DeepCopy()
	cp.TypeMeta = metav1.TypeMeta{APIVersion: kueuev1beta2.SchemeGroupVersion.String(), Kind: "ClusterQueue"}
	sanitizeInfraObjectMeta(&cp.ObjectMeta, hasher)
	cp.Spec.NamespaceSelector = SanitizeInfraLabelSelector(cp.Spec.NamespaceSelector, hasher)
	return toMapWithoutStatus(cp)
}

// SanitizeCohort prepares a Cohort for infra.json (filtering annotations, hashing sensitive/--extra-hash-labels values via hasher, and omitting status).
func SanitizeCohort(cohort *kueuev1beta2.Cohort, hasher SensitiveDataHasher) map[string]any {
	if cohort == nil {
		return nil
	}
	cp := cohort.DeepCopy()
	cp.TypeMeta = metav1.TypeMeta{APIVersion: kueuev1beta2.SchemeGroupVersion.String(), Kind: "Cohort"}
	sanitizeInfraObjectMeta(&cp.ObjectMeta, hasher)
	return toMapWithoutStatus(cp)
}

// SanitizeLocalQueue prepares a LocalQueue for infra.json (filtering annotations, hashing sensitive/--extra-hash-labels values via hasher, and omitting status).
func SanitizeLocalQueue(lq *kueuev1beta2.LocalQueue, hasher SensitiveDataHasher) map[string]any {
	if lq == nil {
		return nil
	}
	cp := lq.DeepCopy()
	cp.TypeMeta = metav1.TypeMeta{APIVersion: kueuev1beta2.SchemeGroupVersion.String(), Kind: "LocalQueue"}
	sanitizeInfraObjectMeta(&cp.ObjectMeta, hasher)
	return toMapWithoutStatus(cp)
}

// SanitizeResourceFlavor prepares a ResourceFlavor for infra.json (filtering annotations, hashing sensitive/--extra-hash-labels
// values including spec.nodeLabels via hasher, and omitting status).
func SanitizeResourceFlavor(rf *kueuev1beta2.ResourceFlavor, hasher SensitiveDataHasher) map[string]any {
	if rf == nil {
		return nil
	}
	cp := rf.DeepCopy()
	cp.TypeMeta = metav1.TypeMeta{APIVersion: kueuev1beta2.SchemeGroupVersion.String(), Kind: "ResourceFlavor"}
	sanitizeInfraObjectMeta(&cp.ObjectMeta, hasher)
	cp.Spec.NodeLabels = SanitizeNodeLabels(cp.Spec.NodeLabels, hasher)
	return toMapWithoutStatus(cp)
}

// SanitizeTopology prepares a Topology for infra.json (filtering annotations, hashing sensitive/--extra-hash-labels values via hasher, and omitting status).
func SanitizeTopology(topo *kueuev1beta2.Topology, hasher SensitiveDataHasher) map[string]any {
	if topo == nil {
		return nil
	}
	cp := topo.DeepCopy()
	cp.TypeMeta = metav1.TypeMeta{APIVersion: kueuev1beta2.SchemeGroupVersion.String(), Kind: "Topology"}
	sanitizeInfraObjectMeta(&cp.ObjectMeta, hasher)
	return toMapWithoutStatus(cp)
}

// SanitizeWorkloadPriorityClass prepares a WorkloadPriorityClass for infra.json (filtering annotations,
// hashing sensitive/--extra-hash-labels values via hasher, clearing free-form description, and omitting status).
func SanitizeWorkloadPriorityClass(wpc *kueuev1beta2.WorkloadPriorityClass, hasher SensitiveDataHasher) map[string]any {
	if wpc == nil {
		return nil
	}
	cp := wpc.DeepCopy()
	cp.TypeMeta = metav1.TypeMeta{APIVersion: kueuev1beta2.SchemeGroupVersion.String(), Kind: "WorkloadPriorityClass"}
	sanitizeInfraObjectMeta(&cp.ObjectMeta, hasher)
	cp.Description = ""
	return toMapWithoutStatus(cp)
}

// SanitizeRuntimeClass prepares a RuntimeClass for infra.json (filtering annotations, sanitizing
// scheduling.nodeSelector and metadata.labels via hasher, and omitting status).
func SanitizeRuntimeClass(rc *nodev1.RuntimeClass, hasher SensitiveDataHasher) map[string]any {
	if rc == nil {
		return nil
	}
	cp := rc.DeepCopy()
	cp.TypeMeta = metav1.TypeMeta{APIVersion: nodev1.SchemeGroupVersion.String(), Kind: "RuntimeClass"}
	sanitizeInfraObjectMeta(&cp.ObjectMeta, hasher)
	if cp.Scheduling != nil {
		// RuntimeClass scheduling.nodeSelector is a plain labels map (map[string]string) and does
		// not support matchExpressions, so label map sanitization via SanitizeNodeLabels is sufficient.
		cp.Scheduling.NodeSelector = SanitizeNodeLabels(cp.Scheduling.NodeSelector, hasher)
		if len(cp.Scheduling.NodeSelector) == 0 && len(cp.Scheduling.Tolerations) == 0 {
			cp.Scheduling = nil
		}
	}
	return toMapWithoutStatus(cp)
}

// SanitizeJob sanitizes a batch/v1 Job for workflows.json.
func SanitizeJob(job *batchv1.Job, pauseImage string, hasher SensitiveDataHasher) map[string]any {
	cp := job.DeepCopy()
	cp.TypeMeta = metav1.TypeMeta{APIVersion: "batch/v1", Kind: "Job"}
	SanitizeWorkflowObjectMeta(&cp.ObjectMeta, hasher)
	cp.Spec.Selector = SanitizeLabelSelector(cp.Spec.Selector, hasher)
	SanitizePodTemplateSpec(&cp.Spec.Template, pauseImage, hasher)
	return toMapWithoutStatus(cp)
}

// SanitizeJobSet sanitizes a jobset.x-k8s.io/v1alpha2 JobSet for workflows.json.
func SanitizeJobSet(js *jobsetv1alpha2.JobSet, pauseImage string, hasher SensitiveDataHasher) map[string]any {
	cp := js.DeepCopy()
	cp.TypeMeta = metav1.TypeMeta{APIVersion: jobsetv1alpha2.GroupVersion.String(), Kind: "JobSet"}
	SanitizeWorkflowObjectMeta(&cp.ObjectMeta, hasher)
	cp.Spec.VolumeClaimPolicies = nil
	if cp.Spec.Network != nil {
		cp.Spec.Network.Subdomain = ""
	}
	for i := range cp.Spec.ReplicatedJobs {
		rj := &cp.Spec.ReplicatedJobs[i]
		rj.Template.ObjectMeta.ManagedFields = nil
		rj.Template.ObjectMeta.Annotations = FilterAnnotations(rj.Template.ObjectMeta.Annotations, hasher)
		rj.Template.ObjectMeta.Labels = SanitizeLabels(rj.Template.ObjectMeta.Labels, hasher)
		rj.Template.Spec.Selector = SanitizeLabelSelector(rj.Template.Spec.Selector, hasher)
		SanitizePodTemplateSpec(&rj.Template.Spec.Template, pauseImage, hasher)
	}
	return toMapWithoutStatus(cp)
}

// SanitizeLeaderWorkerSet sanitizes a leaderworkerset.x-k8s.io/v1 LeaderWorkerSet for workflows.json.
func SanitizeLeaderWorkerSet(lws *leaderworkersetv1.LeaderWorkerSet, pauseImage string, hasher SensitiveDataHasher) map[string]any {
	cp := lws.DeepCopy()
	cp.TypeMeta = metav1.TypeMeta{APIVersion: leaderworkersetv1.GroupVersion.String(), Kind: "LeaderWorkerSet"}
	SanitizeWorkflowObjectMeta(&cp.ObjectMeta, hasher)
	cp.Spec.LeaderWorkerTemplate.VolumeClaimTemplates = nil
	if cp.Spec.LeaderWorkerTemplate.LeaderTemplate != nil {
		SanitizePodTemplateSpec(cp.Spec.LeaderWorkerTemplate.LeaderTemplate, pauseImage, hasher)
	}
	SanitizePodTemplateSpec(&cp.Spec.LeaderWorkerTemplate.WorkerTemplate, pauseImage, hasher)
	return toMapWithoutStatus(cp)
}

// SanitizeDeployment sanitizes an apps/v1 Deployment for workflows.json.
func SanitizeDeployment(deploy *appsv1.Deployment, pauseImage string, hasher SensitiveDataHasher) map[string]any {
	cp := deploy.DeepCopy()
	cp.TypeMeta = metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"}
	SanitizeWorkflowObjectMeta(&cp.ObjectMeta, hasher)
	cp.Spec.Selector = SanitizeLabelSelector(cp.Spec.Selector, hasher)
	SanitizePodTemplateSpec(&cp.Spec.Template, pauseImage, hasher)
	return toMapWithoutStatus(cp)
}

func sanitizeServiceSpec(spec *corev1.ServiceSpec, hasher SensitiveDataHasher) {
	if spec == nil {
		return
	}
	spec.Selector = SanitizeLabels(spec.Selector, hasher)
	if spec.ClusterIP != corev1.ClusterIPNone {
		spec.ClusterIP = ""
	}
	if len(spec.ClusterIPs) != 1 || spec.ClusterIPs[0] != corev1.ClusterIPNone {
		spec.ClusterIPs = nil
	}
	spec.ExternalIPs = nil
	spec.LoadBalancerIP = ""
	spec.LoadBalancerSourceRanges = nil
	spec.ExternalName = ""
	if spec.Type == corev1.ServiceTypeExternalName {
		spec.Type = corev1.ServiceTypeClusterIP
	}
}

func sanitizeRayClusterSpec(spec *rayv1.RayClusterSpec, pauseImage string, hasher SensitiveDataHasher) {
	if spec == nil {
		return
	}
	if pauseImage == "" {
		pauseImage = DefaultPauseImage
	}
	spec.HeadServiceAnnotations = FilterAnnotations(spec.HeadServiceAnnotations, hasher)
	if spec.AutoscalerOptions != nil {
		if spec.AutoscalerOptions.Image != nil && *spec.AutoscalerOptions.Image != "" {
			img := pauseImage
			spec.AutoscalerOptions.Image = &img
		}
		spec.AutoscalerOptions.Command = nil
		spec.AutoscalerOptions.Args = nil
		spec.AutoscalerOptions.Env = nil
		spec.AutoscalerOptions.EnvFrom = nil
		spec.AutoscalerOptions.VolumeMounts = nil
	}
	if spec.HistoryServerOptions != nil && spec.HistoryServerOptions.CollectorOptions != nil {
		co := spec.HistoryServerOptions.CollectorOptions
		if co.Image != nil && *co.Image != "" {
			img := pauseImage
			co.Image = &img
		}
		co.Env = nil
	}
	spec.GcsFaultToleranceOptions = nil
	if spec.AuthOptions != nil {
		spec.AuthOptions.SecretName = nil
	}
	spec.NetworkPolicy = nil
	spec.HeadGroupSpec.RayStartParams = nil
	spec.HeadGroupSpec.EnableIngress = nil
	spec.HeadGroupSpec.IngressOptions = nil
	spec.HeadGroupSpec.Labels = SanitizeLabels(spec.HeadGroupSpec.Labels, hasher)
	if spec.HeadGroupSpec.HeadService != nil {
		SanitizeWorkflowObjectMeta(&spec.HeadGroupSpec.HeadService.ObjectMeta, hasher)
		sanitizeServiceSpec(&spec.HeadGroupSpec.HeadService.Spec, hasher)
	}
	SanitizePodTemplateSpec(&spec.HeadGroupSpec.Template, pauseImage, hasher)
	for i := range spec.WorkerGroupSpecs {
		w := &spec.WorkerGroupSpecs[i]
		w.RayStartParams = nil
		w.Labels = SanitizeLabels(w.Labels, hasher)
		SanitizePodTemplateSpec(&w.Template, pauseImage, hasher)
	}
}

// SanitizeRayJob sanitizes a ray.io/v1 RayJob for workflows.json.
func SanitizeRayJob(rj *rayv1.RayJob, pauseImage string, hasher SensitiveDataHasher) map[string]any {
	cp := rj.DeepCopy()
	cp.TypeMeta = metav1.TypeMeta{APIVersion: rayv1.GroupVersion.String(), Kind: "RayJob"}
	SanitizeWorkflowObjectMeta(&cp.ObjectMeta, hasher)
	cp.Spec.Entrypoint = ""
	cp.Spec.RuntimeEnvYAML = ""
	cp.Spec.JobId = ""
	cp.Spec.Metadata = nil
	cp.Spec.ClusterSelector = SanitizeLabels(cp.Spec.ClusterSelector, hasher)
	if cp.Spec.SubmitterPodTemplate != nil {
		SanitizePodTemplateSpec(cp.Spec.SubmitterPodTemplate, pauseImage, hasher)
	}
	sanitizeRayClusterSpec(cp.Spec.RayClusterSpec, pauseImage, hasher)
	return toMapWithoutStatus(cp)
}

// SanitizeRayCluster sanitizes a ray.io/v1 RayCluster for workflows.json.
func SanitizeRayCluster(rc *rayv1.RayCluster, pauseImage string, hasher SensitiveDataHasher) map[string]any {
	cp := rc.DeepCopy()
	cp.TypeMeta = metav1.TypeMeta{APIVersion: rayv1.GroupVersion.String(), Kind: "RayCluster"}
	SanitizeWorkflowObjectMeta(&cp.ObjectMeta, hasher)
	sanitizeRayClusterSpec(&cp.Spec, pauseImage, hasher)
	return toMapWithoutStatus(cp)
}

// SanitizeStandaloneKueuePod sanitizes a standalone Kueue-managed v1/Pod for workflows.json.
func SanitizeStandaloneKueuePod(pod *corev1.Pod, pauseImage string, hasher SensitiveDataHasher) map[string]any {
	cp := pod.DeepCopy()
	cp.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}
	SanitizeWorkflowObjectMeta(&cp.ObjectMeta, hasher)
	SanitizePodSpec(&cp.Spec, pauseImage, hasher)
	return toMapWithoutStatus(cp)
}
