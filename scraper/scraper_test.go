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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	nodev1 "k8s.io/api/node/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"
	jobsetfake "sigs.k8s.io/jobset/client-go/clientset/versioned/fake"
	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	kueueclientset "sigs.k8s.io/kueue/client-go/clientset/versioned"
	kueuefake "sigs.k8s.io/kueue/client-go/clientset/versioned/fake"
	leaderworkersetv1 "sigs.k8s.io/lws/api/leaderworkerset/v1"
)

// Predefined sensitive sentinel constants reused across sanitization test fixtures (annotations,
// labels, raw resource names, cloud/system IDs, container images, commands/args, env vars, volumes,
// and IPs/CIDRs). Always reuse these constants when adding test cases so testForbiddenSubstrings
// stays small, consistent, and easy to follow.
const (
	testSecretValue      = "SENSITIVE_SECRET_VALUE"
	testSecretImage      = "registry.internal/SENSITIVE_SECRET_IMAGE:v1"
	testSecretEmail      = "owner@sensitive-client.internal"
	testSecretIP         = "10.128.99.42"
	testSecretCIDR       = "10.244.99.0/24"
	testSecretNamePrefix = "secret-name-"
)

var testForbiddenSubstrings = []string{
	testSecretValue,
	testSecretImage,
	testSecretEmail,
	testSecretIP,
	testSecretCIDR,
	testSecretNamePrefix,
}

func readJSONLines[T any](t *testing.T, raw []byte) []T {
	t.Helper()
	var results []T
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) == 1 && lines[0] == "" {
		return nil
	}
	for i, line := range lines {
		if !strings.HasPrefix(line, "{") || !strings.HasSuffix(line, "}") {
			t.Fatalf("line %d is not a single-line JSON object: %q", i+1, line)
		}
		var item T
		if err := json.Unmarshal([]byte(line), &item); err != nil {
			t.Fatalf("unmarshal line %d: %v\nline:\n%s", i+1, err, line)
		}
		results = append(results, item)
	}
	return results
}

func newTestScraper(
	t *testing.T,
	cfg Config,
	kubeClient kubernetes.Interface,
	kueueClient kueueclientset.Interface,
	dynamicClient dynamic.Interface,
) *Scraper {
	t.Helper()
	tmpDir := t.TempDir()
	if cfg.Interval == 0 {
		cfg.Interval = 60 * time.Second
	}
	if cfg.PauseImage == "" {
		cfg.PauseImage = DefaultPauseImage
	}
	if cfg.InfraFile == "" {
		cfg.InfraFile = filepath.Join(tmpDir, "infra.json")
	}
	if cfg.WorkflowsFile == "" {
		cfg.WorkflowsFile = filepath.Join(tmpDir, "workflows.json")
	}
	if cfg.ExecutionTimeFile == "" {
		cfg.ExecutionTimeFile = filepath.Join(tmpDir, "execution-time.json")
	}
	if kubeClient == nil {
		kubeClient = kubefake.NewSimpleClientset()
	}
	if kueueClient == nil {
		kueueClient = kueuefake.NewSimpleClientset()
	}

	infraLog, err := NewJSONEventLog(cfg.InfraFile)
	if err != nil {
		t.Fatalf("create infraLog: %v", err)
	}
	t.Cleanup(func() { _ = infraLog.Close() })

	workflowsLog, err := NewJSONEventLog(cfg.WorkflowsFile)
	if err != nil {
		t.Fatalf("create workflowsLog: %v", err)
	}
	t.Cleanup(func() { _ = workflowsLog.Close() })

	execTimeLog, err := NewJSONEventLog(cfg.ExecutionTimeFile)
	if err != nil {
		t.Fatalf("create execTimeLog: %v", err)
	}
	t.Cleanup(func() { _ = execTimeLog.Close() })

	return NewScraper(
		cfg,
		kubeClient,
		kueueClient,
		jobsetfake.NewSimpleClientset(),
		dynamicClient,
		infraLog,
		workflowsLog,
		execTimeLog,
	)
}

func TestStartupBanner(t *testing.T) {
	var buf bytes.Buffer
	cfg := Config{
		Interval:          60 * time.Second,
		HashNames:         true,
		PauseImage:        DefaultPauseImage,
		InfraFile:         "infra.json",
		WorkflowsFile:     "workflows.json",
		ExecutionTimeFile: "execution-time.json",
	}
	PrintStartupBanner(&buf, cfg)
	out := buf.String()

	requiredSubstrings := []string{
		"Periodic Scrape Interval : 1m0s",
		"infra.json",
		"workflows.json",
		"execution-time.json",
		"Nodes (v1/Node)",
		"Namespaces (v1/Namespace)",
		"RuntimeClasses (node.k8s.io/v1)",
		"ClusterQueues",
		"Cohorts",
		"LocalQueues",
		"ResourceFlavors",
		"Topologies",
		"WorkloadPriorityClasses",
		"Scheduled & Running Pods",
		"Information Omitted / Sanitized",
		"SHA-256 Name Replacement",
		"Enabled                            : true",
		"Workloads, Pods, Jobs, JobSets, LeaderWorkerSets, Deployments, RayJobs, RayClusters",
		"Namespaces, Nodes, ClusterQueues, Cohorts, LocalQueues, ResourceFlavors, Topologies, WorkloadPriorityClasses, RuntimeClasses",
	}
	for _, sub := range requiredSubstrings {
		if !strings.Contains(out, sub) {
			t.Errorf("startup banner missing expected text %q:\n%s", sub, out)
		}
	}
}

func TestShouldKeepAnnotation(t *testing.T) {
	tests := []struct {
		key  string
		want bool
	}{
		// Allowed official domains and subdomains.
		{key: "kubernetes.io/psp", want: true},
		{key: "batch.kubernetes.io/job-tracking", want: true},
		{key: "cluster-autoscaler.kubernetes.io/safe-to-evict", want: true},
		{key: "k8s.io/custom", want: true},
		{key: "jobset.sigs.k8s.io/jobset-name", want: true},
		{key: "kueue.x-k8s.io/queue-name", want: true},
		{key: "alpha.jobset.x-k8s.io/exclusive-topology", want: true},
		{key: "leaderworkerset.x-k8s.io/exclusive-topology", want: true},
		{key: "kwok.x-k8s.io/node", want: true},
		{key: "kueue-optimizer.gke.io/pod-execution-time-seconds", want: true},
		{key: "sub.kueue-optimizer.gke.io/metric", want: true},
		{key: "KUEUE.X-K8S.IO/Queue-Name", want: true},

		// Explicitly denied sensitive Kubernetes annotations.
		{key: corev1.LastAppliedConfigAnnotation, want: false},
		{key: "Kubectl.Kubernetes.io/Last-Applied-Configuration", want: false},
		{key: "kubernetes.io/change-cause", want: false},
		{key: "kubernetes.io/description", want: false},
		{key: "kubernetes.io/service-account.name", want: false},
		{key: "kubernetes.io/service-account.uid", want: false},
		{key: "pod.alpha.kubernetes.io/init-containers", want: false},
		{key: "pod.beta.kubernetes.io/init-containers", want: false},
		{key: "pod.alpha.kubernetes.io/init-container-statuses", want: false},
		{key: "pod.beta.kubernetes.io/init-container-statuses", want: false},
		{key: "csi.volume.kubernetes.io/nodeid", want: false},
		{key: "alpha.kubernetes.io/provided-node-ip", want: false},

		// Custom/third-party domains that contain "k8s", "kubernetes", "x-k8s", or "kueue-optimizer" as substrings.
		{key: "k8s.client-domain.com/owner", want: false},
		{key: "kubernetes.client-domain.com/owner", want: false},
		{key: "x-k8s.client-domain.com/owner", want: false},
		{key: "kueue-optimizer.client-domain.com/owner", want: false},
		{key: "client-domain.com/k8s-owner", want: false},
		{key: "client-domain.com/kubernetes-secret", want: false},
		{key: "client-domain.com/kueue-optimizer", want: false},
		{key: "fake-k8s.io/owner", want: false},
		{key: "notkubernetes.io/owner", want: false},
		{key: "myx-k8s.io/owner", want: false},
		{key: "not-kueue-optimizer.gke.io/owner", want: false},
		{key: "k8s.io.client-domain.com/owner", want: false},
		{key: "kubernetes.io.client-domain.com/owner", want: false},

		// Unprefixed and malformed annotation keys.
		{key: "k8s-owner", want: false},
		{key: "kubernetes-admin", want: false},
		{key: "kueue-optimizer", want: false},
		{key: "kubernetes.io/", want: false},
		{key: "/kubernetes.io", want: false},
		{key: ".kubernetes.io/foo", want: false},
		{key: "kubernetes.io/nested/slash", want: false},
		{key: "", want: false},
	}

	for _, tc := range tests {
		if got := ShouldKeepAnnotation(tc.key); got != tc.want {
			t.Errorf("ShouldKeepAnnotation(%q) = %v, want %v", tc.key, got, tc.want)
		}
	}
}

func TestNodeLabelSanitization(t *testing.T) {
	savedLabels := map[string]string{
		"instance":                            "gpu",
		"role":                                "worker",
		"type":                                "kwok",
		"kubernetes.io/hostname":              "kwok-node-0",
		"kubernetes.io/arch":                  "amd64",
		"node.kubernetes.io/instance-type":    "a2-highgpu-8g",
		"topology.kubernetes.io/zone":         "us-central1-a",
		"cloud.google.com/gke-accelerator":    "nvidia-tesla-a100",
		"cloud.google.com/gke-nodepool":       "gpu-pool",
		"cloud.google.com/gke-topology-block": "block-1",
	}
	omittedNodeLabels := []string{
		"cloud.google.com/gke-cluster-name",
		"CLOUD.GOOGLE.COM/GKE-BOOT-DISK",
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

	input := make(map[string]string, len(savedLabels)+len(omittedNodeLabels))
	for k, v := range savedLabels {
		input[k] = v
	}
	for _, k := range omittedNodeLabels {
		input[k] = testSecretValue
	}

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "kwok-node-0",
			Labels: input,
		},
		Status: corev1.NodeStatus{
			DaemonEndpoints: corev1.NodeDaemonEndpoints{
				KubeletEndpoint: corev1.DaemonEndpoint{Port: 10250},
			},
			NodeInfo: corev1.NodeSystemInfo{
				MachineID: testSecretValue,
			},
		},
	}
	sanitizedMap := SanitizeNode(node, NewSensitiveDataHasher(nil, nil))
	if status, ok := sanitizedMap["status"].(map[string]any); ok {
		if _, hasNodeInfo := status["nodeInfo"]; hasNodeInfo {
			t.Errorf("expected SanitizeNode to strip status.nodeInfo, got %v", status["nodeInfo"])
		}
		if _, hasDaemonEndpoints := status["daemonEndpoints"]; hasDaemonEndpoints {
			t.Errorf("expected SanitizeNode to strip status.daemonEndpoints, got %v", status["daemonEndpoints"])
		}
	} else {
		t.Fatalf("expected SanitizeNode map to contain status map, got %T", sanitizedMap["status"])
	}
	sanitizedBytes, err := json.Marshal(sanitizedMap)
	if err != nil {
		t.Fatalf("marshal sanitized node: %v", err)
	}
	var sanitized corev1.Node
	if err := json.Unmarshal(sanitizedBytes, &sanitized); err != nil {
		t.Fatalf("unmarshal sanitized node: %v", err)
	}

	for k, want := range savedLabels {
		if got := sanitized.Labels[k]; got != want {
			t.Errorf("expected node label %q=%q to be saved, got %q", k, want, got)
		}
	}
	for _, k := range omittedNodeLabels {
		if got, exists := sanitized.Labels[k]; exists {
			t.Errorf("expected node label %q to be omitted, got %q", k, got)
		}
	}
}

func TestFilterLabels(t *testing.T) {
	filtered := FilterLabels(map[string]string{
		"keep-key":        "ok",
		"Custom.Deny/Key": testSecretValue,
		"exact.deny/key":  testSecretValue,
	}, []string{"custom.deny/key", "exact.deny/key"})
	if filtered["keep-key"] != "ok" {
		t.Errorf("expected keep-key to be saved, got %v", filtered)
	}
	for _, omitted := range []string{"Custom.Deny/Key", "custom.deny/key", "exact.deny/key"} {
		if _, exists := filtered[omitted]; exists {
			t.Errorf("expected %q to be omitted", omitted)
		}
	}
	if got := FilterLabels(map[string]string{"only-denied": "v"}, []string{"only-denied"}); got != nil {
		t.Errorf("expected nil when all labels are omitted, got %v", got)
	}
}

func TestSanitizationListsAreLowercase(t *testing.T) {
	for _, list := range []struct {
		name  string
		items []string
	}{
		{"deniedNodeLabelKeys", deniedNodeLabelKeys},
		{"hashedLabelKeys", hashedLabelKeys},
		{"deniedAnnotationKeys", deniedAnnotationKeys},
		{"allowedAnnotationDomains", allowedAnnotationDomains},
	} {
		for _, item := range list.items {
			if item != strings.ToLower(item) {
				t.Errorf("%s entry %q must be lowercase", list.name, item)
			}
		}
	}
	for k := range protectedLabelKeys {
		if k != strings.ToLower(k) {
			t.Errorf("protectedLabelKeys key %q must be lowercase", k)
		}
	}
	for k := range labelKeysHoldingWorkloadNames {
		if k != strings.ToLower(k) {
			t.Errorf("labelKeysHoldingWorkloadNames key %q must be lowercase", k)
		}
	}
	for k := range annotationKeysHoldingWorkloadNames {
		if k != strings.ToLower(k) {
			t.Errorf("annotationKeysHoldingWorkloadNames key %q must be lowercase", k)
		}
	}
}

func TestSanitizeLabels(t *testing.T) {
	savedPlain := map[string]string{
		KueueQueueLabel:         "main-lq",
		KueueManagedLabel:       "true",
		KueuePriorityClassLabel: "high-priority",
		"component":             "trainer",
	}
	hashedWorkloadLabels := []string{
		"app",
		"app.kubernetes.io/name",
		"app.kubernetes.io/instance",
		"job-name",
		"batch.kubernetes.io/job-name",
		"jobset.sigs.k8s.io/jobset-name",
		"jobset.sigs.k8s.io/coordinator",
		"alpha.jobset.sigs.k8s.io/namespaced-job",
		"leaderworkerset.sigs.k8s.io/name",
		"leaderworkerset.sigs.k8s.io/leader-name",
		"ray.io/cluster",
		KueuePrebuiltWorkloadLabel,
		KueuePodGroupNameLabel,
	}
	hashedValueLabels := []string{
		"owner",
		"TEAM",
		"customer",
		"cost-center",
		"project",
		"email",
		"custom-unprefixed",
		"corp.io/owner",
		"k8s.io/owner",
		"kueue-optimizer.gke.io/team",
		"corp.io/custom-tag",
		"sub.wildcard.io/secret-tag",
	}

	rawWorkloadRef := testSecretNamePrefix + "workload"
	input := make(map[string]string)
	for k, v := range savedPlain {
		input[k] = v
	}
	for _, k := range hashedWorkloadLabels {
		input[k] = rawWorkloadRef
	}
	for _, k := range hashedValueLabels {
		input[k] = testSecretValue
	}

	hasher := NewSensitiveDataHasher([]string{
		"Corp.IO/Custom-Tag",
		"custom-unprefixed",
		"wildcard.io/*",
		KueueQueueLabel,         // protected control-plane key must never be hashed
		KueuePriorityClassLabel, // protected priority-class key must never be hashed
	}, nil)
	sanitized := SanitizeLabels(input, hasher)

	for k, v := range sanitized {
		if errs := validation.IsQualifiedName(k); len(errs) > 0 {
			t.Errorf("sanitized label key %q is not a valid Kubernetes label key: %v", k, errs)
		}
		if errs := validation.IsValidLabelValue(v); len(errs) > 0 {
			t.Errorf("sanitized label %q=%q is not a valid Kubernetes label value: %v", k, v, errs)
		}
	}

	for k, want := range savedPlain {
		if got := sanitized[k]; got != want {
			t.Errorf("expected label %q=%q to be preserved in plain text, got %q", k, want, got)
		}
	}
	wantWorkloadHash := hasher.HashName(rawWorkloadRef)
	for _, k := range hashedWorkloadLabels {
		if got := sanitized[k]; got != wantWorkloadHash {
			t.Errorf("expected workload-name label %q to be hashed to %q, got %q", k, wantWorkloadHash, got)
		}
	}
	wantSecretHash := hasher.HashName(testSecretValue)
	for _, k := range hashedValueLabels {
		if got := sanitized[k]; got != wantSecretHash {
			t.Errorf("expected sensitive label %q to keep its key and have hashed value %q, got %q", k, wantSecretHash, got)
		}
	}

	// Verify NoopHasher leaves both workload-referencing and sensitive label values in plain text.
	noopSanitized := SanitizeLabels(input, NewNoopHasher())
	for k, want := range input {
		if got := noopSanitized[k]; got != want {
			t.Errorf("expected NoopHasher to leave label %q=%q in plain text, got %q", k, want, got)
		}
	}
}

func TestSaltedLabelHashingPreventsDictionaryAndCrossRunCorrelation(t *testing.T) {
	lowEntropyVal := "ai"
	unsaltedSum := sha256.Sum256([]byte(lowEntropyVal))
	unsaltedDigest := "h-" + hex.EncodeToString(unsaltedSum[:16])

	run1 := NewSensitiveDataHasher(nil, nil)
	run2 := NewSensitiveDataHasher(nil, nil)

	h1 := run1.HashName(lowEntropyVal)
	h2 := run2.HashName(lowEntropyVal)

	if len(h1) != 34 || !strings.HasPrefix(h1, "h-") {
		t.Errorf("expected 34-char h-<32-hex> HMAC-SHA256 digest, got %q", h1)
	}
	if h1 == unsaltedDigest || h2 == unsaltedDigest {
		t.Errorf("expected salted HMAC-SHA256 digest to differ from unsalted SHA-256 %q", unsaltedDigest)
	}
	if h1 == h2 {
		t.Errorf("expected independent per-run CSPRNG salts to produce distinct digests, both produced %q", h1)
	}
	// Within the same run, hashing the same value is deterministic.
	if run1.HashName(lowEntropyVal) != h1 {
		t.Errorf("expected deterministic digest within the same SensitiveDataHasher run")
	}
}

func TestNoopHasherPreservesPlainNamesAndLabels(t *testing.T) {
	rawName := "jobset-train-01"
	rawLabelVal := "ai"
	noop := NewNoopHasher()

	if noop.HashName(rawName) != rawName || noop.HashNameForKind("JobSet", rawName) != rawName {
		t.Errorf("expected NoopHasher to preserve plain name %q", rawName)
	}
	if noop.HashName(rawLabelVal) != rawLabelVal {
		t.Errorf("expected NoopHasher to preserve plain label value %q", rawLabelVal)
	}
	if noop.ShouldHashLabel("team") || noop.ShouldHashWorkloadLabel("app") {
		t.Errorf("expected NoopHasher ShouldHashLabel/ShouldHashWorkloadLabel to return false")
	}
}

func TestHashNameForKind(t *testing.T) {
	hasher := NewSensitiveDataHasher(nil, nil)
	rawName := "sample-object-name"

	infraKinds := []string{
		"Namespace",
		"Node",
		"ClusterQueue",
		"Cohort",
		"LocalQueue",
		"ResourceFlavor",
		"Topology",
		"WorkloadPriorityClass",
		"RuntimeClass",
	}
	for _, kind := range infraKinds {
		if got := hasher.HashNameForKind(kind, rawName); got != rawName {
			t.Errorf("HashNameForKind(%q, %q) = %q, want plain %q", kind, rawName, got, rawName)
		}
		if got := hasher.HashNameForKind(kind, ""); got != "" {
			t.Errorf("HashNameForKind(%q, \"\") = %q, want empty string", kind, got)
		}
	}

	wantHashed := hasher.HashName(rawName)
	workloadKinds := []string{
		"Workload",
		"Pod",
		"Job",
		"JobSet",
		"LeaderWorkerSet",
		"Deployment",
		"RayJob",
		"RayCluster",
	}
	for _, kind := range workloadKinds {
		if got := hasher.HashNameForKind(kind, rawName); got != wantHashed {
			t.Errorf("HashNameForKind(%q, %q) = %q, want hashed %q", kind, rawName, got, wantHashed)
		}
		if got := hasher.HashNameForKind(kind, ""); got != "" {
			t.Errorf("HashNameForKind(%q, \"\") = %q, want empty string", kind, got)
		}
	}
}

func TestStartupBannerDoesNotLeakHashSalt(t *testing.T) {
	customSalt := []byte("super-secret-salt-must-not-print")
	var banner bytes.Buffer
	PrintStartupBanner(&banner, Config{
		Interval:        60 * time.Second,
		HashNames:       true,
		PauseImage:      DefaultPauseImage,
		ExtraHashLabels: []string{"corp.io/team"},
		HashSalt:        customSalt,
	})
	bannerStr := banner.String()
	if strings.Contains(bannerStr, string(customSalt)) || strings.Contains(bannerStr, hex.EncodeToString(customSalt)) {
		t.Errorf("startup banner leaked HashSalt:\n%s", bannerStr)
	}
}

func TestSelectorAndPodAffinityLabelHashingConsistency(t *testing.T) {
	hasher := NewSensitiveDataHasher([]string{"corp.io/*"}, nil)
	rawDeployName := testSecretNamePrefix + "ranker"
	wantDeployHash := hasher.HashName(rawDeployName)
	wantSecretHash := hasher.HashName(testSecretValue)

	// Deployment selecting pods ONLY via sensitive/hashed labels must keep a non-empty valid selector
	// (with original keys and hashed values) that matches its sanitized PodTemplateSpec labels, and PodSpec
	// affinities/spread constraints must match too.
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rawDeployName,
			Namespace: "ai-lab",
			Labels: map[string]string{
				"team":            testSecretValue,
				"project":         testSecretValue,
				"corp.io/service": testSecretValue,
			},
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"team":            testSecretValue,
					"project":         testSecretValue,
					"corp.io/service": testSecretValue,
				},
				MatchExpressions: []metav1.LabelSelectorRequirement{
					{
						Key:      "corp.io/tier",
						Operator: metav1.LabelSelectorOpIn,
						Values:   []string{testSecretValue},
					},
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app":             rawDeployName,
						"team":            testSecretValue,
						"project":         testSecretValue,
						"corp.io/service": testSecretValue,
						"corp.io/tier":    testSecretValue,
					},
				},
				Spec: corev1.PodSpec{
					Affinity: &corev1.Affinity{
						PodAntiAffinity: &corev1.PodAntiAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{
								{
									LabelSelector: &metav1.LabelSelector{
										MatchLabels: map[string]string{
											"app":  rawDeployName,
											"team": testSecretValue,
										},
									},
									MatchLabelKeys:    []string{"corp.io/service", "app"},
									MismatchLabelKeys: []string{"corp.io/tier"},
									TopologyKey:       "corp.io/team",
								},
							},
						},
					},
					TopologySpreadConstraints: []corev1.TopologySpreadConstraint{
						{
							MaxSkew:           1,
							TopologyKey:       "corp.io/team",
							WhenUnsatisfiable: corev1.DoNotSchedule,
							LabelSelector: &metav1.LabelSelector{
								MatchLabels: map[string]string{
									"app":             rawDeployName,
									"corp.io/service": testSecretValue,
								},
							},
							MatchLabelKeys: []string{"corp.io/tier"},
						},
					},
					Containers: []corev1.Container{{Name: "main", Image: testSecretImage}},
				},
			},
		},
	}

	sanitizedMap := SanitizeDeployment(deploy, DefaultPauseImage, hasher)
	rawJSON, err := json.Marshal(sanitizedMap)
	if err != nil {
		t.Fatalf("marshal sanitized deployment: %v", err)
	}
	jsonStr := string(rawJSON)
	for _, secret := range testForbiddenSubstrings {
		if strings.Contains(jsonStr, secret) {
			t.Errorf("sanitized Deployment leaked %q:\n%s", secret, jsonStr)
		}
	}

	var roundTrip appsv1.Deployment
	if err := json.Unmarshal(rawJSON, &roundTrip); err != nil {
		t.Fatalf("unmarshal sanitized deployment: %v", err)
	}
	if roundTrip.Name != wantDeployHash || roundTrip.Spec.Template.Labels["app"] != wantDeployHash {
		t.Errorf("expected sanitized Deployment.Name and template app label to equal %q; got name=%q app=%q", wantDeployHash, roundTrip.Name, roundTrip.Spec.Template.Labels["app"])
	}
	// Explicitly verify exact hashed selector values so a tautological/always-true selector cannot pass silently.
	for _, key := range []string{"team", "project", "corp.io/service"} {
		if got := roundTrip.Spec.Selector.MatchLabels[key]; got != wantSecretHash {
			t.Errorf("expected Deployment.Spec.Selector.MatchLabels[%q]=%q, got %q", key, wantSecretHash, got)
		}
	}
	if len(roundTrip.Spec.Selector.MatchExpressions) != 1 ||
		roundTrip.Spec.Selector.MatchExpressions[0].Key != "corp.io/tier" ||
		!slices.Equal(roundTrip.Spec.Selector.MatchExpressions[0].Values, []string{wantSecretHash}) {
		t.Errorf("unexpected Deployment.Spec.Selector.MatchExpressions: %+v", roundTrip.Spec.Selector.MatchExpressions)
	}

	sel, err := metav1.LabelSelectorAsSelector(roundTrip.Spec.Selector)
	if err != nil {
		t.Fatalf("Deployment.Spec.Selector invalid after sanitization: %v", err)
	}
	podLabels := labels.Set(roundTrip.Spec.Template.Labels)
	if !sel.Matches(podLabels) {
		t.Errorf("sanitized Deployment selector %v does not match sanitized pod template labels %v", roundTrip.Spec.Selector, podLabels)
	}
	if sel.Matches(labels.Set{"team": "other"}) {
		t.Errorf("sanitized Deployment selector %v unexpectedly matched unrelated labels", roundTrip.Spec.Selector)
	}

	antiTerm := roundTrip.Spec.Template.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution[0]
	if antiTerm.LabelSelector.MatchLabels["app"] != wantDeployHash || antiTerm.LabelSelector.MatchLabels["team"] != wantSecretHash {
		t.Errorf("unexpected PodAntiAffinity MatchLabels: %v", antiTerm.LabelSelector.MatchLabels)
	}
	antiSel, err := metav1.LabelSelectorAsSelector(antiTerm.LabelSelector)
	if err != nil || !antiSel.Matches(podLabels) {
		t.Errorf("sanitized PodAntiAffinity labelSelector %v failed to match pod labels %v (err=%v)", antiTerm.LabelSelector, podLabels, err)
	}
	tsc := roundTrip.Spec.Template.Spec.TopologySpreadConstraints[0]
	if tsc.LabelSelector.MatchLabels["app"] != wantDeployHash || tsc.LabelSelector.MatchLabels["corp.io/service"] != wantSecretHash {
		t.Errorf("unexpected TopologySpreadConstraint MatchLabels: %v", tsc.LabelSelector.MatchLabels)
	}
	tscSel, err := metav1.LabelSelectorAsSelector(tsc.LabelSelector)
	if err != nil || !tscSel.Matches(podLabels) {
		t.Errorf("sanitized TopologySpreadConstraint labelSelector %v failed to match pod labels %v (err=%v)", tsc.LabelSelector, podLabels, err)
	}
	if antiTerm.TopologyKey != "corp.io/team" || tsc.TopologyKey != "corp.io/team" {
		t.Errorf("expected topology keys to remain 'corp.io/team'; got anti=%q tsc=%q", antiTerm.TopologyKey, tsc.TopologyKey)
	}
}

func TestInfraAndWorkloadLabelHashingConsistency(t *testing.T) {
	hasher := NewSensitiveDataHasher([]string{"corp.io/*"}, nil)
	wantSecretHash := hasher.HashName(testSecretValue)

	podSpec := &corev1.PodSpec{
		NodeSelector: map[string]string{
			"cloud.google.com/gke-cluster-name": testSecretValue,
			"corp.io/team":                      testSecretValue,
			"instance":                          "gpu",
		},
		Affinity: &corev1.Affinity{
			NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{
						{
							MatchExpressions: []corev1.NodeSelectorRequirement{
								{
									Key:      "cloud.google.com/gke-cluster-name",
									Operator: corev1.NodeSelectorOpIn,
									Values:   []string{testSecretValue},
								},
								{
									Key:      "corp.io/team",
									Operator: corev1.NodeSelectorOpIn,
									Values:   []string{testSecretValue},
								},
							},
						},
					},
				},
			},
			PodAntiAffinity: &corev1.PodAntiAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{
					{
						NamespaceSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{
								"corp.io/tenant": testSecretValue,
							},
						},
						TopologyKey: "corp.io/team",
					},
				},
			},
		},
		Containers: []corev1.Container{{Name: "main", Image: testSecretImage}},
	}
	SanitizePodSpec(podSpec, DefaultPauseImage, hasher)

	nsMap := SanitizeNamespace(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "ai-lab",
			Labels: map[string]string{
				"team":           testSecretValue,
				"corp.io/tenant": testSecretValue,
			},
		},
	}, hasher)
	cqMap := SanitizeClusterQueue(&kueuev1beta2.ClusterQueue{
		ObjectMeta: metav1.ObjectMeta{Name: "ai-lab-cq"},
		Spec: kueuev1beta2.ClusterQueueSpec{
			NamespaceSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"team":           testSecretValue,
					"corp.io/tenant": testSecretValue,
				},
			},
		},
	}, hasher)
	nsBytes, _ := json.Marshal(nsMap)
	cqBytes, _ := json.Marshal(cqMap)
	var cleanNS corev1.Namespace
	var cleanCQ kueuev1beta2.ClusterQueue
	_ = json.Unmarshal(nsBytes, &cleanNS)
	_ = json.Unmarshal(cqBytes, &cleanCQ)
	if cleanNS.Labels["team"] != wantSecretHash || cleanCQ.Spec.NamespaceSelector.MatchLabels["team"] != wantSecretHash {
		t.Errorf("expected default sensitive key 'team' to have hashed value %q across Namespace and ClusterQueue, got ns=%v cq=%v", wantSecretHash, cleanNS.Labels, cleanCQ.Spec.NamespaceSelector)
	}
	cqNsSel, err := metav1.LabelSelectorAsSelector(cleanCQ.Spec.NamespaceSelector)
	if err != nil || !cqNsSel.Matches(labels.Set(cleanNS.Labels)) {
		t.Errorf("sanitized ClusterQueue.Spec.NamespaceSelector %v does not match sanitized Namespace labels %v (err=%v)", cleanCQ.Spec.NamespaceSelector, cleanNS.Labels, err)
	}
	antiTerm := podSpec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution[0]
	antiNsSel, err := metav1.LabelSelectorAsSelector(antiTerm.NamespaceSelector)
	if err != nil || !antiNsSel.Matches(labels.Set(cleanNS.Labels)) {
		t.Errorf("sanitized PodAntiAffinity.NamespaceSelector %v does not match sanitized Namespace labels %v (err=%v)", antiTerm.NamespaceSelector, cleanNS.Labels, err)
	}

	nodeMap := SanitizeNode(&corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-0",
			Labels: map[string]string{
				"corp.io/team": testSecretValue,
				"instance":     "gpu",
			},
		},
	}, hasher)
	rfMap := SanitizeResourceFlavor(&kueuev1beta2.ResourceFlavor{
		ObjectMeta: metav1.ObjectMeta{Name: "gpu-rf"},
		Spec: kueuev1beta2.ResourceFlavorSpec{
			NodeLabels: map[string]string{
				"corp.io/team": testSecretValue,
				"instance":     "gpu",
			},
		},
	}, hasher)
	topoMap := SanitizeTopology(&kueuev1beta2.Topology{
		ObjectMeta: metav1.ObjectMeta{Name: "team-topo"},
		Spec: kueuev1beta2.TopologySpec{
			Levels: []kueuev1beta2.TopologyLevel{{NodeLabel: "corp.io/team"}},
		},
	}, hasher)
	rcMap := SanitizeRuntimeClass(&nodev1.RuntimeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "gpu-runtime"},
		Handler:    "nvidia",
		Scheduling: &nodev1.Scheduling{
			NodeSelector: map[string]string{
				"cloud.google.com/gke-cluster-name": testSecretValue,
				"corp.io/team":                      testSecretValue,
				"instance":                          "gpu",
			},
		},
	}, hasher)
	nodeBytes, _ := json.Marshal(nodeMap)
	rfBytes, _ := json.Marshal(rfMap)
	topoBytes, _ := json.Marshal(topoMap)
	rcBytes, _ := json.Marshal(rcMap)
	var cleanNode corev1.Node
	var cleanRF kueuev1beta2.ResourceFlavor
	var cleanTopo kueuev1beta2.Topology
	var cleanRC nodev1.RuntimeClass
	_ = json.Unmarshal(nodeBytes, &cleanNode)
	_ = json.Unmarshal(rfBytes, &cleanRF)
	_ = json.Unmarshal(topoBytes, &cleanTopo)
	_ = json.Unmarshal(rcBytes, &cleanRC)

	if cleanNode.Labels["corp.io/team"] != wantSecretHash {
		t.Errorf("expected preserved key 'corp.io/team' with hashed value on Node, got %v", cleanNode.Labels)
	}
	if cleanRF.Spec.NodeLabels["corp.io/team"] != cleanNode.Labels["corp.io/team"] {
		t.Errorf("ResourceFlavor.Spec.NodeLabels %v out of sync with Node.Labels %v", cleanRF.Spec.NodeLabels, cleanNode.Labels)
	}
	if cleanRC.Scheduling == nil || cleanRC.Scheduling.NodeSelector["corp.io/team"] != cleanNode.Labels["corp.io/team"] {
		t.Errorf("RuntimeClass.Scheduling.NodeSelector %v out of sync with Node.Labels %v", cleanRC.Scheduling, cleanNode.Labels)
	}
	if _, leaked := cleanRC.Scheduling.NodeSelector["cloud.google.com/gke-cluster-name"]; leaked {
		t.Errorf("RuntimeClass.Scheduling.NodeSelector must strip cloud-identifying label")
	}
	if cleanTopo.Spec.Levels[0].NodeLabel != "corp.io/team" {
		t.Errorf("expected Topology level NodeLabel to remain 'corp.io/team', got %q", cleanTopo.Spec.Levels[0].NodeLabel)
	}
	for k, v := range podSpec.NodeSelector {
		if cleanNode.Labels[k] != v {
			t.Errorf("PodSpec.NodeSelector[%q]=%q does not match Node.Labels %v", k, v, cleanNode.Labels)
		}
	}
	nodeTerms := podSpec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	if len(nodeTerms) != 1 || len(nodeTerms[0].MatchExpressions) != 1 ||
		nodeTerms[0].MatchExpressions[0].Key != "corp.io/team" ||
		!slices.Equal(nodeTerms[0].MatchExpressions[0].Values, []string{wantSecretHash}) {
		t.Errorf("unexpected sanitized NodeAffinity terms: %+v", nodeTerms)
	}
}

func TestDefaultSensitiveNodeAndResourceFlavorLabelHashing(t *testing.T) {
	defaultHasher := NewSensitiveDataHasher(nil, nil)
	wantHash := defaultHasher.HashName(testSecretValue)

	nodeMap := SanitizeNode(&corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "node-default",
			Labels: map[string]string{"team": testSecretValue, "instance": "gpu"},
		},
	}, defaultHasher)
	rfMap := SanitizeResourceFlavor(&kueuev1beta2.ResourceFlavor{
		ObjectMeta: metav1.ObjectMeta{Name: "gpu-rf"},
		Spec: kueuev1beta2.ResourceFlavorSpec{
			NodeLabels: map[string]string{"team": testSecretValue, "instance": "gpu"},
		},
	}, defaultHasher)
	nodeBytes, _ := json.Marshal(nodeMap)
	rfBytes, _ := json.Marshal(rfMap)
	var cleanNode corev1.Node
	var cleanRF kueuev1beta2.ResourceFlavor
	_ = json.Unmarshal(nodeBytes, &cleanNode)
	_ = json.Unmarshal(rfBytes, &cleanRF)

	if cleanNode.Labels["team"] != wantHash {
		t.Errorf("expected default 'team' label on Node to be hashed to %q without --extra-hash-labels, got %v", wantHash, cleanNode.Labels)
	}
	if cleanRF.Spec.NodeLabels["team"] != wantHash {
		t.Errorf("expected default 'team' label on ResourceFlavor to be hashed to %q without --extra-hash-labels, got %v", wantHash, cleanRF.Spec.NodeLabels)
	}
}

func TestFilterAnnotations(t *testing.T) {
	secretJobName := testSecretNamePrefix + "job"
	secretReplacedSlice := testSecretNamePrefix + "old-slice"
	secretCoordinator := testSecretNamePrefix + "jobset-leader-0-0." + testSecretNamePrefix + "jobset"
	secretLeaderPod := testSecretNamePrefix + "lws-0"
	secretLeaderAddr := testSecretNamePrefix + "lws-0." + testSecretNamePrefix + "lws"
	ann := map[string]string{
		"business.corp.com/cost-center":                     testSecretValue,
		"team.internal/owner":                               testSecretEmail,
		"k8s.client-domain.com/owner":                       testSecretEmail,
		"kubernetes.client-domain.com/team":                 testSecretEmail,
		"client-domain.com/k8s-owner":                       testSecretEmail,
		"k8s-unprefixed-owner":                              testSecretEmail,
		corev1.LastAppliedConfigAnnotation:                  `{"image":"` + testSecretImage + `","secret":"` + testSecretValue + `"}`,
		"kubernetes.io/change-cause":                        "kubectl set image job/" + secretJobName + " main=" + testSecretImage + " --record",
		"kubernetes.io/description":                         testSecretValue,
		"kubernetes.io/service-account.name":                testSecretValue,
		"kubernetes.io/service-account.uid":                 testSecretValue,
		"kueue.x-k8s.io/queue-name":                         "main-lq",
		"kueue-optimizer.gke.io/pod-execution-time-seconds": "15",
		"kubernetes.io/psp":                                 "restricted",
		"k8s.io/custom":                                     "yes",
		"kueue.x-k8s.io/job-owner-name":                     secretJobName,
		KueueWorkloadSliceReplacementForAnnot:               secretReplacedSlice,
		"jobset.sigs.k8s.io/coordinator":                    secretCoordinator,
		"alpha.jobset.sigs.k8s.io/namespaced-job":           "ai-lab/" + secretJobName,
		"leaderworkerset.sigs.k8s.io/leader-name":           secretLeaderPod,
		"leaderworkerset.sigs.k8s.io/leader-address":        secretLeaderAddr,
	}

	hasher := NewSensitiveDataHasher(nil, nil)
	filtered := FilterAnnotations(ann, hasher)
	for _, strippedKey := range []string{
		"business.corp.com/cost-center",
		"team.internal/owner",
		"k8s.client-domain.com/owner",
		"kubernetes.client-domain.com/team",
		"client-domain.com/k8s-owner",
		"k8s-unprefixed-owner",
		corev1.LastAppliedConfigAnnotation,
		"kubernetes.io/change-cause",
		"kubernetes.io/description",
		"kubernetes.io/service-account.name",
		"kubernetes.io/service-account.uid",
	} {
		if _, exists := filtered[strippedKey]; exists {
			t.Errorf("expected annotation %q to be stripped, got value %q", strippedKey, filtered[strippedKey])
		}
	}
	if filtered["kueue.x-k8s.io/queue-name"] != "main-lq" {
		t.Errorf("expected queue-name annotation to be preserved unhashed, got %q", filtered["kueue.x-k8s.io/queue-name"])
	}
	if filtered["kueue-optimizer.gke.io/pod-execution-time-seconds"] != "15" {
		t.Errorf("expected kueue-optimizer annotation to be preserved, got %q", filtered["kueue-optimizer.gke.io/pod-execution-time-seconds"])
	}
	if filtered["kubernetes.io/psp"] != "restricted" {
		t.Errorf("expected kubernetes annotation to be preserved")
	}
	if filtered["k8s.io/custom"] != "yes" {
		t.Errorf("expected k8s annotation to be preserved")
	}
	for k, rawVal := range map[string]string{
		"kueue.x-k8s.io/job-owner-name":              secretJobName,
		KueueWorkloadSliceReplacementForAnnot:        secretReplacedSlice,
		"jobset.sigs.k8s.io/coordinator":             secretCoordinator,
		"alpha.jobset.sigs.k8s.io/namespaced-job":    "ai-lab/" + secretJobName,
		"leaderworkerset.sigs.k8s.io/leader-name":    secretLeaderPod,
		"leaderworkerset.sigs.k8s.io/leader-address": secretLeaderAddr,
	} {
		if filtered[k] != hasher.HashName(rawVal) {
			t.Errorf("expected %q annotation value to be HMAC-SHA256 hashed, got %q", k, filtered[k])
		}
	}

	// Verify NoopHasher preserves name-holding annotations unhashed while still stripping disallowed annotations.
	noopFiltered := FilterAnnotations(ann, NewNoopHasher())
	if noopFiltered["kueue.x-k8s.io/job-owner-name"] != secretJobName {
		t.Errorf("expected NoopHasher to leave job-owner-name plain %q, got %q", secretJobName, noopFiltered["kueue.x-k8s.io/job-owner-name"])
	}
	if _, exists := noopFiltered[corev1.LastAppliedConfigAnnotation]; exists {
		t.Errorf("expected sensitive annotation to still be stripped under NoopHasher")
	}
}

func TestPodSpecProbesLifecycleAndIdentitySanitization(t *testing.T) {
	secretProbe := &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			Exec: &corev1.ExecAction{Command: []string{"/bin/check", testSecretValue}},
			HTTPGet: &corev1.HTTPGetAction{
				Path:        "/" + testSecretValue,
				HTTPHeaders: []corev1.HTTPHeader{{Name: "Authorization", Value: testSecretValue}},
			},
		},
	}
	secretLifecycle := &corev1.Lifecycle{
		PostStart: &corev1.LifecycleHandler{
			Exec: &corev1.ExecAction{Command: []string{"/bin/post-start", testSecretValue}},
		},
		PreStop: &corev1.LifecycleHandler{
			HTTPGet: &corev1.HTTPGetAction{
				HTTPHeaders: []corev1.HTTPHeader{{Name: "X-Auth-Token", Value: testSecretValue}},
			},
		},
	}

	spec := &corev1.PodSpec{
		ServiceAccountName:       testSecretValue,
		DeprecatedServiceAccount: testSecretValue,
		Hostname:                 testSecretValue,
		HostnameOverride:         ptr.To(testSecretValue),
		Subdomain:                testSecretValue,
		HostAliases:              []corev1.HostAlias{{IP: testSecretIP, Hostnames: []string{testSecretValue}}},
		DNSConfig:                &corev1.PodDNSConfig{Nameservers: []string{testSecretIP}, Searches: []string{testSecretValue}},
		Containers: []corev1.Container{
			{
				Name:           "main",
				Image:          testSecretImage,
				LivenessProbe:  secretProbe.DeepCopy(),
				ReadinessProbe: secretProbe.DeepCopy(),
				StartupProbe:   secretProbe.DeepCopy(),
				Lifecycle:      secretLifecycle.DeepCopy(),
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")},
				},
			},
		},
		InitContainers: []corev1.Container{
			{
				Name:           "init",
				Image:          testSecretImage,
				LivenessProbe:  secretProbe.DeepCopy(),
				ReadinessProbe: secretProbe.DeepCopy(),
				StartupProbe:   secretProbe.DeepCopy(),
				Lifecycle:      secretLifecycle.DeepCopy(),
			},
		},
		EphemeralContainers: []corev1.EphemeralContainer{
			{
				EphemeralContainerCommon: corev1.EphemeralContainerCommon{
					Name:           "debug",
					Image:          testSecretImage,
					LivenessProbe:  secretProbe.DeepCopy(),
					ReadinessProbe: secretProbe.DeepCopy(),
					StartupProbe:   secretProbe.DeepCopy(),
					Lifecycle:      secretLifecycle.DeepCopy(),
				},
			},
		},
	}

	SanitizePodSpec(spec, DefaultPauseImage, NewSensitiveDataHasher(nil, nil))

	for _, f := range []struct {
		name  string
		value string
	}{
		{"ServiceAccountName", spec.ServiceAccountName},
		{"DeprecatedServiceAccount", spec.DeprecatedServiceAccount},
		{"Hostname", spec.Hostname},
		{"Subdomain", spec.Subdomain},
	} {
		if f.value != "" {
			t.Errorf("expected PodSpec.%s to be cleared, got %q", f.name, f.value)
		}
	}
	if spec.HostnameOverride != nil {
		t.Errorf("expected PodSpec.HostnameOverride to be nil, got %q", *spec.HostnameOverride)
	}
	if len(spec.HostAliases) > 0 {
		t.Errorf("expected PodSpec.HostAliases to be nil, got %v", spec.HostAliases)
	}
	if spec.DNSConfig != nil {
		t.Errorf("expected PodSpec.DNSConfig to be nil, got %v", spec.DNSConfig)
	}

	for _, c := range append(slices.Clone(spec.Containers), spec.InitContainers...) {
		for _, f := range []struct {
			name  string
			isNil bool
		}{
			{"LivenessProbe", c.LivenessProbe == nil},
			{"ReadinessProbe", c.ReadinessProbe == nil},
			{"StartupProbe", c.StartupProbe == nil},
			{"Lifecycle", c.Lifecycle == nil},
		} {
			if !f.isNil {
				t.Errorf("container %q expected %s to be nil", c.Name, f.name)
			}
		}
	}
	for _, ec := range spec.EphemeralContainers {
		for _, f := range []struct {
			name  string
			isNil bool
		}{
			{"LivenessProbe", ec.LivenessProbe == nil},
			{"ReadinessProbe", ec.ReadinessProbe == nil},
			{"StartupProbe", ec.StartupProbe == nil},
			{"Lifecycle", ec.Lifecycle == nil},
		} {
			if !f.isNil {
				t.Errorf("ephemeralContainer %q expected %s to be nil", ec.Name, f.name)
			}
		}
	}
	if gotCPU := spec.Containers[0].Resources.Requests[corev1.ResourceCPU]; !gotCPU.Equal(resource.MustParse("1")) {
		t.Errorf("expected container CPU request to be preserved, got %v", gotCPU)
	}
}

func TestComputePodResourceRequest(t *testing.T) {
	container := func(name string, requests corev1.ResourceList) corev1.Container {
		return corev1.Container{Name: name, Resources: corev1.ResourceRequirements{Requests: requests}}
	}
	sidecar := func(name string, requests corev1.ResourceList) corev1.Container {
		c := container(name, requests)
		c.RestartPolicy = new(corev1.ContainerRestartPolicyAlways)
		return c
	}
	cpu := func(v string) corev1.ResourceList {
		return corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(v)}
	}
	cpuMem := func(c, m string) corev1.ResourceList {
		return corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(c), corev1.ResourceMemory: resource.MustParse(m)}
	}

	tests := []struct {
		name string
		pod  *corev1.Pod
		want corev1.ResourceList
	}{
		{
			name: "nil pod has no requests",
			pod:  nil,
			want: corev1.ResourceList{},
		},
		{
			name: "sidecar listed after an init container does not add to its peak",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				InitContainers: []corev1.Container{container("init", cpuMem("4", "1Gi")), sidecar("sidecar", cpuMem("1", "2Gi"))},
				Containers:     []corev1.Container{container("main", cpuMem("1", "1Gi"))},
			}},
			want: cpuMem("4", "3Gi"),
		},
		{
			name: "sidecar listed before an init container adds to its peak",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				InitContainers: []corev1.Container{sidecar("sidecar", cpu("1")), container("init", cpu("4"))},
				Containers:     []corev1.Container{container("main", cpu("1"))},
			}},
			want: cpu("5"),
		},
		{
			name: "sidecar adds to the regular containers",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				InitContainers: []corev1.Container{sidecar("sidecar", cpu("2"))},
				Containers:     []corev1.Container{container("main", cpu("1"))},
			}},
			want: cpu("3"),
		},
		{
			name: "pod overhead is added",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				Containers: []corev1.Container{container("main", cpu("1"))},
				Overhead:   cpu("250m"),
			}},
			want: cpu("1250m"),
		},
		{
			name: "zero overhead entry adds no resource",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				Containers: []corev1.Container{container("main", cpu("1"))},
				Overhead:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("0")},
			}},
			want: cpu("1"),
		},
		{
			name: "pod-level request sets its resource",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				Containers: []corev1.Container{container("main", cpuMem("1", "1Gi"))},
				Resources:  &corev1.ResourceRequirements{Requests: cpu("4")},
			}},
			want: cpuMem("4", "1Gi"),
		},
	}

	asStrings := func(l corev1.ResourceList) map[corev1.ResourceName]string {
		out := make(map[corev1.ResourceName]string, len(l))
		for rName, qty := range l {
			out[rName] = qty.String()
		}
		return out
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, want := asStrings(ComputePodResourceRequest(tc.pod)), asStrings(tc.want)
			if !maps.Equal(got, want) {
				t.Errorf("ComputePodResourceRequest() = %v, want %v", got, want)
			}
		})
	}
}

func TestInfraScrapeAndPodWatchEvents(t *testing.T) {
	schedTime := metav1.NewTime(time.Date(2026, 10, 30, 10, 0, 0, 0, time.UTC))
	rawPodName := testSecretNamePrefix + "train-pod-0"

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "kwok-node-0",
			Labels: map[string]string{
				"instance":                          "gpu",
				"kubernetes.io/hostname":            "kwok-node-0",
				"cloud.google.com/gke-accelerator":  "nvidia-tesla-a100",
				"cloud.google.com/gke-cluster-name": testSecretValue,
			},
			Annotations: map[string]string{
				"kwok.x-k8s.io/node":                   "fake",
				"container.googleapis.com/instance_id": testSecretValue,
				"csi.volume.kubernetes.io/nodeid":      testSecretValue,
				"alpha.kubernetes.io/provided-node-ip": testSecretIP,
				corev1.LastAppliedConfigAnnotation:     testSecretValue,
			},
		},
		Spec: corev1.NodeSpec{
			PodCIDR:       testSecretCIDR,
			PodCIDRs:      []string{testSecretCIDR},
			ProviderID:    "gce://" + testSecretValue + "/us-central1-a/kwok-node-0",
			Unschedulable: false,
			Taints: []corev1.Taint{
				{Key: "kwok.x-k8s.io/node", Value: "fake", Effect: corev1.TaintEffectNoSchedule},
			},
		},
		Status: corev1.NodeStatus{
			Phase: corev1.NodeRunning,
			Capacity: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("8"),
				corev1.ResourceMemory: resource.MustParse("32Gi"),
			},
			Allocatable: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("8"),
				corev1.ResourceMemory: resource.MustParse("32Gi"),
			},
			Conditions: []corev1.NodeCondition{
				{Type: corev1.NodeReady, Status: corev1.ConditionTrue, Reason: "KubeletReady"},
			},
			Images: []corev1.ContainerImage{
				{Names: []string{testSecretImage}, SizeBytes: 1024},
			},
			Addresses: []corev1.NodeAddress{
				{Type: corev1.NodeInternalIP, Address: testSecretIP},
				{Type: corev1.NodeInternalDNS, Address: testSecretValue},
			},
			NodeInfo: corev1.NodeSystemInfo{
				MachineID:  testSecretValue,
				SystemUUID: testSecretValue,
				BootID:     testSecretValue,
			},
			VolumesInUse: []corev1.UniqueVolumeName{corev1.UniqueVolumeName(testSecretValue)},
			VolumesAttached: []corev1.AttachedVolume{
				{Name: corev1.UniqueVolumeName(testSecretValue), DevicePath: "/dev/sdb"},
			},
		},
	}
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "ai-lab",
			Labels: map[string]string{"team": testSecretValue, "environment": "prod"},
			Annotations: map[string]string{
				"corp.internal/owner":              testSecretEmail,
				"k8s.io/ns-note":                   "keep-ns-annot",
				corev1.LastAppliedConfigAnnotation: testSecretValue,
			},
		},
		Status: corev1.NamespaceStatus{Phase: corev1.NamespaceActive},
	}
	runningPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rawPodName,
			Namespace: "ai-lab",
			Labels:    map[string]string{"secret-label": testSecretValue},
			Annotations: map[string]string{
				"secret-annotation": testSecretValue,
			},
		},
		Spec: corev1.PodSpec{
			NodeName:         "kwok-node-0",
			ImagePullSecrets: []corev1.LocalObjectReference{{Name: testSecretValue}},
			Volumes:          []corev1.Volume{{Name: testSecretValue}},
			Containers: []corev1.Container{
				{
					Name:    "trainer",
					Image:   testSecretImage,
					Command: []string{"python", testSecretValue},
					Args:    []string{testSecretValue},
					Env:     []corev1.EnvVar{{Name: "API_KEY", Value: testSecretValue}},
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse("4"),
							corev1.ResourceMemory: resource.MustParse("16Gi"),
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{
					Type:               corev1.PodScheduled,
					Status:             corev1.ConditionTrue,
					LastTransitionTime: schedTime,
				},
			},
		},
	}

	cq := &kueuev1beta2.ClusterQueue{
		ObjectMeta: metav1.ObjectMeta{
			Name: "ai-lab-cq",
			Annotations: map[string]string{
				"finance.corp.com/cost-center": testSecretValue,
				"kueue.x-k8s.io/cq-note":       "keep-cq-annot",
			},
		},
		Spec: kueuev1beta2.ClusterQueueSpec{
			CohortName: "main-cohort",
		},
		Status: kueuev1beta2.ClusterQueueStatus{
			AdmittedWorkloads: 5,
		},
	}
	cohort := &kueuev1beta2.Cohort{
		ObjectMeta: metav1.ObjectMeta{
			Name: "main-cohort",
			Labels: map[string]string{
				"team": testSecretValue,
				"tier": "shared",
			},
			Annotations: map[string]string{
				"finance.corp.com/cost-center":     testSecretValue,
				corev1.LastAppliedConfigAnnotation: testSecretValue,
				"kueue.x-k8s.io/cohort-note":       "keep-cohort-annot",
			},
		},
		Spec: kueuev1beta2.CohortSpec{
			ParentName: "root-cohort",
			FairSharing: &kueuev1beta2.FairSharing{
				Weight: ptr.To(resource.MustParse("2")),
			},
			ResourceGroups: []kueuev1beta2.ResourceGroup{
				{
					CoveredResources: []corev1.ResourceName{corev1.ResourceCPU},
					Flavors: []kueuev1beta2.FlavorQuotas{
						{
							Name: "default-flavor",
							Resources: []kueuev1beta2.ResourceQuota{
								{
									Name:           corev1.ResourceCPU,
									NominalQuota:   resource.MustParse("16"),
									BorrowingLimit: ptr.To(resource.MustParse("8")),
									LendingLimit:   ptr.To(resource.MustParse("4")),
								},
							},
						},
					},
				},
			},
		},
		Status: kueuev1beta2.CohortStatus{
			FairSharing: &kueuev1beta2.FairSharingStatus{
				WeightedShare: 100,
			},
		},
	}
	lq := &kueuev1beta2.LocalQueue{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ai-lab-lq",
			Namespace: "ai-lab",
			Annotations: map[string]string{
				"corp.internal/owner": testSecretEmail,
			},
		},
		Spec: kueuev1beta2.LocalQueueSpec{
			ClusterQueue: "ai-lab-cq",
		},
		Status: kueuev1beta2.LocalQueueStatus{
			AdmittedWorkloads: 5,
		},
	}
	rf := &kueuev1beta2.ResourceFlavor{
		ObjectMeta: metav1.ObjectMeta{
			Name: "default-flavor",
			Annotations: map[string]string{
				"k8s.client-domain.com/owner": testSecretEmail,
			},
		},
		Spec: kueuev1beta2.ResourceFlavorSpec{
			NodeLabels: map[string]string{"instance": "gpu"},
		},
	}
	topo := &kueuev1beta2.Topology{
		ObjectMeta: metav1.ObjectMeta{
			Name: "default-topology",
			Annotations: map[string]string{
				"corp.internal/doc": testSecretValue,
			},
		},
		Spec: kueuev1beta2.TopologySpec{
			Levels: []kueuev1beta2.TopologyLevel{{NodeLabel: "kubernetes.io/hostname"}},
		},
	}
	wpc := &kueuev1beta2.WorkloadPriorityClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "high-priority",
			Labels: map[string]string{"team": testSecretValue},
			Annotations: map[string]string{
				"corp.internal/owner": testSecretEmail,
				"kueue.x-k8s.io/note": "keep-wpc-annot",
			},
		},
		Value:       1000,
		Description: testSecretValue,
	}
	runtimeClass := &nodev1.RuntimeClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "gvisor",
			Labels: map[string]string{"team": testSecretValue},
			Annotations: map[string]string{
				"corp.internal/owner": testSecretEmail,
				"k8s.io/rc-note":      "keep-rc-annot",
			},
		},
		Handler: "runsc",
		Overhead: &nodev1.Overhead{
			PodFixed: corev1.ResourceList{
				corev1.ResourceCPU: resource.MustParse("250m"),
			},
		},
		Scheduling: &nodev1.Scheduling{
			NodeSelector: map[string]string{
				"instance":                          "gpu",
				"team":                              testSecretValue,
				"cloud.google.com/gke-cluster-name": testSecretValue,
			},
		},
	}

	kubeClient := kubefake.NewSimpleClientset(node, ns, runtimeClass, runningPod)
	kueueClient := kueuefake.NewSimpleClientset(cq, cohort, lq, rf, topo, wpc)
	scraper := newTestScraper(t, Config{HashNames: true}, kubeClient, kueueClient, nil)

	ctx := context.Background()
	scraper.ScrapeOnce(ctx)

	// Simulate watch deletion of the running pod.
	scraper.handlePodWatchEvent(watch.Deleted, runningPod)

	rawInfra, err := os.ReadFile(scraper.cfg.InfraFile)
	if err != nil {
		t.Fatalf("read infra.json: %v", err)
	}
	events := readJSONLines[map[string]any](t, rawInfra)

	if len(events) != 11 {
		t.Fatalf("expected 11 infra events (9 infra objects + 1 pod new + 1 pod deleted), got %d", len(events))
	}

	seenKinds := make(map[string]int)
	for _, ev := range events {
		if len(ev) != 3 {
			t.Errorf("expected InfraEvent to have strictly 3 top-level keys (timestamp, eventType, payload), got %v", ev)
		}
		evType := ev["eventType"].(string)
		if evType == "" || evType[0] < 'A' || evType[0] > 'Z' {
			t.Errorf("expected eventType to start with uppercase letter, got %q", evType)
		}
		payload := ev["payload"].(map[string]any)
		kind := payload["kind"].(string)
		seenKinds[kind]++

		switch kind {
		case "Node":
			meta := payload["metadata"].(map[string]any)
			name := meta["name"].(string)
			if name != "kwok-node-0" {
				t.Errorf("Node name must not be hashed, got %q", name)
			}
			status, hasStatus := payload["status"].(map[string]any)
			if !hasStatus {
				t.Errorf("Node payload must include status")
			} else {
				if _, hasNodeInfo := status["nodeInfo"]; hasNodeInfo {
					t.Errorf("Node payload status must NOT include zero-valued nodeInfo, got %v", status["nodeInfo"])
				}
				if _, hasDaemonEndpoints := status["daemonEndpoints"]; hasDaemonEndpoints {
					t.Errorf("Node payload status must NOT include zero-valued daemonEndpoints, got %v", status["daemonEndpoints"])
				}
			}
		case "Namespace", "RuntimeClass", "ClusterQueue", "Cohort", "LocalQueue", "ResourceFlavor", "Topology", "WorkloadPriorityClass":
			meta := payload["metadata"].(map[string]any)
			name := meta["name"].(string)
			if _, hasStatus := payload["status"]; hasStatus {
				t.Errorf("%s payload must NOT include status", kind)
			}
			if strings.HasPrefix(name, "h-") && len(name) == 34 {
				t.Errorf("%s name must not be hashed, got %q", kind, name)
			}
			if kind == "WorkloadPriorityClass" {
				if gotVal, _ := payload["value"].(float64); gotVal != 1000 {
					t.Errorf("WorkloadPriorityClass expected value=1000, got %v", payload["value"])
				}
				if _, hasDesc := payload["description"]; hasDesc {
					t.Errorf("WorkloadPriorityClass must strip free-form description, got %v", payload["description"])
				}
			}
			if kind == "RuntimeClass" {
				if payload["handler"] != "runsc" {
					t.Errorf("RuntimeClass expected handler=runsc, got %v", payload["handler"])
				}
			}
		case "Pod":
			name := payload["name"].(string)
			expectedHash := scraper.hasher.HashName(rawPodName)
			if name != expectedHash {
				t.Errorf("Pod name should be HMAC-SHA256 hashed to %q, got %q", expectedHash, name)
			}
			if payload["placement"] != "kwok-node-0" {
				t.Errorf("Pod placement must be kwok-node-0 (unhashed), got %v", payload["placement"])
			}
			// Ensure NO forbidden fields exist in Pod payload.
			for _, forbidden := range []string{"spec", "status", "containers", "volumes", "secrets", "annotations", "labels", "command", "args", "env"} {
				if _, exists := payload[forbidden]; exists {
					t.Errorf("Pod payload must not contain %q", forbidden)
				}
			}
			if evType == string(EventDeleted) && payload["deletionTime"] == nil {
				t.Errorf("Deleted Pod event must record deletionTime")
			}
		}
	}

	// Ensure none of the predefined sensitive sentinel constants leaked anywhere into infra.json.
	infraStr := string(rawInfra)
	for _, secret := range testForbiddenSubstrings {
		if strings.Contains(infraStr, secret) {
			t.Errorf("infra.json leaked sensitive information %q:\n%s", secret, infraStr)
		}
	}

	// Ensure scheduling-relevant Node/Namespace/Cohort/RuntimeClass/WorkloadPriorityClass fields and allowed annotations WERE preserved in infra.json.
	for _, expected := range []string{
		"kwok.x-k8s.io/node",
		"nvidia-tesla-a100",
		"KubeletReady",
		"keep-ns-annot",
		"keep-cq-annot",
		"keep-cohort-annot",
		"root-cohort",
		"keep-wpc-annot",
		"keep-rc-annot",
		"high-priority",
		"gvisor",
		"runsc",
		`"environment":"prod"`,
		`"team":"` + scraper.hasher.HashName(testSecretValue) + `"`,
	} {
		if !strings.Contains(infraStr, expected) {
			t.Errorf("infra.json missing expected sanitized field/value %q:\n%s", expected, infraStr)
		}
	}
	for _, k := range []string{"Node", "Namespace", "RuntimeClass", "ClusterQueue", "Cohort", "LocalQueue", "ResourceFlavor", "Topology", "WorkloadPriorityClass"} {
		if seenKinds[k] != 1 {
			t.Errorf("expected 1 %s event in infra.json, got %d", k, seenKinds[k])
		}
	}
	if strings.Contains(infraStr, `"nodeInfo"`) || strings.Contains(infraStr, `"daemonEndpoints"`) {
		t.Errorf("infra.json must not contain zero-valued nodeInfo or daemonEndpoints:\n%s", infraStr)
	}
}

func TestWorkflowsSanitizationAndRecording(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = leaderworkersetv1.AddToScheme(scheme)
	_ = rayv1.AddToScheme(scheme)
	dynClient := dynamicfake.NewSimpleDynamicClient(scheme)

	scraper := newTestScraper(t, Config{
		HashNames:       true,
		ExtraHashLabels: []string{"corp.io/custom-hashed"},
	}, nil, nil, dynClient)

	createTime := metav1.NewTime(time.Date(2026, 10, 30, 10, 5, 0, 0, time.UTC))

	secretPodTemplate := corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				"kueue-optimizer.gke.io/pod-execution-time-seconds": "20",
				"corp.internal/secret-note":                         testSecretValue,
			},
			Labels: map[string]string{
				KueueQueueLabel:         "team-a-lq",
				"owner":                 testSecretEmail,
				"team":                  testSecretValue,
				"customer":              testSecretValue,
				"corp.io/custom-hashed": testSecretValue,
			},
		},
		Spec: corev1.PodSpec{
			ServiceAccountName:       testSecretValue,
			DeprecatedServiceAccount: testSecretValue,
			Hostname:                 testSecretValue,
			HostnameOverride:         ptr.To(testSecretValue),
			Subdomain:                testSecretValue,
			Volumes:                  []corev1.Volume{{Name: testSecretValue}},
			ImagePullSecrets:         []corev1.LocalObjectReference{{Name: testSecretValue}},
			Containers: []corev1.Container{
				{
					Name:         "worker",
					Image:        testSecretImage,
					Command:      []string{testSecretValue},
					Args:         []string{testSecretValue},
					Env:          []corev1.EnvVar{{Name: "API_KEY", Value: testSecretValue}},
					EnvFrom:      []corev1.EnvFromSource{{Prefix: testSecretValue}},
					VolumeMounts: []corev1.VolumeMount{{Name: testSecretValue, MountPath: "/mnt"}},
					LivenessProbe: &corev1.Probe{
						ProbeHandler: corev1.ProbeHandler{
							Exec: &corev1.ExecAction{Command: []string{testSecretValue}},
						},
					},
					ReadinessProbe: &corev1.Probe{
						ProbeHandler: corev1.ProbeHandler{
							HTTPGet: &corev1.HTTPGetAction{
								HTTPHeaders: []corev1.HTTPHeader{{Name: "Authorization", Value: testSecretValue}},
							},
						},
					},
					StartupProbe: &corev1.Probe{
						ProbeHandler: corev1.ProbeHandler{
							Exec: &corev1.ExecAction{Command: []string{testSecretValue}},
						},
					},
					Lifecycle: &corev1.Lifecycle{
						PostStart: &corev1.LifecycleHandler{
							Exec: &corev1.ExecAction{Command: []string{testSecretValue}},
						},
						PreStop: &corev1.LifecycleHandler{
							Exec: &corev1.ExecAction{Command: []string{testSecretValue}},
						},
					},
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU: resource.MustParse("2"),
						},
					},
				},
			},
		},
	}

	// 1. Job
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:              testSecretNamePrefix + "batch-job",
			Namespace:         "ai-lab",
			CreationTimestamp: createTime,
			Labels:            map[string]string{KueueQueueLabel: "team-a-lq"},
			Annotations: map[string]string{
				"business.org/project":             testSecretValue,
				"k8s.client-domain.com/owner":      testSecretEmail,
				"kubernetes.io/change-cause":       "kubectl apply -f " + testSecretValue + " --record",
				corev1.LastAppliedConfigAnnotation: `{"apiVersion":"batch/v1","kind":"Job","metadata":{"annotations":{"k8s.client-domain.com/owner":"` + testSecretEmail + `"}},"spec":{"template":{"spec":{"containers":[{"image":"` + testSecretImage + `","env":[{"name":"API_SECRET","value":"` + testSecretValue + `"}]}]}}}}`,
				"k8s.io/keep-me":                   "true",
			},
		},
		Spec: batchv1.JobSpec{
			Template: secretPodTemplate,
		},
	}
	scraper.handleJobWatchEvent(watch.Added, job)

	// 2. JobSet
	js := &jobsetv1alpha2.JobSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:              testSecretNamePrefix + "jobset",
			Namespace:         "ai-lab",
			CreationTimestamp: createTime,
			Labels:            map[string]string{KueueQueueLabel: "team-a-lq"},
			Annotations: map[string]string{
				"business.org/owner":                  testSecretEmail,
				"kueue-optimizer.gke.io/pod-count":    "4",
				"kueue.x-k8s.io/workload-slice-name":  testSecretNamePrefix + "jobset",
				KueueWorkloadSliceReplacementForAnnot: testSecretNamePrefix + "old-jobset",
			},
		},
		Spec: jobsetv1alpha2.JobSetSpec{
			VolumeClaimPolicies: []jobsetv1alpha2.VolumeClaimPolicy{{}},
			Network: &jobsetv1alpha2.Network{
				EnableDNSHostnames: ptr.To(true),
				Subdomain:          testSecretNamePrefix + "custom-subdomain",
			},
			ReplicatedJobs: []jobsetv1alpha2.ReplicatedJob{
				{
					Name:     "workers",
					Replicas: 1,
					Template: batchv1.JobTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{
								"business.corp/custom": testSecretValue,
								"kubernetes.io/info":   "keep-me",
							},
						},
						Spec: batchv1.JobSpec{
							Template: secretPodTemplate,
						},
					},
				},
			},
		},
	}
	scraper.handleJobSetWatchEvent(watch.Added, js)

	// 3. LeaderWorkerSet
	lws := &leaderworkersetv1.LeaderWorkerSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:              testSecretNamePrefix + "lws",
			Namespace:         "ai-lab",
			CreationTimestamp: createTime,
			Labels:            map[string]string{KueueQueueLabel: "team-a-lq"},
		},
		Spec: leaderworkersetv1.LeaderWorkerSetSpec{
			Replicas: ptr.To[int32](2),
			LeaderWorkerTemplate: leaderworkersetv1.LeaderWorkerTemplate{
				LeaderTemplate:       secretPodTemplate.DeepCopy(),
				WorkerTemplate:       secretPodTemplate,
				VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: testSecretValue}}},
			},
		},
	}
	scraper.handleLeaderWorkerSetWatchEvent(watch.Added, lws)

	// 4. Deployment
	deployTpl := secretPodTemplate
	deployTpl.Labels = map[string]string{
		KueueQueueLabel:          "team-a-lq",
		"app":                    testSecretNamePrefix + "deployment",
		"app.kubernetes.io/name": testSecretNamePrefix + "deployment",
		"owner":                  testSecretEmail,
		"corp.io/custom-hashed":  testSecretValue,
	}
	deployTpl.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{
		{
			MaxSkew:           1,
			TopologyKey:       "kubernetes.io/hostname",
			WhenUnsatisfiable: corev1.DoNotSchedule,
			LabelSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app":   testSecretNamePrefix + "deployment",
					"owner": testSecretEmail,
				},
			},
		},
	}
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:              testSecretNamePrefix + "deployment",
			Namespace:         "ai-lab",
			CreationTimestamp: createTime,
			Labels: map[string]string{
				KueueQueueLabel:          "team-a-lq",
				"app":                    testSecretNamePrefix + "deployment",
				"app.kubernetes.io/name": testSecretNamePrefix + "deployment",
				"customer":               testSecretValue,
			},
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app":   testSecretNamePrefix + "deployment",
					"owner": testSecretEmail,
				},
			},
			Template: deployTpl,
		},
	}
	scraper.handleDeploymentWatchEvent(watch.Added, deploy)

	// 5. RayJob
	rj := &rayv1.RayJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:              testSecretNamePrefix + "rayjob",
			Namespace:         "ai-lab",
			CreationTimestamp: createTime,
			Labels:            map[string]string{KueueQueueLabel: "team-a-lq"},
		},
		Spec: rayv1.RayJobSpec{
			Entrypoint:           testSecretValue,
			RuntimeEnvYAML:       testSecretValue,
			JobId:                testSecretNamePrefix + "custom-job-id",
			Metadata:             map[string]string{"owner": testSecretEmail},
			ClusterSelector:      map[string]string{"ray.io/cluster": testSecretNamePrefix + "raycluster", "owner": testSecretEmail},
			SubmitterPodTemplate: secretPodTemplate.DeepCopy(),
			RayClusterSpec: &rayv1.RayClusterSpec{
				AutoscalerOptions: &rayv1.AutoscalerOptions{
					Image:        ptr.To(testSecretImage),
					Command:      []string{testSecretValue},
					Args:         []string{testSecretValue},
					Env:          []corev1.EnvVar{{Name: "TOKEN", Value: testSecretValue}},
					EnvFrom:      []corev1.EnvFromSource{{Prefix: testSecretValue}},
					VolumeMounts: []corev1.VolumeMount{{Name: testSecretValue, MountPath: "/mnt"}},
					Resources: &corev1.ResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m")},
					},
				},
				HistoryServerOptions: &rayv1.HistoryServerOptions{
					CollectorOptions: &rayv1.CollectorOptions{
						Image: ptr.To(testSecretImage),
						Env:   []corev1.EnvVar{{Name: "COLLECTOR_KEY", Value: testSecretValue}},
					},
				},
				GcsFaultToleranceOptions: &rayv1.GcsFaultToleranceOptions{
					RedisAddress:             testSecretIP + ":6379",
					ExternalStorageNamespace: testSecretValue,
					RedisUsername:            &rayv1.RedisCredential{Value: testSecretValue},
					RedisPassword:            &rayv1.RedisCredential{Value: testSecretValue},
				},
				AuthOptions: &rayv1.AuthOptions{
					Mode:       rayv1.AuthModeToken,
					SecretName: ptr.To(testSecretValue),
				},
				HeadGroupSpec: rayv1.HeadGroupSpec{
					Labels:        map[string]string{"owner": testSecretEmail, "corp.io/custom-hashed": testSecretValue},
					EnableIngress: ptr.To(true),
					IngressOptions: &rayv1.IngressOptions{
						Host: ptr.To(testSecretValue),
						Path: ptr.To("/" + testSecretValue),
						TLS:  []networkingv1.IngressTLS{{Hosts: []string{testSecretValue}, SecretName: testSecretValue}},
					},
					HeadService: &corev1.Service{
						ObjectMeta: metav1.ObjectMeta{Name: testSecretNamePrefix + "ray-head-svc"},
						Spec: corev1.ServiceSpec{
							Selector:                 map[string]string{"ray.io/cluster": testSecretNamePrefix + "raycluster", "owner": testSecretEmail},
							ClusterIP:                testSecretIP,
							ClusterIPs:               []string{testSecretIP},
							ExternalIPs:              []string{testSecretIP},
							LoadBalancerIP:           testSecretIP,
							LoadBalancerSourceRanges: []string{testSecretCIDR},
							ExternalName:             testSecretValue,
						},
					},
					RayStartParams: map[string]string{
						testSecretValue:  "1",
						"redis-password": testSecretValue,
					},
					Template: secretPodTemplate,
				},
			},
		},
	}
	scraper.handleRayJobWatchEvent(watch.Added, rj)

	// 6. RayCluster (including headless HeadService with ClusterIP="None" preserved)
	rc := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:              testSecretNamePrefix + "raycluster",
			Namespace:         "ai-lab",
			CreationTimestamp: createTime,
			Labels:            map[string]string{KueueQueueLabel: "team-a-lq"},
		},
		Spec: rayv1.RayClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{
				Labels:         map[string]string{"owner": testSecretEmail},
				RayStartParams: map[string]string{"redis-password": testSecretValue},
				HeadService: &corev1.Service{
					ObjectMeta: metav1.ObjectMeta{Name: testSecretNamePrefix + "headless-svc"},
					Spec: corev1.ServiceSpec{
						ClusterIP:  corev1.ClusterIPNone,
						ClusterIPs: []string{corev1.ClusterIPNone},
					},
				},
				Template: secretPodTemplate,
			},
			WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
				{
					GroupName:      "gpu-group",
					Labels:         map[string]string{"team": testSecretValue},
					RayStartParams: map[string]string{"token": testSecretValue},
					Template:       secretPodTemplate,
				},
			},
		},
	}
	scraper.handleRayClusterWatchEvent(watch.Added, rc)

	// 7. Standalone Kueue-managed Pod (should be recorded) vs Job-owned Pod (should NOT be recorded in workflows.json)
	standalonePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              testSecretNamePrefix + "standalone-kueue-pod",
			Namespace:         "ai-lab",
			CreationTimestamp: createTime,
			Labels:            map[string]string{KueueQueueLabel: "team-a-lq"},
			Annotations:       secretPodTemplate.Annotations,
		},
		Spec: secretPodTemplate.Spec,
	}
	scraper.handlePodWatchEvent(watch.Added, standalonePod)

	jobOwnedPod := standalonePod.DeepCopy()
	jobOwnedPod.Name = testSecretNamePrefix + "job-owned-pod"
	jobOwnedPod.OwnerReferences = []metav1.OwnerReference{
		{APIVersion: "batch/v1", Kind: "Job", Name: testSecretNamePrefix + "batch-job"},
	}
	scraper.handlePodWatchEvent(watch.Added, jobOwnedPod)

	// Also verify unstructured conversion for Dynamic watchers (LWS, RayJob, RayCluster).
	uMap, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(lws)
	scraper.handleLeaderWorkerSetUnstructured(watch.Modified, &unstructured.Unstructured{Object: uMap})

	rawWorkflows, err := os.ReadFile(scraper.cfg.WorkflowsFile)
	if err != nil {
		t.Fatalf("read workflows.json: %v", err)
	}
	wfEvents := readJSONLines[WorkflowEvent](t, rawWorkflows)

	if len(wfEvents) != 7 {
		t.Fatalf("expected 7 workflow events (Job, JobSet, LWS, Deployment, RayJob, RayCluster, standalone Pod), got %d", len(wfEvents))
	}

	for _, ev := range wfEvents {
		if ev.EventType == "" || ev.EventType[0] < 'A' || ev.EventType[0] > 'Z' {
			t.Errorf("expected eventType to start with uppercase letter, got %q", ev.EventType)
		}
		if ev.WorkloadState != WorkloadStateAdmitted && ev.WorkloadState != WorkloadStateAdmissionInProgress && ev.WorkloadState != WorkloadStateNotAdmitted {
			t.Errorf("%s expected valid workloadState, got %q", ev.Payload.ObjectKind, ev.WorkloadState)
		}
		if ev.Payload.LocalQueueName != "team-a-lq" {
			t.Errorf("%s expected localQueueName 'team-a-lq', got %q", ev.Payload.ObjectKind, ev.Payload.LocalQueueName)
		}
		if ev.Payload.CreationTime.IsZero() {
			t.Errorf("%s expected non-zero creationTime", ev.Payload.ObjectKind)
		}
		if len(ev.Payload.Name) != 34 || !strings.HasPrefix(ev.Payload.Name, "h-") {
			t.Errorf("%s expected 34-char h-<32-hex> HMAC-SHA256 hashed name, got %q", ev.Payload.ObjectKind, ev.Payload.Name)
		}
	}

	wfStr := string(rawWorkflows)
	// Verify none of the predefined sensitive sentinel constants or raw names leaked into workflows.json.
	for _, secret := range testForbiddenSubstrings {
		if strings.Contains(wfStr, secret) {
			t.Errorf("workflows.json leaked sensitive information %q:\n%s", secret, wfStr)
		}
	}

	// Verify pause image and k8s/kubernetes/k8s-x annotations WERE preserved.
	for _, expected := range []string{
		DefaultPauseImage,
		"kueue-optimizer.gke.io/pod-execution-time-seconds",
		"k8s.io/keep-me",
		"kubernetes.io/info",
		"team-a-lq",
		`"clusterIP":"None"`,
	} {
		if !strings.Contains(wfStr, expected) {
			t.Errorf("workflows.json missing expected sanitized value %q:\n%s", expected, wfStr)
		}
	}
}

func TestWorkloadExecutionTimeTracking(t *testing.T) {
	scraper := newTestScraper(t, Config{HashNames: true}, nil, nil, nil)

	t0 := time.Date(2026, 10, 30, 10, 0, 0, 0, time.UTC)
	tAdmit1 := t0.Add(5 * time.Second)
	tPreempt := t0.Add(20 * time.Second) // 15s partial execution
	tAdmit2 := t0.Add(30 * time.Second)
	tFinish := t0.Add(75 * time.Second) // 45s full execution

	wl := &kueuev1beta2.Workload{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "jobset-train-01-wl",
			Namespace:         "ai-lab",
			CreationTimestamp: metav1.NewTime(t0),
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "jobset.x-k8s.io/v1alpha2",
					Kind:       "JobSet",
					Name:       "jobset-train-01",
					Controller: ptr.To(true),
				},
			},
		},
		Spec: kueuev1beta2.WorkloadSpec{
			QueueName: "ai-lab-lq",
		},
		Status: kueuev1beta2.WorkloadStatus{
			Admission: &kueuev1beta2.Admission{
				ClusterQueue: "ai-lab-cq",
			},
			Conditions: []metav1.Condition{
				{
					Type:               kueuev1beta2.WorkloadAdmitted,
					Status:             metav1.ConditionTrue,
					Reason:             "Admitted",
					LastTransitionTime: metav1.NewTime(tAdmit1),
				},
			},
		},
	}

	// 1. Workload gets admitted.
	scraper.handleWorkloadWatchEvent(watch.Added, wl)

	// 2. Workload gets preempted after 15s.
	wlPreempted := wl.DeepCopy()
	wlPreempted.Status.Conditions = []metav1.Condition{
		{
			Type:               kueuev1beta2.WorkloadAdmitted,
			Status:             metav1.ConditionFalse,
			Reason:             "NoReservation",
			LastTransitionTime: metav1.NewTime(tPreempt),
		},
		{
			Type:               kueuev1beta2.WorkloadPreempted,
			Status:             metav1.ConditionTrue,
			Reason:             kueuev1beta2.InClusterQueueReason,
			Message:            "Preempted by higher-priority workload",
			LastTransitionTime: metav1.NewTime(tPreempt),
		},
	}
	scraper.handleWorkloadWatchEvent(watch.Modified, wlPreempted)

	// 3. Workload gets re-admitted at tAdmit2.
	wlReadmitted := wl.DeepCopy()
	wlReadmitted.Status.Conditions = []metav1.Condition{
		{
			Type:               kueuev1beta2.WorkloadAdmitted,
			Status:             metav1.ConditionTrue,
			Reason:             "Admitted",
			LastTransitionTime: metav1.NewTime(tAdmit2),
		},
	}
	scraper.handleWorkloadWatchEvent(watch.Modified, wlReadmitted)

	// 4. Workload finishes without preemption at tFinish (45s execution).
	wlFinished := wlReadmitted.DeepCopy()
	wlFinished.Status.Conditions = append(wlFinished.Status.Conditions, metav1.Condition{
		Type:               kueuev1beta2.WorkloadFinished,
		Status:             metav1.ConditionTrue,
		Reason:             kueuev1beta2.WorkloadFinishedReasonSucceeded,
		Message:            "JobSet completed successfully",
		LastTransitionTime: metav1.NewTime(tFinish),
	})
	scraper.handleWorkloadWatchEvent(watch.Modified, wlFinished)

	// 5. Subsequent metadata/finalizer update on finished workload must NOT re-emit Admitted.
	scraper.handleWorkloadWatchEvent(watch.Modified, wlFinished)

	rawExec, err := os.ReadFile(scraper.cfg.ExecutionTimeFile)
	if err != nil {
		t.Fatalf("read execution-time.json: %v", err)
	}
	execEvents := readJSONLines[ExecutionTimeEvent](t, rawExec)

	if len(execEvents) != 4 {
		t.Fatalf("expected 4 execution-time events (Admitted, PartialExecution, Admitted, FullExecution), got %d", len(execEvents))
	}

	if execEvents[0].EventType != ExecutionEventAdmitted || execEvents[0].WorkloadState != WorkloadStateAdmitted {
		t.Errorf("event[0] expected Admitted with workloadState=Admitted, got %+v", execEvents[0])
	}
	if execEvents[1].EventType != ExecutionEventPartial || execEvents[1].WorkloadState != WorkloadStateNotAdmitted || execEvents[1].Payload.ExecutionType != "Partial" || !execEvents[1].Payload.Preempted {
		t.Errorf("event[1] expected PartialExecution with workloadState=NotAdmitted and preempted=true, got %+v", execEvents[1])
	}
	if execEvents[1].Payload.ExecutionDurationSeconds == nil || *execEvents[1].Payload.ExecutionDurationSeconds != 15.0 {
		t.Errorf("event[1] expected 15s partial execution duration, got %v", execEvents[1].Payload.ExecutionDurationSeconds)
	}
	if execEvents[2].EventType != ExecutionEventAdmitted || execEvents[2].WorkloadState != WorkloadStateAdmitted {
		t.Errorf("event[2] expected second Admitted event with workloadState=Admitted, got %+v", execEvents[2])
	}
	if execEvents[3].EventType != ExecutionEventFull || execEvents[3].WorkloadState != WorkloadStateAdmitted || execEvents[3].Payload.ExecutionType != "Full" || execEvents[3].Payload.Preempted {
		t.Errorf("event[3] expected FullExecution with workloadState=Admitted and preempted=false, got %+v", execEvents[3])
	}
	if execEvents[3].Payload.ExecutionDurationSeconds == nil || *execEvents[3].Payload.ExecutionDurationSeconds != 45.0 {
		t.Errorf("event[3] expected 45s full execution duration, got %v", execEvents[3].Payload.ExecutionDurationSeconds)
	}

	// Verify workload and owner names are hashed, while namespace and queue names are unhashed.
	for _, ev := range execEvents {
		if ev.EventType == "" || ev.EventType[0] < 'A' || ev.EventType[0] > 'Z' {
			t.Errorf("expected eventType to start with uppercase letter, got %q", ev.EventType)
		}
		if ev.Payload.WorkloadName != scraper.hasher.HashName("jobset-train-01-wl") {
			t.Errorf("expected hashed workloadName, got %q", ev.Payload.WorkloadName)
		}
		if ev.Payload.OwnerName != scraper.hasher.HashName("jobset-train-01") {
			t.Errorf("expected hashed ownerName, got %q", ev.Payload.OwnerName)
		}
		if ev.Payload.Namespace != "ai-lab" || ev.Payload.LocalQueueName != "ai-lab-lq" || ev.Payload.ClusterQueueName != "ai-lab-cq" {
			t.Errorf("expected unhashed namespace/queues, got ns=%q lq=%q cq=%q", ev.Payload.Namespace, ev.Payload.LocalQueueName, ev.Payload.ClusterQueueName)
		}
	}

	// Verify workflow state progression: NotAdmitted -> AdmissionInProgress -> Admitted.
	jsSuspended := &jobsetv1alpha2.JobSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "jobset-state-test",
			Namespace:         "ai-lab",
			CreationTimestamp: metav1.NewTime(t0),
			Labels:            map[string]string{KueueQueueLabel: "ai-lab-lq"},
		},
		Spec: jobsetv1alpha2.JobSetSpec{
			Suspend: ptr.To(true),
		},
	}
	scraper.handleJobSetWatchEvent(watch.Added, jsSuspended)

	jsUnsuspended := jsSuspended.DeepCopy()
	jsUnsuspended.Spec.Suspend = ptr.To(false)
	scraper.handleJobSetWatchEvent(watch.Modified, jsUnsuspended)

	childJobSuspended := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "jobset-state-test-workers-0",
			Namespace:         "ai-lab",
			CreationTimestamp: metav1.NewTime(t0),
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: "jobset.x-k8s.io/v1alpha2", Kind: "JobSet", Name: "jobset-state-test"},
			},
		},
		Spec: batchv1.JobSpec{
			Suspend: ptr.To(true),
		},
	}
	scraper.handleJobWatchEvent(watch.Added, childJobSuspended)

	childJobUnsuspended := childJobSuspended.DeepCopy()
	childJobUnsuspended.Spec.Suspend = ptr.To(false)
	scraper.handleJobWatchEvent(watch.Modified, childJobUnsuspended)

	rawWf, err := os.ReadFile(scraper.cfg.WorkflowsFile)
	if err != nil {
		t.Fatalf("read workflows.json: %v", err)
	}
	wfEvents := readJSONLines[WorkflowEvent](t, rawWf)
	if len(wfEvents) != 4 {
		t.Fatalf("expected 4 workflow events, got %d", len(wfEvents))
	}
	expectedStates := []WorkloadState{
		WorkloadStateNotAdmitted,
		WorkloadStateAdmitted,
		WorkloadStateAdmissionInProgress,
		WorkloadStateAdmitted,
	}
	for i, want := range expectedStates {
		if wfEvents[i].WorkloadState != want {
			t.Errorf("wfEvents[%d] expected workloadState=%q, got %q", i, want, wfEvents[i].WorkloadState)
		}
	}
}

func TestParseFlags(t *testing.T) {
	var errBuf bytes.Buffer
	cfg, qps, burst, err := parseFlags("cluster-scraper", []string{
		"--output-dir", "./traces/run-basic-dev",
		"--kubeconfig", "/tmp/test-kubeconfig",
		"-n", "15",
		"--duration", "5m",
		"--sha256-names",
		"--extra-hash-labels", "Corp.IO/Custom-Hashed, billing-code",
		"--extra-hash-labels", "extra-tag",
		"--qps", "150",
		"--burst", "300",
	}, &errBuf)
	if err != nil {
		t.Fatalf("parseFlags returned unexpected error: %v", err)
	}
	if !slices.Equal(cfg.ExtraHashLabels, []string{"corp.io/custom-hashed", "billing-code", "extra-tag"}) {
		t.Errorf("unexpected ExtraHashLabels: %v", cfg.ExtraHashLabels)
	}
	if cfg.Kubeconfig != "/tmp/test-kubeconfig" {
		t.Errorf("expected Kubeconfig=/tmp/test-kubeconfig, got %q", cfg.Kubeconfig)
	}
	if cfg.Interval != 15*time.Second {
		t.Errorf("expected Interval=15s, got %s", cfg.Interval)
	}
	if cfg.Duration != 5*time.Minute {
		t.Errorf("expected Duration=5m, got %s", cfg.Duration)
	}
	if !cfg.HashNames {
		t.Errorf("expected HashNames=true when --sha256-names is set")
	}
	if cfg.InfraFile != filepath.Join("traces", "run-basic-dev", "infra.json") {
		t.Errorf("unexpected InfraFile: %q", cfg.InfraFile)
	}
	if cfg.WorkflowsFile != filepath.Join("traces", "run-basic-dev", "workflows.json") {
		t.Errorf("unexpected WorkflowsFile: %q", cfg.WorkflowsFile)
	}
	if cfg.ExecutionTimeFile != filepath.Join("traces", "run-basic-dev", "execution-time.json") {
		t.Errorf("unexpected ExecutionTimeFile: %q", cfg.ExecutionTimeFile)
	}
	if qps != 150.0 || burst != 300 {
		t.Errorf("expected qps=150, burst=300; got qps=%v, burst=%d", qps, burst)
	}

	// Verify --extra-hash-labels without --hash-names returns an error.
	errBuf.Reset()
	_, _, _, err = parseFlags("cluster-scraper", []string{"--extra-hash-labels", "corp.io/team"}, &errBuf)
	if err == nil {
		t.Errorf("expected error when --extra-hash-labels is set without --hash-names")
	}

	// Verify --help returns flag.ErrHelp cleanly without panicking.
	errBuf.Reset()
	_, _, _, err = parseFlags("cluster-scraper", []string{"--help"}, &errBuf)
	if !errors.Is(err, flag.ErrHelp) {
		t.Errorf("expected flag.ErrHelp on --help, got %v", err)
	}

	// Verify invalid interval returns an error and writes message to errOut.
	errBuf.Reset()
	_, _, _, err = parseFlags("cluster-scraper", []string{"--interval", "-5s"}, &errBuf)
	if err == nil {
		t.Errorf("expected error for negative interval, got nil")
	}
}

func TestScraperUsesNoopHasherWhenHashNamesDisabled(t *testing.T) {
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "ai-lab",
			Labels: map[string]string{"team": "research-alpha"},
		},
	}
	scraper := newTestScraper(t, Config{HashNames: false}, kubefake.NewSimpleClientset(ns), nil, nil)
	scraper.ScrapeOnce(context.Background())

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "plain-job-name",
			Namespace: "ai-lab",
			Labels: map[string]string{
				KueueQueueLabel: "team-a-lq",
				"team":          "research-alpha",
				"app":           "plain-job-name",
			},
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "main", Image: testSecretImage}},
				},
			},
		},
	}
	scraper.handleJobWatchEvent(watch.Added, job)

	rawInfra, _ := os.ReadFile(scraper.cfg.InfraFile)
	if !strings.Contains(string(rawInfra), `"team":"research-alpha"`) {
		t.Errorf("expected plain label value in infra.json when HashNames=false, got:\n%s", string(rawInfra))
	}
	rawWf, _ := os.ReadFile(scraper.cfg.WorkflowsFile)
	for _, want := range []string{`"name":"plain-job-name"`, `"team":"research-alpha"`, `"app":"plain-job-name"`} {
		if !strings.Contains(string(rawWf), want) {
			t.Errorf("expected %s in workflows.json when HashNames=false, got:\n%s", want, string(rawWf))
		}
	}
	if strings.Contains(string(rawWf), testSecretImage) {
		t.Errorf("container image must still be replaced with pause image even when HashNames=false")
	}
}

func TestCanonicalDigestIgnoresResourceVersion(t *testing.T) {
	nodeV1 := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "kwok-node-0",
			ResourceVersion: "100",
			Labels:          map[string]string{"role": "worker"},
		},
		Spec: corev1.NodeSpec{PodCIDR: "10.0.0.0/24"},
	}
	nodeV2 := nodeV1.DeepCopy()
	nodeV2.ResourceVersion = "205"

	hasher := NewSensitiveDataHasher(nil, nil)
	payloadV1 := SanitizeNode(nodeV1, hasher)
	payloadV2 := SanitizeNode(nodeV2, hasher)

	digestV1 := canonicalDigest(payloadV1)
	digestV2 := canonicalDigest(payloadV2)
	if digestV1 == "" {
		t.Fatalf("expected non-empty digest")
	}
	if digestV1 != digestV2 {
		t.Errorf("expected identical digest when only metadata.resourceVersion changes: got %q vs %q", digestV1, digestV2)
	}

	// Verify that changing a non-resourceVersion field changes the digest.
	nodeV3 := nodeV2.DeepCopy()
	nodeV3.Labels["role"] = "gpu-worker"
	digestV3 := canonicalDigest(SanitizeNode(nodeV3, hasher))
	if digestV3 == digestV1 {
		t.Errorf("expected digest to change when metadata.labels changes")
	}
}

func TestCohortScrapeLifecycle(t *testing.T) {
	const (
		initialCohortWeight = "2"
		updatedCohortWeight = "5"
	)

	cohort := &kueuev1beta2.Cohort{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "main-cohort",
			ResourceVersion: "1",
			ManagedFields: []metav1.ManagedFieldsEntry{
				{Manager: "kueue-controller", Operation: metav1.ManagedFieldsOperationApply},
			},
			Labels: map[string]string{
				"team":             testSecretValue,
				"corp.io/org-unit": testSecretValue,
				"tier":             "shared",
			},
			Annotations: map[string]string{
				"corp.internal/owner":              testSecretEmail,
				corev1.LastAppliedConfigAnnotation: testSecretValue,
				"kueue.x-k8s.io/cohort-note":       "keep-cohort-annot",
			},
		},
		Spec: kueuev1beta2.CohortSpec{
			ParentName: "root-cohort",
			FairSharing: &kueuev1beta2.FairSharing{
				Weight: ptr.To(resource.MustParse(initialCohortWeight)),
			},
			ResourceGroups: []kueuev1beta2.ResourceGroup{
				{
					CoveredResources: []corev1.ResourceName{corev1.ResourceCPU},
					Flavors: []kueuev1beta2.FlavorQuotas{
						{
							Name: "default-flavor",
							Resources: []kueuev1beta2.ResourceQuota{
								{
									Name:           corev1.ResourceCPU,
									NominalQuota:   resource.MustParse("32"),
									BorrowingLimit: ptr.To(resource.MustParse("16")),
									LendingLimit:   ptr.To(resource.MustParse("8")),
								},
							},
						},
					},
				},
			},
		},
		Status: kueuev1beta2.CohortStatus{
			FairSharing: &kueuev1beta2.FairSharingStatus{
				WeightedShare: 50,
			},
		},
	}

	kueueClient := kueuefake.NewSimpleClientset(cohort)
	scraper := newTestScraper(t, Config{
		HashNames:       true,
		ExtraHashLabels: []string{"corp.io/org-unit"},
	}, nil, kueueClient, nil)

	if got := SanitizeCohort(nil, scraper.hasher); got != nil {
		t.Errorf("expected SanitizeCohort(nil, hasher) == nil, got %v", got)
	}

	ctx := context.Background()

	// 1. Initial scrape emits "New" for Cohort.
	scraper.ScrapeOnce(ctx)

	// 2. Second scrape with only ResourceVersion and Status modified must NOT emit an Update.
	modifiedMeta := cohort.DeepCopy()
	modifiedMeta.ResourceVersion = "2"
	modifiedMeta.Status.FairSharing.WeightedShare = 99
	if _, err := kueueClient.KueueV1beta2().Cohorts().Update(ctx, modifiedMeta, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update cohort status/resourceVersion: %v", err)
	}
	scraper.ScrapeOnce(ctx)

	// 3. Updating Spec.FairSharing.Weight emits "Update".
	modifiedSpec := modifiedMeta.DeepCopy()
	modifiedSpec.Spec.FairSharing.Weight = ptr.To(resource.MustParse(updatedCohortWeight))
	if _, err := kueueClient.KueueV1beta2().Cohorts().Update(ctx, modifiedSpec, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update cohort spec: %v", err)
	}
	scraper.ScrapeOnce(ctx)

	// 4. Deleting the Cohort emits "Deleted".
	if err := kueueClient.KueueV1beta2().Cohorts().Delete(ctx, cohort.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete cohort: %v", err)
	}
	scraper.ScrapeOnce(ctx)

	rawInfra, err := os.ReadFile(scraper.cfg.InfraFile)
	if err != nil {
		t.Fatalf("read infra.json: %v", err)
	}
	events := readJSONLines[InfraEvent](t, rawInfra)
	if len(events) != 3 {
		t.Fatalf("expected 3 Cohort lifecycle events (New, Update, Deleted), got %d:\n%s", len(events), string(rawInfra))
	}

	wantEventTypes := []EventType{EventNew, EventUpdate, EventDeleted}
	wantWeights := []string{initialCohortWeight, updatedCohortWeight, updatedCohortWeight}
	for i, wantType := range wantEventTypes {
		if events[i].EventType != wantType {
			t.Errorf("events[%d].EventType = %q, want %q", i, events[i].EventType, wantType)
		}
		payloadMap, ok := events[i].Payload.(map[string]any)
		if !ok {
			t.Fatalf("events[%d].Payload is not a map", i)
		}
		if payloadMap["apiVersion"] != kueuev1beta2.SchemeGroupVersion.String() || payloadMap["kind"] != "Cohort" {
			t.Errorf("events[%d] unexpected apiVersion/kind: %v / %v", i, payloadMap["apiVersion"], payloadMap["kind"])
		}
		if _, hasStatus := payloadMap["status"]; hasStatus {
			t.Errorf("events[%d] Cohort payload must omit status", i)
		}
		meta := payloadMap["metadata"].(map[string]any)
		if meta["name"] != "main-cohort" {
			t.Errorf("events[%d] Cohort name must remain plain 'main-cohort', got %v", i, meta["name"])
		}
		if _, hasManagedFields := meta["managedFields"]; hasManagedFields {
			t.Errorf("events[%d] Cohort metadata must omit managedFields", i)
		}
		spec := payloadMap["spec"].(map[string]any)
		fairSharing := spec["fairSharing"].(map[string]any)
		if fairSharing["weight"] != wantWeights[i] {
			t.Errorf("events[%d] Cohort spec.fairSharing.weight = %v, want %q", i, fairSharing["weight"], wantWeights[i])
		}
	}

	// Verify hashed label values, preserved spec fields, and zero secret leaks.
	infraStr := string(rawInfra)
	for _, secret := range testForbiddenSubstrings {
		if strings.Contains(infraStr, secret) {
			t.Errorf("infra.json leaked sensitive information %q:\n%s", secret, infraStr)
		}
	}
	wantSecretHash := scraper.hasher.HashName(testSecretValue)
	for _, expected := range []string{
		`"team":"` + wantSecretHash + `"`,
		`"corp.io/org-unit":"` + wantSecretHash + `"`,
		`"tier":"shared"`,
		`"kueue.x-k8s.io/cohort-note":"keep-cohort-annot"`,
		`"parentName":"root-cohort"`,
		`"nominalQuota":"32"`,
		`"borrowingLimit":"16"`,
		`"lendingLimit":"8"`,
		`"weight":"` + initialCohortWeight + `"`,
		`"weight":"` + updatedCohortWeight + `"`,
	} {
		if !strings.Contains(infraStr, expected) {
			t.Errorf("infra.json missing expected Cohort content %q:\n%s", expected, infraStr)
		}
	}
}

func TestCohortScrapeMissingCRD(t *testing.T) {
	cqOnlyClient := kueuefake.NewSimpleClientset(&kueuev1beta2.ClusterQueue{
		ObjectMeta: metav1.ObjectMeta{Name: "ai-lab-cq"},
	})
	cqOnlyClient.PrependReactor("list", "cohorts", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewNotFound(schema.GroupResource{Group: "kueue.x-k8s.io", Resource: "cohorts"}, "")
	})
	scraper := newTestScraper(t, Config{}, nil, cqOnlyClient, nil)
	scraper.ScrapeOnce(context.Background())

	rawMissing, err := os.ReadFile(scraper.cfg.InfraFile)
	if err != nil {
		t.Fatalf("read missing-CRD infra.json: %v", err)
	}
	missingEvents := readJSONLines[InfraEvent](t, rawMissing)
	if len(missingEvents) != 1 {
		t.Fatalf("expected 1 ClusterQueue event when Cohort CRD is absent, got %d", len(missingEvents))
	}
	cqPayload := missingEvents[0].Payload.(map[string]any)
	if cqPayload["kind"] != "ClusterQueue" {
		t.Errorf("expected ClusterQueue event, got %v", cqPayload["kind"])
	}
}

func TestSuppressStaleAdmittedDuringActiveEvictionAndPreemption(t *testing.T) {
	scraper := newTestScraper(t, Config{}, nil, nil, nil)

	t0 := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	tAdmit1 := t0.Add(5 * time.Second)
	tPreempt := t0.Add(20 * time.Second)
	tAdmit2 := t0.Add(35 * time.Second)

	wl := &kueuev1beta2.Workload{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "wl-stale-admit",
			Namespace:         "ai-lab",
			CreationTimestamp: metav1.NewTime(t0),
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "jobset.x-k8s.io/v1alpha2",
					Kind:       "JobSet",
					Name:       "jobset-stale-admit",
					Controller: ptr.To(true),
				},
			},
		},
		Spec: kueuev1beta2.WorkloadSpec{QueueName: "ai-lab-lq"},
		Status: kueuev1beta2.WorkloadStatus{
			Admission: &kueuev1beta2.Admission{ClusterQueue: "ai-lab-cq"},
			Conditions: []metav1.Condition{
				{
					Type:               kueuev1beta2.WorkloadAdmitted,
					Status:             metav1.ConditionTrue,
					Reason:             "Admitted",
					LastTransitionTime: metav1.NewTime(tAdmit1),
				},
			},
		},
	}

	// 1. Initial admission at tAdmit1.
	scraper.handleWorkloadWatchEvent(watch.Added, wl)

	// 2. Preemption starts at tPreempt while Admitted=True (tAdmit1) is STILL latched during two-phase eviction.
	wlEvicting := wl.DeepCopy()
	wlEvicting.Status.Conditions = []metav1.Condition{
		{
			Type:               kueuev1beta2.WorkloadAdmitted,
			Status:             metav1.ConditionTrue,
			Reason:             "Admitted",
			LastTransitionTime: metav1.NewTime(tAdmit1),
		},
		{
			Type:               kueuev1beta2.WorkloadEvicted,
			Status:             metav1.ConditionTrue,
			Reason:             kueuev1beta2.WorkloadEvictedByPreemption,
			Message:            "Preempted to accommodate higher-priority workload",
			LastTransitionTime: metav1.NewTime(tPreempt),
		},
		{
			Type:               kueuev1beta2.WorkloadPreempted,
			Status:             metav1.ConditionTrue,
			Reason:             kueuev1beta2.InClusterQueueReason,
			Message:            "Preempted in ClusterQueue",
			LastTransitionTime: metav1.NewTime(tPreempt),
		},
	}
	scraper.handleWorkloadWatchEvent(watch.Modified, wlEvicting)

	// 3. Additional pod/condition updates arrive while eviction is STILL active and Admitted=True (tAdmit1) remains latched.
	wlEvictingUpdate := wlEvicting.DeepCopy()
	wlEvictingUpdate.Status.Conditions = append(wlEvictingUpdate.Status.Conditions, metav1.Condition{
		Type:               kueuev1beta2.WorkloadPodsReady,
		Status:             metav1.ConditionFalse,
		Reason:             "PodsTerminating",
		LastTransitionTime: metav1.NewTime(tPreempt.Add(1 * time.Second)),
	})
	scraper.handleWorkloadWatchEvent(watch.Modified, wlEvictingUpdate)
	if !scraper.workloadStates["ai-lab/wl-stale-admit"].currentlyAdmitted {
		t.Errorf("expected currentlyAdmitted to stay consistent with Kueue Admitted=True while eviction is in flight")
	}

	// 4. Eviction completes and Kueue flips Admitted=False -> currentlyAdmitted becomes false.
	wlEvictedDone := wlEvicting.DeepCopy()
	wlEvictedDone.Status.Conditions[0].Status = metav1.ConditionFalse
	wlEvictedDone.Status.Conditions[0].Reason = kueuev1beta2.WorkloadAdmittedReasonNoReservation
	wlEvictedDone.Status.Conditions[0].LastTransitionTime = metav1.NewTime(tPreempt.Add(2 * time.Second))
	scraper.handleWorkloadWatchEvent(watch.Modified, wlEvictedDone)
	if scraper.workloadStates["ai-lab/wl-stale-admit"].currentlyAdmitted {
		t.Errorf("expected currentlyAdmitted=false once Kueue sets Admitted=False")
	}

	// 5. Even if Preempted/Evicted are cleared before Admitted updates its LastTransitionTime away from tAdmit1,
	// stale tAdmit1 must NOT re-emit Admitted.
	wlStaleAdmittedTimestamp := wl.DeepCopy()
	wlStaleAdmittedTimestamp.Status.Conditions = []metav1.Condition{
		{
			Type:               kueuev1beta2.WorkloadAdmitted,
			Status:             metav1.ConditionTrue,
			Reason:             "Admitted",
			LastTransitionTime: metav1.NewTime(tAdmit1),
		},
	}
	scraper.handleWorkloadWatchEvent(watch.Modified, wlStaleAdmittedTimestamp)

	// 6. Genuine re-admission at tAdmit2 emits a fresh Admitted event.
	wlReadmitted := wl.DeepCopy()
	wlReadmitted.Status.Conditions = []metav1.Condition{
		{
			Type:               kueuev1beta2.WorkloadAdmitted,
			Status:             metav1.ConditionTrue,
			Reason:             "Admitted",
			LastTransitionTime: metav1.NewTime(tAdmit2),
		},
	}
	scraper.handleWorkloadWatchEvent(watch.Modified, wlReadmitted)

	// 7. Non-preemption eviction (e.g. PodsReadyTimeout) while Admitted=True (tAdmit2) is still latched must NOT emit
	// duplicate Admitted, and must record a deduplicated PartialExecution with interruptionReason=PodsReadyTimeout.
	wlNonPreemptEvict := wlReadmitted.DeepCopy()
	wlNonPreemptEvict.Status.Conditions = append(wlNonPreemptEvict.Status.Conditions, metav1.Condition{
		Type:               kueuev1beta2.WorkloadEvicted,
		Status:             metav1.ConditionTrue,
		Reason:             kueuev1beta2.WorkloadEvictedByPodsReadyTimeout,
		LastTransitionTime: metav1.NewTime(tAdmit2.Add(10 * time.Second)),
	})
	scraper.handleWorkloadWatchEvent(watch.Modified, wlNonPreemptEvict)
	scraper.handleWorkloadWatchEvent(watch.Modified, wlNonPreemptEvict)

	rawExec, err := os.ReadFile(scraper.cfg.ExecutionTimeFile)
	if err != nil {
		t.Fatalf("read execution-time.json: %v", err)
	}
	execEvents := readJSONLines[ExecutionTimeEvent](t, rawExec)
	if len(execEvents) != 4 {
		t.Fatalf("expected strictly 4 events (Admitted@tAdmit1, PartialExecution@tPreempt, Admitted@tAdmit2, PartialExecution@PodsReadyTimeout), got %d:\n%s", len(execEvents), string(rawExec))
	}
	if execEvents[0].EventType != ExecutionEventAdmitted || !execEvents[0].Payload.AdmittedTime.Time.Equal(tAdmit1) {
		t.Errorf("event[0] expected Admitted at %v, got %+v", tAdmit1, execEvents[0])
	}
	if execEvents[1].EventType != ExecutionEventPartial || *execEvents[1].Payload.ExecutionDurationSeconds != 15.0 ||
		execEvents[1].Payload.InterruptionReason != InterruptionReasonPreempted || !execEvents[1].Payload.Preempted {
		t.Errorf("event[1] expected PartialExecution (15s, Preempted), got %+v", execEvents[1])
	}
	if execEvents[2].EventType != ExecutionEventAdmitted || !execEvents[2].Payload.AdmittedTime.Time.Equal(tAdmit2) || execEvents[2].Payload.PreemptionCount != 1 {
		t.Errorf("event[2] expected Admitted at %v with preemptionCount=1, got %+v", tAdmit2, execEvents[2])
	}
	if execEvents[3].EventType != ExecutionEventPartial || *execEvents[3].Payload.ExecutionDurationSeconds != 10.0 ||
		execEvents[3].Payload.InterruptionReason != InterruptionReasonPodsReadyTimeout || execEvents[3].Payload.Preempted {
		t.Errorf("event[3] expected PartialExecution (10s, PodsReadyTimeout, preempted=false), got %+v", execEvents[3])
	}
}

func TestFullExecutionWhenCompletionWinsPreemptionRace(t *testing.T) {
	t0 := time.Date(2026, 11, 1, 13, 0, 0, 0, time.UTC)
	tAdmit := t0.Add(5 * time.Second)
	tPreempt := t0.Add(24 * time.Second)
	tFinish := t0.Add(25 * time.Second)

	ownerRef := []metav1.OwnerReference{
		{
			APIVersion: "jobset.x-k8s.io/v1alpha2",
			Kind:       "JobSet",
			Name:       "jobset-preempt-race",
			Controller: ptr.To(true),
		},
	}

	wlAdmitted := &kueuev1beta2.Workload{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "wl-preempt-race",
			Namespace:         "ai-lab",
			CreationTimestamp: metav1.NewTime(t0),
			OwnerReferences:   ownerRef,
		},
		Spec: kueuev1beta2.WorkloadSpec{QueueName: "ai-lab-lq"},
		Status: kueuev1beta2.WorkloadStatus{
			Admission: &kueuev1beta2.Admission{ClusterQueue: "ai-lab-cq"},
			Conditions: []metav1.Condition{
				{
					Type:               kueuev1beta2.WorkloadAdmitted,
					Status:             metav1.ConditionTrue,
					Reason:             "Admitted",
					LastTransitionTime: metav1.NewTime(tAdmit),
				},
			},
		},
	}

	wlPreempting := wlAdmitted.DeepCopy()
	wlPreempting.Status.Conditions = append(wlPreempting.Status.Conditions, metav1.Condition{
		Type:               kueuev1beta2.WorkloadPreempted,
		Status:             metav1.ConditionTrue,
		Reason:             kueuev1beta2.InClusterQueueReason,
		LastTransitionTime: metav1.NewTime(tPreempt),
	})

	wlSucceededWithLatchedPreempted := wlPreempting.DeepCopy()
	wlSucceededWithLatchedPreempted.Status.Conditions = append(wlSucceededWithLatchedPreempted.Status.Conditions, metav1.Condition{
		Type:               kueuev1beta2.WorkloadFinished,
		Status:             metav1.ConditionTrue,
		Reason:             kueuev1beta2.WorkloadFinishedReasonSucceeded,
		Message:            "Job completed successfully before eviction",
		LastTransitionTime: metav1.NewTime(tFinish),
	})

	wlFailedPreempted := &kueuev1beta2.Workload{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "wl-preempt-failed",
			Namespace:         "ai-lab",
			CreationTimestamp: metav1.NewTime(t0),
			OwnerReferences:   ownerRef,
		},
		Spec: kueuev1beta2.WorkloadSpec{QueueName: "ai-lab-lq"},
		Status: kueuev1beta2.WorkloadStatus{
			Conditions: []metav1.Condition{
				{
					Type:               kueuev1beta2.WorkloadAdmitted,
					Status:             metav1.ConditionFalse,
					Reason:             kueuev1beta2.WorkloadAdmittedReasonNoReservation,
					LastTransitionTime: metav1.NewTime(tPreempt),
				},
				{
					Type:               kueuev1beta2.WorkloadPreempted,
					Status:             metav1.ConditionTrue,
					Reason:             kueuev1beta2.InClusterQueueReason,
					LastTransitionTime: metav1.NewTime(tPreempt),
				},
				{
					Type:               kueuev1beta2.WorkloadFinished,
					Status:             metav1.ConditionTrue,
					Reason:             kueuev1beta2.WorkloadFinishedReasonFailed,
					LastTransitionTime: metav1.NewTime(tFinish),
				},
			},
		},
	}

	wlUnowned := wlSucceededWithLatchedPreempted.DeepCopy()
	wlUnowned.Name = "wl-unowned"
	wlUnowned.OwnerReferences = nil

	type watchStep struct {
		eventType watch.EventType
		workload  *kueuev1beta2.Workload
	}
	type wantExec struct {
		eventType ExecutionEventType
		durSec    *float64
	}

	tests := []struct {
		name  string
		steps []watchStep
		want  []wantExec
	}{
		{
			name: "two-step race emits FullExecution after Preempted=True when Succeeded wins",
			steps: []watchStep{
				{watch.Added, wlAdmitted},
				{watch.Modified, wlPreempting},
				{watch.Modified, wlSucceededWithLatchedPreempted},
			},
			want: []wantExec{
				{ExecutionEventAdmitted, nil},
				{ExecutionEventPartial, ptr.To(19.0)},
				{ExecutionEventFull, ptr.To(20.0)},
			},
		},
		{
			name: "coalesced single-event race emits Admitted and FullExecution without PartialExecution",
			steps: []watchStep{
				{watch.Added, wlSucceededWithLatchedPreempted},
			},
			want: []wantExec{
				{ExecutionEventAdmitted, nil},
				{ExecutionEventFull, ptr.To(20.0)},
			},
		},
		{
			name: "preempted and failed workload emits PartialExecution without FullExecution",
			steps: []watchStep{
				{watch.Added, wlFailedPreempted},
			},
			want: []wantExec{
				{ExecutionEventPartial, ptr.To(24.0)},
			},
		},
		{
			name: "workload without owner is ignored",
			steps: []watchStep{
				{watch.Added, wlUnowned},
			},
			want: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			scraper := newTestScraper(t, Config{}, nil, nil, nil)
			for _, step := range tc.steps {
				scraper.handleWorkloadWatchEvent(step.eventType, step.workload)
			}

			rawExec, err := os.ReadFile(scraper.cfg.ExecutionTimeFile)
			if err != nil {
				t.Fatalf("read execution-time.json: %v", err)
			}
			got := readJSONLines[ExecutionTimeEvent](t, rawExec)
			if len(got) != len(tc.want) {
				t.Fatalf("expected %d events, got %d:\n%s", len(tc.want), len(got), string(rawExec))
			}
			for i, want := range tc.want {
				if got[i].EventType != want.eventType {
					t.Errorf("event[%d].EventType = %q, want %q", i, got[i].EventType, want.eventType)
				}
				if want.durSec != nil {
					if got[i].Payload.ExecutionDurationSeconds == nil || *got[i].Payload.ExecutionDurationSeconds != *want.durSec {
						t.Errorf("event[%d].ExecutionDurationSeconds = %v, want %v", i, got[i].Payload.ExecutionDurationSeconds, *want.durSec)
					}
				}
			}
		})
	}
}

func TestSubSecondPartialExecutionDoesNotUseCumulativePastSeconds(t *testing.T) {
	scraper := newTestScraper(t, Config{}, nil, nil, nil)

	t0 := time.Date(2026, 11, 1, 14, 0, 0, 0, time.UTC)
	tAdmit1 := t0.Add(5 * time.Second)
	tPreempt1 := t0.Add(25 * time.Second) // 20s first run
	tAdmit2 := t0.Add(40 * time.Second)
	tPreempt2 := t0.Add(40 * time.Second) // sub-second second run in same RFC3339 second

	wl := &kueuev1beta2.Workload{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "wl-subsec-preempt",
			Namespace:         "ai-lab",
			CreationTimestamp: metav1.NewTime(t0),
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "jobset.x-k8s.io/v1alpha2",
					Kind:       "JobSet",
					Name:       "jobset-subsec-preempt",
					Controller: ptr.To(true),
				},
			},
		},
		Spec: kueuev1beta2.WorkloadSpec{QueueName: "ai-lab-lq"},
		Status: kueuev1beta2.WorkloadStatus{
			Admission: &kueuev1beta2.Admission{ClusterQueue: "ai-lab-cq"},
			Conditions: []metav1.Condition{
				{
					Type:               kueuev1beta2.WorkloadAdmitted,
					Status:             metav1.ConditionTrue,
					Reason:             "Admitted",
					LastTransitionTime: metav1.NewTime(tAdmit1),
				},
			},
		},
	}

	// Cycle 1: Admitted at tAdmit1, preempted at tPreempt1 (20s).
	scraper.handleWorkloadWatchEvent(watch.Added, wl)

	wlPreempt1 := wl.DeepCopy()
	wlPreempt1.Status.AccumulatedPastExecutionTimeSeconds = ptr.To[int32](20)
	wlPreempt1.Status.Conditions = []metav1.Condition{
		{
			Type:               kueuev1beta2.WorkloadAdmitted,
			Status:             metav1.ConditionFalse,
			Reason:             kueuev1beta2.WorkloadAdmittedReasonNoReservation,
			LastTransitionTime: metav1.NewTime(tPreempt1),
		},
		{
			Type:               kueuev1beta2.WorkloadPreempted,
			Status:             metav1.ConditionTrue,
			Reason:             kueuev1beta2.InClusterQueueReason,
			LastTransitionTime: metav1.NewTime(tPreempt1),
		},
	}
	scraper.handleWorkloadWatchEvent(watch.Modified, wlPreempt1)

	// Cycle 2: Re-admitted at tAdmit2, then preempted within the same second (tPreempt2 == tAdmit2)
	// while cumulative AccumulatedPastExecutionTimeSeconds is still 20 from Cycle 1.
	wlAdmit2 := wl.DeepCopy()
	wlAdmit2.Status.AccumulatedPastExecutionTimeSeconds = ptr.To[int32](20)
	wlAdmit2.Status.Conditions = []metav1.Condition{
		{
			Type:               kueuev1beta2.WorkloadAdmitted,
			Status:             metav1.ConditionTrue,
			Reason:             "Admitted",
			LastTransitionTime: metav1.NewTime(tAdmit2),
		},
	}
	scraper.handleWorkloadWatchEvent(watch.Modified, wlAdmit2)

	wlPreempt2 := wlAdmit2.DeepCopy()
	wlPreempt2.Status.Conditions = []metav1.Condition{
		{
			Type:               kueuev1beta2.WorkloadAdmitted,
			Status:             metav1.ConditionFalse,
			Reason:             kueuev1beta2.WorkloadAdmittedReasonNoReservation,
			LastTransitionTime: metav1.NewTime(tPreempt2),
		},
		{
			Type:               kueuev1beta2.WorkloadPreempted,
			Status:             metav1.ConditionTrue,
			Reason:             kueuev1beta2.InClusterQueueReason,
			LastTransitionTime: metav1.NewTime(tPreempt2),
		},
	}
	scraper.handleWorkloadWatchEvent(watch.Modified, wlPreempt2)

	rawExec, err := os.ReadFile(scraper.cfg.ExecutionTimeFile)
	if err != nil {
		t.Fatalf("read execution-time.json: %v", err)
	}
	execEvents := readJSONLines[ExecutionTimeEvent](t, rawExec)
	if len(execEvents) != 4 {
		t.Fatalf("expected 4 execution-time events, got %d:\n%s", len(execEvents), string(rawExec))
	}
	if execEvents[1].EventType != ExecutionEventPartial || *execEvents[1].Payload.ExecutionDurationSeconds != 20.0 {
		t.Errorf("cycle 1 expected 20s PartialExecution, got %+v", execEvents[1].Payload)
	}
	if execEvents[3].EventType != ExecutionEventPartial || execEvents[3].Payload.ExecutionDurationSeconds == nil || *execEvents[3].Payload.ExecutionDurationSeconds != 0.0 || execEvents[3].Payload.ExecutionDuration != "0s" {
		t.Errorf("cycle 2 sub-second preemption expected 0s PartialExecution (must NOT substitute cumulative 20s), got %+v", execEvents[3].Payload)
	}
}

func TestHandleDeploymentWatchEventFiltersUnqueuedDeployments(t *testing.T) {
	mkDeploy := func(ns, name string, deployLabels, podLabels map[string]string) *appsv1.Deployment {
		tplLabels := map[string]string{"app": name}
		for k, v := range podLabels {
			tplLabels[k] = v
		}
		return &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:              name,
				Namespace:         ns,
				CreationTimestamp: metav1.NewTime(time.Date(2026, 11, 1, 15, 0, 0, 0, time.UTC)),
				Labels:            deployLabels,
			},
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": name}},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: tplLabels},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: "main", Image: testSecretImage}},
					},
				},
			},
		}
	}

	scraper := newTestScraper(t, Config{}, nil, nil, nil)

	// Unqueued Deployments must be skipped regardless of namespace (both system and user namespaces).
	scraper.handleDeploymentWatchEvent(watch.Added, mkDeploy("kube-system", "coredns", nil, nil))
	scraper.handleDeploymentWatchEvent(watch.Added, mkDeploy("gmp-system", "collector", nil, nil))
	scraper.handleDeploymentWatchEvent(watch.Added, mkDeploy("ai-lab", "unqueued-user-web", nil, nil))

	// Queued / Kueue-managed Deployments must be recorded regardless of namespace.
	queuedUser := mkDeploy("ai-lab", "serving-pool", map[string]string{KueueQueueLabel: "serving-lq"}, nil)
	queuedTemplate := mkDeploy("ai-lab", "template-queued", nil, map[string]string{KueueQueueLabel: "serving-lq"})
	managedUser := mkDeploy("ai-lab", "managed-pool", map[string]string{KueueManagedLabel: "true"}, nil)
	queuedSystem := mkDeploy("kueue-system", "queued-canary", map[string]string{KueueQueueLabel: "system-lq"}, nil)

	scraper.handleDeploymentWatchEvent(watch.Added, queuedUser)
	scraper.handleDeploymentWatchEvent(watch.Added, queuedTemplate)
	scraper.handleDeploymentWatchEvent(watch.Added, managedUser)
	scraper.handleDeploymentWatchEvent(watch.Added, queuedSystem)

	// Once tracked, a Deployment that later loses its queue label or is deleted still records lifecycle events.
	unlabeledTracked := queuedUser.DeepCopy()
	unlabeledTracked.Labels = nil
	scraper.handleDeploymentWatchEvent(watch.Deleted, unlabeledTracked)

	rawWf, err := os.ReadFile(scraper.cfg.WorkflowsFile)
	if err != nil {
		t.Fatalf("read workflows.json: %v", err)
	}
	wfEvents := readJSONLines[WorkflowEvent](t, rawWf)

	type recorded struct {
		eventType EventType
		key       string
	}
	var got []recorded
	for _, ev := range wfEvents {
		got = append(got, recorded{
			eventType: ev.EventType,
			key:       ev.Payload.Namespace + "/" + ev.Payload.Name,
		})
	}
	want := []recorded{
		{EventNew, "ai-lab/serving-pool"},
		{EventNew, "ai-lab/template-queued"},
		{EventNew, "ai-lab/managed-pool"},
		{EventNew, "kueue-system/queued-canary"},
		{EventDeleted, "ai-lab/serving-pool"},
	}
	if !slices.Equal(got, want) {
		t.Errorf("unexpected recorded Deployment events:\ngot:  %+v\nwant: %+v", got, want)
	}
}

func TestRunStartupBarrierLinksKueuePodsAndSeparatesInfraPods(t *testing.T) {
	t0 := metav1.NewTime(time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC))
	secretJSName := testSecretNamePrefix + "train-js"
	secretJobName := secretJSName + "-worker-0"
	secretWlName := "jobset-" + secretJSName + "-abcde"
	secretJSPodName := secretJobName + "-0-xyz"
	secretPGName := testSecretNamePrefix + "pg-group"
	secretPGPodName := secretPGName + "-0"
	secretDeployName := testSecretNamePrefix + "serving-deploy"
	secretRSName := secretDeployName + "-7f89c"
	secretDeployWl := "deployment-" + secretDeployName + "-11111"
	secretDeployPodName := secretRSName + "-p0"
	unmanagedSysPodName := "fluentbit-ds-node0"
	unmanagedJobName := testSecretNamePrefix + "cron-clean"
	unmanagedJobPodName := unmanagedJobName + "-p0"

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "tpu-node-0",
			Labels: map[string]string{
				"kubernetes.io/hostname": "tpu-node-0",
				"corp.io/rack":           testSecretValue,
			},
		},
		Status: corev1.NodeStatus{
			Phase:       corev1.NodeRunning,
			Capacity:    corev1.ResourceList{"google.com/tpu": resource.MustParse("8")},
			Allocatable: corev1.ResourceList{"google.com/tpu": resource.MustParse("8")},
		},
	}

	js := &jobsetv1alpha2.JobSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:              secretJSName,
			Namespace:         "ai-lab",
			CreationTimestamp: t0,
			Labels: map[string]string{
				KueueQueueLabel:         "tpu-lq",
				KueuePriorityClassLabel: "high-prio",
			},
		},
		Spec: jobsetv1alpha2.JobSetSpec{
			Suspend: ptr.To(false),
			ReplicatedJobs: []jobsetv1alpha2.ReplicatedJob{
				{
					Name:     "worker",
					Replicas: 1,
					Template: batchv1.JobTemplateSpec{
						Spec: batchv1.JobSpec{
							Parallelism: ptr.To[int32](1),
							Completions: ptr.To[int32](1),
							Template: corev1.PodTemplateSpec{
								ObjectMeta: metav1.ObjectMeta{
									Annotations: map[string]string{
										KueuePodSetRequiredTopologyAnnot: "corp.io/rack",
									},
								},
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{{
										Name:  "trainer",
										Image: testSecretImage,
										Resources: corev1.ResourceRequirements{
											Requests: corev1.ResourceList{"google.com/tpu": resource.MustParse("4")},
										},
									}},
								},
							},
						},
					},
				},
			},
		},
	}
	childJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:              secretJobName,
			Namespace:         "ai-lab",
			CreationTimestamp: t0,
			Labels:            map[string]string{"jobset.sigs.k8s.io/jobset-name": secretJSName},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "jobset.x-k8s.io/v1alpha2",
				Kind:       "JobSet",
				Name:       secretJSName,
				Controller: ptr.To(true),
			}},
		},
		Spec: batchv1.JobSpec{
			Suspend:  ptr.To(false),
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: testSecretImage}}}},
		},
	}
	jsWl := &kueuev1beta2.Workload{
		ObjectMeta: metav1.ObjectMeta{
			Name:              secretWlName,
			Namespace:         "ai-lab",
			CreationTimestamp: t0,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "jobset.x-k8s.io/v1alpha2",
				Kind:       "JobSet",
				Name:       secretJSName,
				Controller: ptr.To(true),
			}},
		},
		Spec: kueuev1beta2.WorkloadSpec{
			QueueName:        "tpu-lq",
			PriorityClassRef: kueuev1beta2.NewWorkloadPriorityClassRef("high-prio"),
			Priority:         ptr.To[int32](500),
			PodSets: []kueuev1beta2.PodSet{{
				Name:  "worker",
				Count: 1,
				TopologyRequest: &kueuev1beta2.PodSetTopologyRequest{
					Required: ptr.To("corp.io/rack"),
				},
			}},
		},
		Status: kueuev1beta2.WorkloadStatus{
			Admission: &kueuev1beta2.Admission{
				ClusterQueue: "tpu-cq",
				PodSetAssignments: []kueuev1beta2.PodSetAssignment{{
					Name:          "worker",
					Count:         ptr.To[int32](1),
					Flavors:       map[corev1.ResourceName]kueuev1beta2.ResourceFlavorReference{"google.com/tpu": "tpu-rf"},
					ResourceUsage: corev1.ResourceList{"google.com/tpu": resource.MustParse("4")},
					TopologyAssignment: &kueuev1beta2.TopologyAssignment{
						Levels: []string{"corp.io/rack", "kubernetes.io/hostname"},
						Slices: []kueuev1beta2.TopologyAssignmentSlice{{
							DomainCount: 1,
							ValuesPerLevel: []kueuev1beta2.TopologyAssignmentSliceLevelValues{
								{Universal: ptr.To(testSecretValue)},
								{Universal: ptr.To("tpu-node-0")},
							},
							PodCounts: kueuev1beta2.TopologyAssignmentSlicePodCounts{Universal: ptr.To[int32](1)},
						}},
					},
				}},
			},
			Conditions: []metav1.Condition{{
				Type:               kueuev1beta2.WorkloadAdmitted,
				Status:             metav1.ConditionTrue,
				Reason:             "Admitted",
				LastTransitionTime: t0,
			}},
		},
	}
	jsPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              secretJSPodName,
			Namespace:         "ai-lab",
			CreationTimestamp: t0,
			Labels: map[string]string{
				"batch.kubernetes.io/job-name":       secretJobName,
				"jobset.sigs.k8s.io/replicatedjob-name": "worker",
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "batch/v1",
				Kind:       "Job",
				Name:       secretJobName,
				Controller: ptr.To(true),
			}},
		},
		Spec: corev1.PodSpec{
			NodeName: "tpu-node-0",
			Containers: []corev1.Container{{
				Name:  "trainer",
				Image: testSecretImage,
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{"google.com/tpu": resource.MustParse("4")},
				},
			}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}

	pgWl := &kueuev1beta2.Workload{
		ObjectMeta: metav1.ObjectMeta{
			Name:              secretPGName,
			Namespace:         "ai-lab",
			CreationTimestamp: t0,
			Annotations:       map[string]string{KueueIsGroupWorkloadAnnotation: "true"},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "v1",
				Kind:       "Pod",
				Name:       secretPGPodName,
			}},
		},
		Spec: kueuev1beta2.WorkloadSpec{
			QueueName: "tpu-lq",
			PodSets:   []kueuev1beta2.PodSet{{Name: "abcd1234", Count: 1}},
		},
		Status: kueuev1beta2.WorkloadStatus{
			Admission: &kueuev1beta2.Admission{ClusterQueue: "tpu-cq"},
			Conditions: []metav1.Condition{{
				Type:               kueuev1beta2.WorkloadAdmitted,
				Status:             metav1.ConditionTrue,
				Reason:             "Admitted",
				LastTransitionTime: t0,
			}},
		},
	}
	pgPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              secretPGPodName,
			Namespace:         "ai-lab",
			CreationTimestamp: t0,
			Labels: map[string]string{
				KueueQueueLabel:        "tpu-lq",
				KueueManagedLabel:      "true",
				KueuePodGroupNameLabel: secretPGName,
			},
			Annotations: map[string]string{
				KueueRoleHashAnnotation: "abcd1234",
				KueueWorkloadAnnot:      secretPGName,
			},
		},
		Spec: corev1.PodSpec{
			NodeName: "tpu-node-0",
			Containers: []corev1.Container{{
				Name:      "w",
				Image:     testSecretImage,
				Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{"google.com/tpu": resource.MustParse("2")}},
			}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}

	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:              secretDeployName,
			Namespace:         "ai-lab",
			CreationTimestamp: t0,
			Labels:            map[string]string{KueueQueueLabel: "tpu-lq"},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To[int32](1),
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": secretDeployName}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": secretDeployName, KueueQueueLabel: "tpu-lq"}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "srv", Image: testSecretImage}}},
			},
		},
		Status: appsv1.DeploymentStatus{Replicas: 1, ReadyReplicas: 1},
	}
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretRSName,
			Namespace: "ai-lab",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       secretDeployName,
				Controller: ptr.To(true),
			}},
		},
	}
	deployWl := &kueuev1beta2.Workload{
		ObjectMeta: metav1.ObjectMeta{
			Name:              secretDeployWl,
			Namespace:         "ai-lab",
			CreationTimestamp: t0,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       secretDeployName,
				Controller: ptr.To(true),
			}},
		},
		Spec: kueuev1beta2.WorkloadSpec{
			QueueName: "tpu-lq",
			PodSets:   []kueuev1beta2.PodSet{{Name: "main", Count: 1}},
		},
		Status: kueuev1beta2.WorkloadStatus{
			Admission: &kueuev1beta2.Admission{ClusterQueue: "tpu-cq"},
			Conditions: []metav1.Condition{{
				Type:               kueuev1beta2.WorkloadAdmitted,
				Status:             metav1.ConditionTrue,
				Reason:             "Admitted",
				LastTransitionTime: t0,
			}},
		},
	}
	deployPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              secretDeployPodName,
			Namespace:         "ai-lab",
			CreationTimestamp: t0,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "ReplicaSet",
				Name:       secretRSName,
				Controller: ptr.To(true),
			}},
		},
		Spec: corev1.PodSpec{
			NodeName: "tpu-node-0",
			Containers: []corev1.Container{{
				Name:      "srv",
				Image:     testSecretImage,
				Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{"google.com/tpu": resource.MustParse("1")}},
			}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}

	unmanagedSysPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      unmanagedSysPodName,
			Namespace: "kube-system",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "DaemonSet",
				Name:       "fluentbit",
				Controller: ptr.To(true),
			}},
		},
		Spec: corev1.PodSpec{
			NodeName: "tpu-node-0",
			Containers: []corev1.Container{{
				Name:      "agent",
				Image:     testSecretImage,
				Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")}},
			}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	unmanagedJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: unmanagedJobName, Namespace: "ai-lab", CreationTimestamp: t0},
		Spec:       batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: testSecretImage}}}}},
	}
	unmanagedJobPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      unmanagedJobPodName,
			Namespace: "ai-lab",
			Labels:    map[string]string{"job-name": unmanagedJobName},
		},
		Spec: corev1.PodSpec{
			NodeName: "tpu-node-0",
			Containers: []corev1.Container{{
				Name:      "c",
				Image:     testSecretImage,
				Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{"google.com/tpu": resource.MustParse("1")}},
			}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}

	kubeClient := kubefake.NewSimpleClientset(node, childJob, unmanagedJob, deploy, rs, jsPod, pgPod, deployPod, unmanagedSysPod, unmanagedJobPod)
	kueueClient := kueuefake.NewSimpleClientset(jsWl, pgWl, deployWl)
	jobsetClient := jobsetfake.NewSimpleClientset(js)
	scraper := newTestScraper(t, Config{
		HashNames:       true,
		Interval:        10 * time.Hour,
		ExtraHashLabels: []string{"corp.io/*"},
	}, kubeClient, kueueClient, nil)
	scraper.jobsetClient = jobsetClient

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- scraper.Run(ctx)
	}()

	// Wait briefly for the synchronous startup barrier + initial ScrapeOnce to flush, then stop Run().
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if scraper.infraLog.Count() >= 6 && scraper.execTimeLog.Count() >= 3 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run() returned unexpected error: %v", err)
	}

	rawInfra, err := os.ReadFile(scraper.cfg.InfraFile)
	if err != nil {
		t.Fatalf("read infra.json: %v", err)
	}
	rawWf, err := os.ReadFile(scraper.cfg.WorkflowsFile)
	if err != nil {
		t.Fatalf("read workflows.json: %v", err)
	}
	rawExec, err := os.ReadFile(scraper.cfg.ExecutionTimeFile)
	if err != nil {
		t.Fatalf("read execution-time.json: %v", err)
	}

	for name, blob := range map[string]string{
		"infra.json":          string(rawInfra),
		"workflows.json":      string(rawWf),
		"execution-time.json": string(rawExec),
	} {
		for _, secret := range testForbiddenSubstrings {
			if strings.Contains(blob, secret) {
				t.Errorf("%s leaked sensitive substring %q:\n%s", name, secret, blob)
			}
		}
	}

	infraEvents := readJSONLines[map[string]any](t, rawInfra)
	podByHashedName := make(map[string]map[string]any)
	for _, ev := range infraEvents {
		pl := ev["payload"].(map[string]any)
		if pl["kind"] == "Pod" && ev["eventType"] == string(EventNew) {
			podByHashedName[pl["name"].(string)] = pl
		}
	}

	// 1. JobSet TAS pod must be linked to hashed JobSet Workload & JobSet owner with isTAS=true.
	gotJSPod := podByHashedName[scraper.hasher.HashName(secretJSPodName)]
	if gotJSPod == nil {
		t.Fatalf("missing JobSet pod in infra.json")
	}
	if gotJSPod["workloadName"] != scraper.hasher.HashName(secretWlName) ||
		gotJSPod["ownerKind"] != "JobSet" ||
		gotJSPod["ownerName"] != scraper.hasher.HashName(secretJSName) ||
		gotJSPod["podSetName"] != "worker" ||
		gotJSPod["isTAS"] != true {
		t.Errorf("unexpected JobSet pod linkage: %+v", gotJSPod)
	}

	// 2. PodGroup pod must be linked to hashed PodGroup Workload & PodGroup owner with role-hash podSetName.
	gotPGPod := podByHashedName[scraper.hasher.HashName(secretPGPodName)]
	if gotPGPod == nil {
		t.Fatalf("missing PodGroup pod in infra.json")
	}
	if gotPGPod["workloadName"] != scraper.hasher.HashName(secretPGName) ||
		gotPGPod["ownerKind"] != "PodGroup" ||
		gotPGPod["ownerName"] != scraper.hasher.HashName(secretPGName) ||
		gotPGPod["podSetName"] != "abcd1234" {
		t.Errorf("unexpected PodGroup pod linkage: %+v", gotPGPod)
	}

	// 3. Kueue-managed Deployment ReplicaSet pod must resolve through ReplicaSet -> Deployment -> Workload.
	gotDeployPod := podByHashedName[scraper.hasher.HashName(secretDeployPodName)]
	if gotDeployPod == nil {
		t.Fatalf("missing Deployment pod in infra.json")
	}
	if gotDeployPod["workloadName"] != scraper.hasher.HashName(secretDeployWl) ||
		gotDeployPod["ownerKind"] != "Deployment" ||
		gotDeployPod["ownerName"] != scraper.hasher.HashName(secretDeployName) ||
		gotDeployPod["podSetName"] != "main" {
		t.Errorf("unexpected Deployment pod linkage: %+v", gotDeployPod)
	}

	// 4. Non-Kueue DaemonSet and unmanaged Job pods must NOT carry workloadName/ownerName linkage.
	for label, pName := range map[string]string{
		"DaemonSet sys pod": unmanagedSysPodName,
		"unmanaged Job pod": unmanagedJobPodName,
	} {
		gotUnmanaged := podByHashedName[scraper.hasher.HashName(pName)]
		if gotUnmanaged == nil {
			t.Fatalf("missing %s in infra.json", label)
		}
		for _, k := range []string{"workloadName", "ownerKind", "ownerName", "podSetName", "isTAS"} {
			if _, hasKey := gotUnmanaged[k]; hasKey {
				t.Errorf("expected %s to omit %q, got %+v", label, k, gotUnmanaged)
			}
		}
	}

	// 5. Admitted execution-time event must carry priorityClassName, priority, and sanitized TAS podSetAssignments.
	execEvents := readJSONLines[ExecutionTimeEvent](t, rawExec)
	var foundJSAdmit *ExecutionTimePayload
	for i := range execEvents {
		if execEvents[i].EventType == ExecutionEventAdmitted &&
			execEvents[i].Payload.WorkloadName == scraper.hasher.HashName(secretWlName) {
			foundJSAdmit = &execEvents[i].Payload
			break
		}
	}
	if foundJSAdmit == nil {
		t.Fatalf("missing Admitted event for JobSet workload in execution-time.json")
	}
	if foundJSAdmit.PriorityClassName != "high-prio" || foundJSAdmit.Priority == nil || *foundJSAdmit.Priority != 500 {
		t.Errorf("expected priorityClassName=high-prio priority=500, got %+v", foundJSAdmit)
	}
	if len(foundJSAdmit.PodSetAssignments) != 1 || foundJSAdmit.PodSetAssignments[0].TopologyAssignment == nil {
		t.Fatalf("expected PodSetAssignments with TopologyAssignment on Admitted event, got %+v", foundJSAdmit.PodSetAssignments)
	}
	ta := foundJSAdmit.PodSetAssignments[0].TopologyAssignment
	wantRackHash := scraper.hasher.HashName(testSecretValue)
	if ta.Slices[0].ValuesPerLevel[0].Universal == nil || *ta.Slices[0].ValuesPerLevel[0].Universal != wantRackHash {
		t.Errorf("expected corp.io/rack topology value to be hashed to %q, got %+v", wantRackHash, ta.Slices[0].ValuesPerLevel[0])
	}
	if ta.Slices[0].ValuesPerLevel[1].Universal == nil || *ta.Slices[0].ValuesPerLevel[1].Universal != "tpu-node-0" {
		t.Errorf("expected kubernetes.io/hostname topology value to remain plain 'tpu-node-0', got %+v", ta.Slices[0].ValuesPerLevel[1])
	}
}

func TestVanillaAndChamberT0ParityBoundPendingAndTerminalPodDeletion(t *testing.T) {
	t0 := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	tDone := t0.Add(18 * time.Second)

	// 1. Vanilla K8s bound Pod in PodPending ("ContainerCreating") at t0 -> MUST be captured in infra.json at t0!
	vanillaBoundPendingPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "vanilla-job-0-abc",
			Namespace:         "team-ns",
			CreationTimestamp: metav1.NewTime(t0.Add(-10 * time.Second)),
			Labels: map[string]string{
				"job-name":                 "vanilla-job",
				"kueue.x-k8s.io/managed":   "true",
				KueueQueueLabel:            "team-lq",
			},
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: "batch/v1", Kind: "Job", Name: "vanilla-job", Controller: ptr.To(true)},
			},
		},
		Spec: corev1.PodSpec{
			NodeName: "worker-0",
			Containers: []corev1.Container{
				{
					Name:  "train",
					Image: "gcr.io/corp/secret:v1",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:           resource.MustParse("4"),
							corev1.ResourceMemory:        resource.MustParse("16Gi"),
							corev1.ResourceName("google.com/tpu"): resource.MustParse("4"),
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
		},
	}

	// 2. Chamber pre-admitted t0 PodGroup pod ("t0pg-init-0") at t0 -> MUST link to PodGroup Workload!
	chamberPGPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "init-0-w-0",
			Namespace:         "team-ns",
			CreationTimestamp: metav1.NewTime(t0.Add(-5 * time.Second)),
			Labels: map[string]string{
				KueueManagedLabel:      "true",
				KueueQueueLabel:        "team-lq",
				KueuePodGroupNameLabel: "t0pg-init-0",
				"kueue-optimizer.gke.io/initial-pod": "true",
			},
		},
		Spec: corev1.PodSpec{
			NodeName: "worker-1",
			Containers: []corev1.Container{
				{
					Name:  "pause",
					Image: DefaultPauseImage,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:           resource.MustParse("4"),
							corev1.ResourceName("google.com/tpu"): resource.MustParse("4"),
						},
					},
				},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}

	// 3. Historical pre-t0 already-Succeeded Job & Pod -> MUST be skipped at initial sync!
	histDoneTime := metav1.NewTime(t0.Add(-120 * time.Second))
	historicalCompletedJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "old-finished-job",
			Namespace:         "team-ns",
			CreationTimestamp: metav1.NewTime(t0.Add(-300 * time.Second)),
			Labels:            map[string]string{KueueQueueLabel: "team-lq"},
		},
		Spec: batchv1.JobSpec{
			Suspend: ptr.To(false),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers:    []corev1.Container{{Name: "c", Image: "busybox"}},
				},
			},
		},
		Status: batchv1.JobStatus{
			CompletionTime: &histDoneTime,
			Conditions: []batchv1.JobCondition{
				{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
			},
		},
	}
	historicalSucceededPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "old-standalone-pod",
			Namespace:         "team-ns",
			CreationTimestamp: metav1.NewTime(t0.Add(-300 * time.Second)),
			Labels:            map[string]string{KueueQueueLabel: "team-lq", KueueManagedLabel: "true"},
		},
		Spec: corev1.PodSpec{
			NodeName:   "worker-0",
			Containers: []corev1.Container{{Name: "c", Image: "busybox"}},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodSucceeded,
		},
	}

	activeVanillaJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "vanilla-job",
			Namespace:         "team-ns",
			CreationTimestamp: metav1.NewTime(t0.Add(-10 * time.Second)),
			Labels:            map[string]string{KueueQueueLabel: "team-lq"},
		},
		Spec: batchv1.JobSpec{
			Suspend: ptr.To(false),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers:    []corev1.Container{{Name: "train", Image: "gcr.io/corp/secret:v1"}},
				},
			},
		},
	}

	vanillaWl := &kueuev1beta2.Workload{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "job-vanilla-job-12345",
			Namespace:         "team-ns",
			CreationTimestamp: metav1.NewTime(t0.Add(-10 * time.Second)),
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: "batch/v1", Kind: "Job", Name: "vanilla-job", Controller: ptr.To(true)},
			},
		},
		Spec: kueuev1beta2.WorkloadSpec{
			QueueName: "team-lq",
			PodSets:   []kueuev1beta2.PodSet{{Name: "main", Count: 1}},
		},
		Status: kueuev1beta2.WorkloadStatus{
			Admission: &kueuev1beta2.Admission{ClusterQueue: "team-cq"},
			Conditions: []metav1.Condition{
				{Type: kueuev1beta2.WorkloadQuotaReserved, Status: metav1.ConditionTrue, LastTransitionTime: metav1.NewTime(t0.Add(-9 * time.Second))},
				{Type: kueuev1beta2.WorkloadAdmitted, Status: metav1.ConditionTrue, LastTransitionTime: metav1.NewTime(t0.Add(-9 * time.Second))},
			},
		},
	}
	chamberPGWl := &kueuev1beta2.Workload{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "t0pg-init-0",
			Namespace:         "team-ns",
			CreationTimestamp: metav1.NewTime(t0.Add(-5 * time.Second)),
			Annotations:       map[string]string{"kueue.x-k8s.io/is-group-workload": "true"},
		},
		Spec: kueuev1beta2.WorkloadSpec{
			QueueName: "team-lq",
			PodSets:   []kueuev1beta2.PodSet{{Name: "main", Count: 1}},
		},
		Status: kueuev1beta2.WorkloadStatus{
			Admission: &kueuev1beta2.Admission{ClusterQueue: "team-cq"},
			Conditions: []metav1.Condition{
				{Type: kueuev1beta2.WorkloadQuotaReserved, Status: metav1.ConditionTrue, LastTransitionTime: metav1.NewTime(t0.Add(-5 * time.Second))},
				{Type: kueuev1beta2.WorkloadAdmitted, Status: metav1.ConditionTrue, LastTransitionTime: metav1.NewTime(t0.Add(-5 * time.Second))},
			},
		},
	}

	kubeClient := kubefake.NewClientset(
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-0"}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-1"}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "team-ns"}},
		vanillaBoundPendingPod,
		chamberPGPod,
		historicalCompletedJob,
		historicalSucceededPod,
		activeVanillaJob,
	)
	kueueClient := kueuefake.NewSimpleClientset(vanillaWl, chamberPGWl)
	scraper := newTestScraper(t, Config{
		Interval:   time.Hour,
		Duration:   25 * time.Millisecond,
		PauseImage: DefaultPauseImage,
	}, kubeClient, kueueClient, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	if err := scraper.Run(ctx); err != nil {
		t.Fatalf("Scraper.Run: %v", err)
	}

	rawInfra, err := os.ReadFile(scraper.cfg.InfraFile)
	if err != nil {
		t.Fatalf("read infra.json: %v", err)
	}
	rawWf, err := os.ReadFile(scraper.cfg.WorkflowsFile)
	if err != nil {
		t.Fatalf("read workflows.json: %v", err)
	}

	infraEvents := readJSONLines[map[string]any](t, rawInfra)
	var firstTS string
	podsAtT0 := make(map[string]map[string]any)
	for _, ev := range infraEvents {
		ts := ev["timestamp"].(string)
		if firstTS == "" {
			firstTS = ts
		}
		if ts != firstTS {
			t.Errorf("expected unified startupTime %q across all initial infra events, got %q", firstTS, ts)
		}
		pl := ev["payload"].(map[string]any)
		if pl["kind"] == "Pod" && ev["eventType"] == string(EventNew) {
			podsAtT0[pl["name"].(string)] = pl
		}
	}

	// Vanilla K8s bound Pending ("ContainerCreating") pod MUST be present at t0 and linked to vanilla-job!
	if got := podsAtT0["vanilla-job-0-abc"]; got == nil {
		t.Fatalf("expected vanilla bound Pending pod in infra.json at t0")
	} else if got["workloadName"] != "job-vanilla-job-12345" || got["ownerKind"] != "Job" || got["ownerName"] != "vanilla-job" {
		t.Errorf("unexpected linkage on vanilla bound Pending pod: %+v", got)
	}

	// Chamber pre-admitted PodGroup pod MUST be present at t0 and linked to PodGroup "t0pg-init-0"!
	if got := podsAtT0["init-0-w-0"]; got == nil {
		t.Fatalf("expected chamber t0 PodGroup pod in infra.json at t0")
	} else if got["workloadName"] != "t0pg-init-0" || got["ownerKind"] != "PodGroup" || got["ownerName"] != "t0pg-init-0" {
		t.Errorf("unexpected linkage on chamber PodGroup pod: %+v", got)
	}

	// Historical pre-t0 Succeeded pod and completed Job MUST NOT be emitted!
	if _, exists := podsAtT0["old-standalone-pod"]; exists {
		t.Errorf("expected pre-t0 Succeeded standalone pod to be excluded from infra.json")
	}
	wfEvents := readJSONLines[map[string]any](t, rawWf)
	wfNames := make(map[string]bool)
	for _, ev := range wfEvents {
		if ev["timestamp"].(string) != firstTS {
			t.Errorf("expected unified startupTime %q on initial workflow event, got %v", firstTS, ev["timestamp"])
		}
		pl := ev["payload"].(map[string]any)
		wfNames[pl["objectKind"].(string)+"/"+pl["name"].(string)] = true
	}
	if wfNames["Job/old-finished-job"] {
		t.Errorf("expected pre-t0 already-completed Job to be excluded from workflows.json")
	}
	if wfNames["Pod/old-standalone-pod"] {
		t.Errorf("expected pre-t0 already-Succeeded Pod to be excluded from workflows.json")
	}
	if !wfNames["Job/vanilla-job"] || !wfNames["Pod/init-0-w-0"] {
		t.Errorf("expected active vanilla Job and chamber PodGroup member Pod in workflows.json, got %v", wfNames)
	}

	// Verify PodRunning -> PodSucceeded -> watch.Deleted emits at-most-once Deleted event with container finishedAt!
	watchLogPath := filepath.Join(t.TempDir(), "watch-infra.json")
	scraper.infraLog, err = NewJSONEventLog(watchLogPath)
	if err != nil {
		t.Fatalf("NewJSONEventLog: %v", err)
	}
	succPod := vanillaBoundPendingPod.DeepCopy()
	succPod.Status = corev1.PodStatus{
		Phase: corev1.PodSucceeded,
		ContainerStatuses: []corev1.ContainerStatus{
			{
				Name: "train",
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						ExitCode:   0,
						FinishedAt: metav1.NewTime(tDone),
					},
				},
			},
		},
	}
	scraper.handlePodWatchEventAt(watch.Modified, succPod, tDone.Add(2*time.Second), false)
	scraper.handlePodWatchEventAt(watch.Deleted, succPod, tDone.Add(30*time.Second), false)
	_ = scraper.infraLog.Close()

	watchRaw, err := os.ReadFile(watchLogPath)
	if err != nil {
		t.Fatalf("read watch-infra.json: %v", err)
	}
	watchEvs := readJSONLines[map[string]any](t, watchRaw)
	if len(watchEvs) != 1 || watchEvs[0]["eventType"] != string(EventDeleted) {
		t.Fatalf("expected exactly 1 Deleted event across PodSucceeded -> GC Deleted, got %d: %+v", len(watchEvs), watchEvs)
	}
	delPl := watchEvs[0]["payload"].(map[string]any)
	if delPl["deletionTime"] != tDone.Format(time.RFC3339) {
		t.Errorf("expected deletionTime=%q from container Terminated.FinishedAt, got %v", tDone.Format(time.RFC3339), delPl["deletionTime"])
	}
}

func TestNodeWatchRealTimeFailureRecoveryDeletionAndHeartbeatSuppression(t *testing.T) {
	scraper := newTestScraper(t, Config{}, nil, nil, nil)
	t0 := time.Date(2026, 11, 22, 1, 0, 0, 0, time.UTC)

	makeNode := func(name string, ready corev1.ConditionStatus, hb time.Time, trans time.Time, taints []corev1.Taint) *corev1.Node {
		return &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:            name,
				ResourceVersion: hb.Format(time.RFC3339Nano),
				Labels: map[string]string{
					"kubernetes.io/hostname": name,
				},
			},
			Spec: corev1.NodeSpec{
				Taints: taints,
			},
			Status: corev1.NodeStatus{
				Phase: corev1.NodeRunning,
				Conditions: []corev1.NodeCondition{
					{
						Type:               corev1.NodeReady,
						Status:             ready,
						LastHeartbeatTime:  metav1.NewTime(hb),
						LastTransitionTime: metav1.NewTime(trans),
					},
				},
			},
		}
	}

	// 1. Initial Added -> EventNew
	n0 := makeNode("st-node-00", corev1.ConditionTrue, t0, t0, nil)
	scraper.handleNodeWatchEventAt(watch.Added, n0, t0)

	// 2. Pure heartbeat update (only LastHeartbeatTime and ResourceVersion changed) -> MUST NOT emit Update!
	n0Heartbeat := makeNode("st-node-00", corev1.ConditionTrue, t0.Add(10*time.Second), t0, nil)
	scraper.handleNodeWatchEventAt(watch.Modified, n0Heartbeat, t0.Add(10*time.Second))

	// 3. Node failure (Ready=False + NoExecute taint) -> EventUpdate
	n0Failed := makeNode("st-node-00", corev1.ConditionFalse, t0.Add(15*time.Second), t0.Add(15*time.Second), []corev1.Taint{
		{Key: "node.kubernetes.io/unreachable", Effect: corev1.TaintEffectNoExecute},
	})
	scraper.handleNodeWatchEventAt(watch.Modified, n0Failed, t0.Add(15*time.Second))

	// 4. Node recovery (Ready=True, taints cleared) -> EventUpdate
	n0Recovered := makeNode("st-node-00", corev1.ConditionTrue, t0.Add(25*time.Second), t0.Add(25*time.Second), nil)
	scraper.handleNodeWatchEventAt(watch.Modified, n0Recovered, t0.Add(25*time.Second))

	// 5. Node deletion -> EventDeleted
	scraper.handleNodeWatchEventAt(watch.Deleted, n0Recovered, t0.Add(35*time.Second))

	_ = scraper.infraLog.Sync()
	rawInfra, err := os.ReadFile(scraper.cfg.InfraFile)
	if err != nil {
		t.Fatalf("read infra.json: %v", err)
	}
	evs := readJSONLines[InfraEvent](t, rawInfra)
	wantTypes := []EventType{EventNew, EventUpdate, EventUpdate, EventDeleted}
	if len(evs) != len(wantTypes) {
		t.Fatalf("expected %d Node events (heartbeat suppressed), got %d:\n%s", len(wantTypes), len(evs), string(rawInfra))
	}
	for i, wt := range wantTypes {
		if evs[i].EventType != wt {
			t.Errorf("evs[%d].EventType = %q, want %q", i, evs[i].EventType, wt)
		}
	}
}

func TestPartialExecutionNodeFailurePodsReadyTimeoutFailedAndScrapeEndedFlush(t *testing.T) {
	scraper := newTestScraper(t, Config{}, nil, nil, nil)
	t0 := time.Date(2026, 11, 22, 2, 0, 0, 0, time.UTC)

	ownerRef := func(name string) []metav1.OwnerReference {
		return []metav1.OwnerReference{
			{
				APIVersion: "batch/v1",
				Kind:       "Job",
				Name:       name,
				Controller: ptr.To(true),
			},
		}
	}

	// A. TAS Hot-Swap (unhealthyNodes populated transiently, then cleared by Kueue while staying Admitted -> FullExecution retains unhealthyNodes!)
	wlHotSwap := &kueuev1beta2.Workload{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "wl-hotswap",
			Namespace:         "ai-lab",
			CreationTimestamp: metav1.NewTime(t0),
			OwnerReferences:   ownerRef("job-hotswap"),
		},
		Spec: kueuev1beta2.WorkloadSpec{QueueName: "lq-main"},
		Status: kueuev1beta2.WorkloadStatus{
			Admission: &kueuev1beta2.Admission{ClusterQueue: "cq-main"},
			Conditions: []metav1.Condition{
				{Type: kueuev1beta2.WorkloadAdmitted, Status: metav1.ConditionTrue, Reason: "Admitted", LastTransitionTime: metav1.NewTime(t0)},
			},
		},
	}
	scraper.handleWorkloadWatchEventAt(watch.Added, wlHotSwap, t0)

	wlHotSwapFault := wlHotSwap.DeepCopy()
	wlHotSwapFault.Status.UnhealthyNodes = []kueuev1beta2.UnhealthyNode{{Name: "st-node-00"}}
	scraper.handleWorkloadWatchEventAt(watch.Modified, wlHotSwapFault, t0.Add(5*time.Second))

	wlHotSwapDone := wlHotSwap.DeepCopy()
	wlHotSwapDone.Status.UnhealthyNodes = nil // Kueue clears unhealthyNodes after swapping
	wlHotSwapDone.Status.Conditions = append(wlHotSwapDone.Status.Conditions, metav1.Condition{
		Type: kueuev1beta2.WorkloadFinished, Status: metav1.ConditionTrue, Reason: kueuev1beta2.WorkloadFinishedReasonSucceeded,
		LastTransitionTime: metav1.NewTime(t0.Add(16 * time.Second)),
	})
	scraper.handleWorkloadWatchEventAt(watch.Modified, wlHotSwapDone, t0.Add(16*time.Second))

	// B. NodeFailure Eviction with trueDurationSeconds & missingDurationMultiplier annotations
	wlNodeFail := &kueuev1beta2.Workload{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "wl-nodefail",
			Namespace:         "ai-lab",
			CreationTimestamp: metav1.NewTime(t0),
			OwnerReferences:   ownerRef("job-nodefail"),
			Annotations: map[string]string{
				"kueue-optimizer.gke.io/true-execution-time-seconds": "42.5",
				"kueue-optimizer.gke.io/missing-duration-multiplier": "2.5",
			},
		},
		Spec: kueuev1beta2.WorkloadSpec{QueueName: "lq-main"},
		Status: kueuev1beta2.WorkloadStatus{
			Admission:      &kueuev1beta2.Admission{ClusterQueue: "cq-main"},
			UnhealthyNodes: []kueuev1beta2.UnhealthyNode{{Name: "st-node-01"}},
			Conditions: []metav1.Condition{
				{Type: kueuev1beta2.WorkloadAdmitted, Status: metav1.ConditionTrue, Reason: "Admitted", LastTransitionTime: metav1.NewTime(t0)},
				{Type: kueuev1beta2.WorkloadEvicted, Status: metav1.ConditionTrue, Reason: kueuev1beta2.WorkloadEvictedDueToNodeFailures, LastTransitionTime: metav1.NewTime(t0.Add(7 * time.Second))},
			},
		},
	}
	scraper.handleWorkloadWatchEventAt(watch.Added, wlNodeFail, t0.Add(7*time.Second))

	// C. Terminal Failed Workload (must emit PartialExecution with InterruptionReasonFailed, NEVER FullExecution!)
	wlFailed := &kueuev1beta2.Workload{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "wl-failed",
			Namespace:         "ai-lab",
			CreationTimestamp: metav1.NewTime(t0),
			OwnerReferences:   ownerRef("job-failed"),
		},
		Spec: kueuev1beta2.WorkloadSpec{QueueName: "lq-main"},
		Status: kueuev1beta2.WorkloadStatus{
			Admission: &kueuev1beta2.Admission{ClusterQueue: "cq-main"},
			Conditions: []metav1.Condition{
				{Type: kueuev1beta2.WorkloadAdmitted, Status: metav1.ConditionTrue, Reason: "Admitted", LastTransitionTime: metav1.NewTime(t0.Add(2 * time.Second))},
			},
		},
	}
	scraper.handleWorkloadWatchEventAt(watch.Added, wlFailed, t0.Add(2*time.Second))
	wlFailedTerm := wlFailed.DeepCopy()
	wlFailedTerm.Status.Conditions = append(wlFailedTerm.Status.Conditions, metav1.Condition{
		Type: kueuev1beta2.WorkloadFinished, Status: metav1.ConditionTrue, Reason: kueuev1beta2.WorkloadFinishedReasonFailed,
		LastTransitionTime: metav1.NewTime(t0.Add(11 * time.Second)),
	})
	scraper.handleWorkloadWatchEventAt(watch.Modified, wlFailedTerm, t0.Add(11*time.Second))

	// D. In-Flight Admitted Workload flushed on Shutdown -> PartialExecution (InterruptionReasonScrapeEnded)
	wlInFlight := &kueuev1beta2.Workload{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "wl-inflight",
			Namespace:         "ai-lab",
			CreationTimestamp: metav1.NewTime(t0.Add(5 * time.Second)),
			OwnerReferences:   ownerRef("job-inflight"),
		},
		Spec: kueuev1beta2.WorkloadSpec{QueueName: "lq-main"},
		Status: kueuev1beta2.WorkloadStatus{
			Admission: &kueuev1beta2.Admission{ClusterQueue: "cq-main"},
			Conditions: []metav1.Condition{
				{Type: kueuev1beta2.WorkloadAdmitted, Status: metav1.ConditionTrue, Reason: "Admitted", LastTransitionTime: metav1.NewTime(t0.Add(5 * time.Second))},
			},
		},
	}
	scraper.handleWorkloadWatchEventAt(watch.Added, wlInFlight, t0.Add(5*time.Second))
	scraper.flushInFlightWorkloadsAsPartial(t0.Add(25 * time.Second))
	_ = scraper.execTimeLog.Sync()

	rawExec, err := os.ReadFile(scraper.cfg.ExecutionTimeFile)
	if err != nil {
		t.Fatalf("read execution-time.json: %v", err)
	}
	execEvs := readJSONLines[ExecutionTimeEvent](t, rawExec)

	byWl := make(map[string][]ExecutionTimeEvent)
	for _, ev := range execEvs {
		byWl[ev.Payload.WorkloadName] = append(byWl[ev.Payload.WorkloadName], ev)
	}

	// Verify A (wl-hotswap): Admitted + FullExecution(16s) with unhealthyNodes=["st-node-00"] and 0 PartialExecution
	hsEvs := byWl["wl-hotswap"]
	if len(hsEvs) != 2 || hsEvs[0].EventType != ExecutionEventAdmitted || hsEvs[1].EventType != ExecutionEventFull {
		t.Fatalf("wl-hotswap expected [Admitted, FullExecution], got %+v", hsEvs)
	}
	if len(hsEvs[1].Payload.UnhealthyNodes) != 1 || hsEvs[1].Payload.UnhealthyNodes[0] != "st-node-00" {
		t.Errorf("wl-hotswap FullExecution expected unhealthyNodes=[st-node-00], got %+v", hsEvs[1].Payload.UnhealthyNodes)
	}

	// Verify B (wl-nodefail): PartialExecution(7s, NodeFailure, preempted=false, trueDuration=42.5, mult=2.5)
	nfEvs := byWl["wl-nodefail"]
	if len(nfEvs) != 1 || nfEvs[0].EventType != ExecutionEventPartial || nfEvs[0].Payload.InterruptionReason != InterruptionReasonNodeFailure || nfEvs[0].Payload.Preempted {
		t.Fatalf("wl-nodefail expected [PartialExecution(NodeFailure, preempted=false)], got %+v", nfEvs)
	}
	if nfEvs[0].Payload.TrueDurationSeconds == nil || *nfEvs[0].Payload.TrueDurationSeconds != 42.5 ||
		nfEvs[0].Payload.MissingDurationMultiplier == nil || *nfEvs[0].Payload.MissingDurationMultiplier != 2.5 {
		t.Errorf("wl-nodefail missing trueDurationSeconds/missingDurationMultiplier: %+v", nfEvs[0].Payload)
	}

	// Verify C (wl-failed): Admitted + PartialExecution(9s, Failed), NO FullExecution
	flEvs := byWl["wl-failed"]
	if len(flEvs) != 2 || flEvs[1].EventType != ExecutionEventPartial || flEvs[1].Payload.InterruptionReason != InterruptionReasonFailed || *flEvs[1].Payload.ExecutionDurationSeconds != 9.0 {
		t.Fatalf("wl-failed expected [Admitted, PartialExecution(9s, Failed)], got %+v", flEvs)
	}

	// Verify D (wl-inflight): Admitted + PartialExecution(20s, ScrapeEnded, workloadState=Admitted)
	ifEvs := byWl["wl-inflight"]
	if len(ifEvs) != 2 || ifEvs[1].EventType != ExecutionEventPartial || ifEvs[1].Payload.InterruptionReason != InterruptionReasonScrapeEnded ||
		ifEvs[1].WorkloadState != WorkloadStateAdmitted || *ifEvs[1].Payload.ExecutionDurationSeconds != 20.0 {
		t.Fatalf("wl-inflight expected [Admitted, PartialExecution(20s, ScrapeEnded, state=Admitted)], got %+v", ifEvs)
	}
}

func TestScrapeAndSanitizeKueueConfiguration(t *testing.T) {
	rawYAML := `apiVersion: config.kueue.x-k8s.io/v1beta2
kind: Configuration
namespace: ` + testSecretValue + `
health:
  healthProbeBindAddress: :8081
metrics:
  bindAddress: :8443
  enableClusterQueueResources: true
webhook:
  port: 9443
leaderElection:
  leaderElect: true
  resourceName: ` + testSecretValue + `
internalCertManagement:
  enable: true
  webhookSecretName: ` + testSecretValue + `
clientConnection:
  qps: 50
  burst: 100
manageJobsWithoutQueueName: true
fairSharing:
  preemptionStrategies:
  - LessThanOrEqualToFinalShare
admissionFairSharing:
  usageHalfLifeTime: 20s
  usageSamplingInterval: 1s
  resourceWeights:
    google.com/tpu: 1
    cpu: 0
waitForPodsReady:
  timeout: 30s
  blockAdmission: true
featureGates:
  TASReplaceMultipleFailedNodes: true
`
	sanitized, ok := SanitizeKueueConfigurationYAML(rawYAML)
	if !ok {
		t.Fatalf("expected SanitizeKueueConfigurationYAML to accept valid Configuration YAML")
	}
	encoded, err := json.Marshal(sanitized)
	if err != nil {
		t.Fatalf("marshal sanitized Configuration: %v", err)
	}
	for _, secret := range testForbiddenSubstrings {
		if strings.Contains(string(encoded), secret) {
			t.Errorf("sanitized Configuration leaked %q: %s", secret, string(encoded))
		}
	}
	for _, forbiddenKey := range []string{"namespace", "health", "metrics", "webhook", "leaderElection", "internalCertManagement", "clientConnection"} {
		if _, exists := sanitized[forbiddenKey]; exists {
			t.Errorf("expected internal field %q to be stripped from sanitized Configuration: %s", forbiddenKey, string(encoded))
		}
	}
	for _, wantKey := range []string{"fairSharing", "admissionFairSharing", "waitForPodsReady", "featureGates", "manageJobsWithoutQueueName"} {
		if _, exists := sanitized[wantKey]; !exists {
			t.Errorf("expected scheduling policy field %q to be retained in sanitized Configuration: %s", wantKey, string(encoded))
		}
	}

	// Legacy v1beta1 fairSharing.enable: false must normalize to fairSharing omitted,
	// and legacy waitForPodsReady.enable: false must set featureGates.DisableWaitForPodsReady=true.
	legacyDisabledYAML := `apiVersion: config.kueue.x-k8s.io/v1beta1
kind: Configuration
fairSharing:
  enable: false
  preemptionStrategies:
  - LessThanOrEqualToFinalShare
waitForPodsReady:
  enable: false
  timeout: 10m
`
	legacySanitized, ok := SanitizeKueueConfigurationYAML(legacyDisabledYAML)
	if !ok {
		t.Fatalf("expected legacy v1beta1 Configuration to parse")
	}
	if _, hasFS := legacySanitized["fairSharing"]; hasFS {
		t.Errorf("expected legacy fairSharing.enable=false to omit fairSharing, got %+v", legacySanitized)
	}
	if _, hasWFPR := legacySanitized["waitForPodsReady"]; hasWFPR {
		t.Errorf("expected legacy waitForPodsReady.enable=false to omit waitForPodsReady, got %+v", legacySanitized)
	}
	fgSan, _ := legacySanitized["featureGates"].(map[string]any)
	if fgSan["DisableWaitForPodsReady"] != true {
		t.Errorf("expected legacy waitForPodsReady.enable=false to set featureGates.DisableWaitForPodsReady=true, got %+v", legacySanitized)
	}

	// Integration test: missing ConfigMap is a graceful no-op; adding/updating/deleting emits New -> suppressed -> Update -> Deleted.
	t0 := time.Date(2026, 11, 22, 3, 0, 0, 0, time.UTC)
	scraper := newTestScraper(t, Config{}, nil, nil, nil)
	scraper.scrapeKueueConfiguration(context.Background(), t0)

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      DefaultKueueConfigMapName,
			Namespace: DefaultKueueNamespace,
		},
		Data: map[string]string{
			"controller_manager_config.yaml": rawYAML,
		},
	}
	if _, err := scraper.kubeClient.CoreV1().ConfigMaps(DefaultKueueNamespace).Create(context.Background(), cm, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create kueue-manager-config: %v", err)
	}
	scraper.scrapeKueueConfiguration(context.Background(), t0.Add(5*time.Second))
	// Identical tick must not emit a duplicate event.
	scraper.scrapeKueueConfiguration(context.Background(), t0.Add(10*time.Second))

	// Update ConfigMap to globally disable FairSharing and AdmissionFairSharing.
	cmDisabled := cm.DeepCopy()
	cmDisabled.Data["controller_manager_config.yaml"] = "apiVersion: config.kueue.x-k8s.io/v1beta2\nkind: Configuration\nmetrics:\n  bindAddress: :8443\n"
	if _, err := scraper.kubeClient.CoreV1().ConfigMaps(DefaultKueueNamespace).Update(context.Background(), cmDisabled, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update kueue-manager-config: %v", err)
	}
	scraper.scrapeKueueConfiguration(context.Background(), t0.Add(15*time.Second))

	// Delete ConfigMap -> emits Deleted.
	if err := scraper.kubeClient.CoreV1().ConfigMaps(DefaultKueueNamespace).Delete(context.Background(), DefaultKueueConfigMapName, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete kueue-manager-config: %v", err)
	}
	scraper.scrapeKueueConfiguration(context.Background(), t0.Add(20*time.Second))
	_ = scraper.infraLog.Sync()

	rawInfra, err := os.ReadFile(scraper.cfg.InfraFile)
	if err != nil {
		t.Fatalf("read infra.json: %v", err)
	}
	evs := readJSONLines[InfraEvent](t, rawInfra)
	if len(evs) != 3 {
		t.Fatalf("expected [New, Update, Deleted] Configuration events, got %d:\n%s", len(evs), string(rawInfra))
	}
	if evs[0].EventType != EventNew || evs[1].EventType != EventUpdate || evs[2].EventType != EventDeleted {
		t.Errorf("unexpected Configuration event sequence: %+v", evs)
	}
	updMap, ok := evs[1].Payload.(map[string]any)
	if !ok {
		t.Fatalf("expected Update payload map, got %T", evs[1].Payload)
	}
	if _, hasFS := updMap["fairSharing"]; hasFS {
		t.Errorf("expected Update payload to omit fairSharing when disabled globally, got %+v", updMap)
	}
}

func TestDeletedAdmittedWorkloadEmitsDeletedPartialAndClearsScrapeEnded(t *testing.T) {
	scraper := newTestScraper(t, Config{}, nil, nil, nil)
	t0 := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)

	wl := &kueuev1beta2.Workload{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "wl-deleted-running",
			Namespace:         "ai-lab",
			CreationTimestamp: metav1.NewTime(t0),
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "batch/v1",
					Kind:       "Job",
					Name:       "job-deleted-running",
					Controller: ptr.To(true),
				},
			},
		},
		Spec: kueuev1beta2.WorkloadSpec{QueueName: "lq-main"},
		Status: kueuev1beta2.WorkloadStatus{
			Admission: &kueuev1beta2.Admission{ClusterQueue: "cq-main"},
			Conditions: []metav1.Condition{
				{
					Type:               kueuev1beta2.WorkloadAdmitted,
					Status:             metav1.ConditionTrue,
					Reason:             "Admitted",
					LastTransitionTime: metav1.NewTime(t0.Add(10 * time.Second)),
				},
			},
		},
	}

	// Admitted at t0+10s, deleted at t0+60s, scraper stops 2 hours later (t0+7200s).
	scraper.handleWorkloadWatchEventAt(watch.Added, wl, t0.Add(10*time.Second))
	scraper.handleWorkloadWatchEventAt(watch.Deleted, wl, t0.Add(60*time.Second))
	scraper.flushInFlightWorkloadsAsPartial(t0.Add(7200 * time.Second))
	_ = scraper.execTimeLog.Sync()

	rawExec, err := os.ReadFile(scraper.cfg.ExecutionTimeFile)
	if err != nil {
		t.Fatalf("read execution-time.json: %v", err)
	}
	evs := readJSONLines[ExecutionTimeEvent](t, rawExec)
	if len(evs) != 2 {
		t.Fatalf("expected strictly 2 events [Admitted, PartialExecution(Deleted)], got %d:\n%s", len(evs), string(rawExec))
	}
	if evs[1].EventType != ExecutionEventPartial || evs[1].Payload.InterruptionReason != InterruptionReasonDeleted {
		t.Fatalf("expected PartialExecution with InterruptionReason=%q, got %+v", InterruptionReasonDeleted, evs[1])
	}
	if evs[1].Payload.ExecutionDurationSeconds == nil || *evs[1].Payload.ExecutionDurationSeconds != 50.0 {
		t.Errorf("expected 50s deleted partial duration (not 7190s ScrapeEnded), got %v", evs[1].Payload.ExecutionDurationSeconds)
	}
}

func TestNeverAdmittedEvictedWorkloadEmitsZeroPartialExecution(t *testing.T) {
	scraper := newTestScraper(t, Config{}, nil, nil, nil)
	t0 := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)

	ownerRef := []metav1.OwnerReference{
		{
			APIVersion: "batch/v1",
			Kind:       "Job",
			Name:       "job-never-admitted",
			Controller: ptr.To(true),
		},
	}
	// 1. Workload evicted by AdmissionCheck 30m after creation without ever being admitted.
	wlPendingEvicted := &kueuev1beta2.Workload{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "wl-ac-evicted",
			Namespace:         "ai-lab",
			CreationTimestamp: metav1.NewTime(t0),
			OwnerReferences:   ownerRef,
		},
		Spec: kueuev1beta2.WorkloadSpec{QueueName: "lq-main"},
		Status: kueuev1beta2.WorkloadStatus{
			Conditions: []metav1.Condition{
				{
					Type:               kueuev1beta2.WorkloadAdmitted,
					Status:             metav1.ConditionFalse,
					Reason:             "UnsatisfiedChecks",
					LastTransitionTime: metav1.NewTime(t0),
				},
				{
					Type:               kueuev1beta2.WorkloadEvicted,
					Status:             metav1.ConditionTrue,
					Reason:             "AdmissionCheck",
					Message:            "ProvisioningRequest timed out",
					LastTransitionTime: metav1.NewTime(t0.Add(1800 * time.Second)),
				},
			},
		},
	}
	scraper.handleWorkloadWatchEventAt(watch.Added, wlPendingEvicted, t0.Add(1800*time.Second))

	// 2. Startup-pending workload with latched historical Preempted=True and zero observed admission / past seconds.
	wlLatchedPreemptPending := &kueuev1beta2.Workload{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "wl-latched-pending",
			Namespace:         "ai-lab",
			CreationTimestamp: metav1.NewTime(t0.Add(-3600 * time.Second)),
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: "batch/v1", Kind: "Job", Name: "job-latched", Controller: ptr.To(true)},
			},
		},
		Spec: kueuev1beta2.WorkloadSpec{QueueName: "lq-main"},
		Status: kueuev1beta2.WorkloadStatus{
			Conditions: []metav1.Condition{
				{
					Type:               kueuev1beta2.WorkloadAdmitted,
					Status:             metav1.ConditionFalse,
					Reason:             kueuev1beta2.WorkloadAdmittedReasonNoReservation,
					LastTransitionTime: metav1.NewTime(t0.Add(-600 * time.Second)),
				},
				{
					Type:               kueuev1beta2.WorkloadPreempted,
					Status:             metav1.ConditionTrue,
					Reason:             kueuev1beta2.InClusterQueueReason,
					LastTransitionTime: metav1.NewTime(t0.Add(-600 * time.Second)),
				},
			},
		},
	}
	scraper.handleWorkloadWatchEventAt(watch.Added, wlLatchedPreemptPending, t0)
	_ = scraper.execTimeLog.Sync()

	rawExec, err := os.ReadFile(scraper.cfg.ExecutionTimeFile)
	if err != nil {
		t.Fatalf("read execution-time.json: %v", err)
	}
	evs := readJSONLines[ExecutionTimeEvent](t, rawExec)
	if len(evs) != 0 {
		t.Fatalf("expected 0 PartialExecution events for never-admitted / unmeasured pre-scrape pending workloads, got %d:\n%s", len(evs), string(rawExec))
	}
}

func TestNonKueueFilteringWatchRelistAndStalePodListProtection(t *testing.T) {
	scraper := newTestScraper(t, Config{}, nil, nil, nil)
	t0 := time.Date(2026, 11, 22, 11, 0, 0, 0, time.UTC)

	mkJob := func(ns, name, lq string, complete bool) *batchv1.Job {
		var labels map[string]string
		if lq != "" {
			labels = map[string]string{KueueQueueLabel: lq}
		}
		var conds []batchv1.JobCondition
		if complete {
			conds = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
		}
		return &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:              name,
				Namespace:         ns,
				CreationTimestamp: metav1.NewTime(t0),
				Labels:            labels,
			},
			Spec: batchv1.JobSpec{
				Suspend: ptr.To(lq != ""),
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						RestartPolicy: corev1.RestartPolicyNever,
						Containers:    []corev1.Container{{Name: "c", Image: "busybox"}},
					},
				},
			},
			Status: batchv1.JobStatus{Conditions: conds},
		}
	}

	// 1. Non-Kueue kube-system Job and unqueued user Job are ignored; queued Job is recorded.
	scraper.handleJobWatchEventAt(watch.Added, mkJob("kube-system", "sys-cron", "", false), t0, false)
	scraper.handleJobWatchEventAt(watch.Added, mkJob("ai-lab", "plain-k8s-job", "", false), t0, false)
	scraper.handleJobWatchEventAt(watch.Added, mkJob("ai-lab", "kueue-job-1", "lq-main", false), t0, false)

	// 2. Simulated 410-Gone re-list: historical completed untracked job is skipped (isInitialSync=true),
	// and kueue-job-1 missing from present set emits Deleted.
	scraper.handleJobWatchEventAt(watch.Added, mkJob("ai-lab", "old-finished-job", "lq-main", true), t0.Add(5*time.Second), true)
	scraper.reconcileWorkflowDeletionsOnRelist("Job", map[string]bool{}, t0.Add(5*time.Second))
	_ = scraper.workflowsLog.Sync()

	rawWf, err := os.ReadFile(scraper.cfg.WorkflowsFile)
	if err != nil {
		t.Fatalf("read workflows.json: %v", err)
	}
	wfEvs := readJSONLines[WorkflowEvent](t, rawWf)
	if len(wfEvs) != 2 || wfEvs[0].EventType != EventNew || wfEvs[0].Payload.Name != "kueue-job-1" ||
		wfEvs[1].EventType != EventDeleted || wfEvs[1].Payload.Name != "kueue-job-1" {
		t.Fatalf("expected strictly [New(kueue-job-1), Deleted(kueue-job-1)], got %+v", wfEvs)
	}

	// 3. Real-time Pod watch emits New immediately, and a stale periodic Pods().List() snapshot taken
	// BEFORE the pod bound does NOT emit a false Deleted event!
	boundPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "rt-pod-1",
			Namespace:         "ai-lab",
			CreationTimestamp: metav1.NewTime(t0.Add(10 * time.Second)),
			Labels:            map[string]string{KueueQueueLabel: "lq-main"},
		},
		Spec: corev1.PodSpec{
			NodeName:   "node-1",
			Containers: []corev1.Container{{Name: "c", Image: "busybox"}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	scraper.handlePodWatchEventAt(watch.Added, boundPod, t0.Add(12*time.Second), false)
	// Simulate scrapeScheduledAndRunningPods where listStartedAt would be before firstNoticeTime:
	// pass empty prelistedPods with now == t0+11s (< firstNoticeTime t0+12s).
	scraper.scrapeScheduledAndRunningPodsWithList(context.Background(), t0.Add(11*time.Second), []corev1.Pod{})
	_ = scraper.infraLog.Sync()

	rawInfra, err := os.ReadFile(scraper.cfg.InfraFile)
	if err != nil {
		t.Fatalf("read infra.json: %v", err)
	}
	infEvs := readJSONLines[InfraEvent](t, rawInfra)
	if len(infEvs) != 1 || infEvs[0].EventType != EventNew {
		t.Fatalf("expected real-time Pod New with zero false Deleted from stale list, got %+v", infEvs)
	}
}

func TestScrapePriorityClassesDeploymentFeatureGatesAndInfraWatch(t *testing.T) {
	scraper := newTestScraper(t, Config{}, nil, nil, nil)
	t0 := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)

	// Custom ConfigMap name discovered from Deployment --config volume + --feature-gates CLI override.
	customCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "custom-kueue-cfg", Namespace: DefaultKueueNamespace},
		Data: map[string]string{
			"config.yaml": "apiVersion: config.kueue.x-k8s.io/v1beta2\nkind: Configuration\nmanageJobsWithoutQueueName: true\n",
		},
	}
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "kueue-controller-manager", Namespace: DefaultKueueNamespace},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{
						{
							Name:         "cfg-vol",
							VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: "custom-kueue-cfg"}}},
						},
					},
					Containers: []corev1.Container{
						{
							Name:         "manager",
							Args:         []string{"--config=/etc/custom/config.yaml", "--feature-gates=TopologyAwareScheduling=true,MultiKueue=false"},
							VolumeMounts: []corev1.VolumeMount{{Name: "cfg-vol", MountPath: "/etc/custom"}},
						},
					},
				},
			},
		},
	}
	_, _ = scraper.kubeClient.CoreV1().ConfigMaps(DefaultKueueNamespace).Create(context.Background(), customCM, metav1.CreateOptions{})
	_, _ = scraper.kubeClient.AppsV1().Deployments(DefaultKueueNamespace).Create(context.Background(), deploy, metav1.CreateOptions{})

	// System PriorityClass is skipped; user PriorityClass is recorded.
	_, _ = scraper.kubeClient.SchedulingV1().PriorityClasses().Create(context.Background(), &schedulingv1.PriorityClass{
		ObjectMeta: metav1.ObjectMeta{Name: "system-node-critical"},
		Value:      2000001000,
	}, metav1.CreateOptions{})
	_, _ = scraper.kubeClient.SchedulingV1().PriorityClasses().Create(context.Background(), &schedulingv1.PriorityClass{
		ObjectMeta: metav1.ObjectMeta{Name: "gpu-high-prio"},
		Value:      5000,
	}, metav1.CreateOptions{})

	scraper.scrapeKueueConfiguration(context.Background(), t0)
	scraper.scrapePriorityClasses(context.Background(), t0)

	// Real-time ClusterQueue watch update when nominalQuota changes.
	cq1 := &kueuev1beta2.ClusterQueue{
		ObjectMeta: metav1.ObjectMeta{Name: "cq-dyn"},
		Spec:       kueuev1beta2.ClusterQueueSpec{CohortName: "main-cohort"},
	}
	cq2 := cq1.DeepCopy()
	cq2.Spec.CohortName = "expanded-cohort"
	scraper.handleInfraObjectWatchEventAt(watch.Added, "ClusterQueue", "cq-dyn", "cq-dyn", "", SanitizeClusterQueue(cq1, scraper.hasher), t0.Add(1*time.Second))
	scraper.handleInfraObjectWatchEventAt(watch.Modified, "ClusterQueue", "cq-dyn", "cq-dyn", "", SanitizeClusterQueue(cq1, scraper.hasher), t0.Add(2*time.Second))
	scraper.handleInfraObjectWatchEventAt(watch.Modified, "ClusterQueue", "cq-dyn", "cq-dyn", "", SanitizeClusterQueue(cq2, scraper.hasher), t0.Add(3*time.Second))
	_ = scraper.infraLog.Sync()

	rawInfra, err := os.ReadFile(scraper.cfg.InfraFile)
	if err != nil {
		t.Fatalf("read infra.json: %v", err)
	}
	evs := readJSONLines[InfraEvent](t, rawInfra)
	if len(evs) != 4 {
		t.Fatalf("expected 4 infra events [Configuration(New), PriorityClass(New), ClusterQueue(New), ClusterQueue(Update)], got %d:\n%s", len(evs), string(rawInfra))
	}
	cfgMap, _ := evs[0].Payload.(map[string]any)
	fgMap, _ := cfgMap["featureGates"].(map[string]any)
	if fgMap["TopologyAwareScheduling"] != true || fgMap["MultiKueue"] != false || cfgMap["manageJobsWithoutQueueName"] != true {
		t.Errorf("unexpected merged Configuration payload: %+v", cfgMap)
	}
	if evs[3].EventType != EventUpdate {
		t.Errorf("expected ClusterQueue Update event, got %+v", evs[3])
	}
}

func TestExtractPodEndTimeIgnoresInitContainersAndDeletionGraceTimestamp(t *testing.T) {
	t0 := time.Date(2026, 3, 25, 12, 0, 0, 0, time.UTC)
	initFinished := metav1.NewTime(t0.Add(-7190 * time.Second))
	scrapeDeleteNow := t0.Add(600 * time.Second)
	futureGraceTs := metav1.NewTime(scrapeDeleteNow.Add(30 * time.Second))

	// Case 1 & 2 repro: long-running pod with terminated init container from startup (2h ago) and
	// DeletionTimestamp (+30s grace in the future) deleted while its main container is still Running.
	runningWithInitPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "long-running-pod",
			Namespace:         "ai-lab",
			CreationTimestamp: metav1.NewTime(t0.Add(-7200 * time.Second)),
			DeletionTimestamp: &futureGraceTs,
		},
		Spec: corev1.PodSpec{NodeName: "node-0"},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			InitContainerStatuses: []corev1.ContainerStatus{{
				Name: "init-setup",
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						ExitCode:   0,
						FinishedAt: initFinished,
					},
				},
			}},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "main-trainer",
				State: corev1.ContainerState{
					Running: &corev1.ContainerStateRunning{
						StartedAt: metav1.NewTime(t0.Add(-7189 * time.Second)),
					},
				},
			}},
		},
	}
	if got := ExtractPodEndTime(runningWithInitPod, scrapeDeleteNow); !got.Equal(scrapeDeleteNow) {
		t.Fatalf("expected fallback event time %s when main container is still running, got %s", scrapeDeleteNow, got)
	}

	// Multi-container pod where 1 main container finished 1h ago and 2nd main container is still Running.
	partialTermPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "multi-container-pod",
			Namespace:         "ai-lab",
			DeletionTimestamp: &futureGraceTs,
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "helper",
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							FinishedAt: metav1.NewTime(t0.Add(-3600 * time.Second)),
						},
					},
				},
				{
					Name: "worker",
					State: corev1.ContainerState{
						Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(t0)},
					},
				},
			},
		},
	}
	if got := ExtractPodEndTime(partialTermPod, scrapeDeleteNow); !got.Equal(scrapeDeleteNow) {
		t.Fatalf("expected fallback event time %s when any main container has not terminated, got %s", scrapeDeleteNow, got)
	}

	// Succeeded pod where all main containers have terminated returns the latest main container FinishedAt.
	mainFinished := t0.Add(595 * time.Second)
	succeededPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "done-pod", Namespace: "ai-lab"},
		Status: corev1.PodStatus{
			Phase: corev1.PodSucceeded,
			InitContainerStatuses: []corev1.ContainerStatus{{
				Name: "init-setup",
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{FinishedAt: initFinished},
				},
			}},
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "c1",
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{FinishedAt: metav1.NewTime(t0.Add(590 * time.Second))},
					},
				},
				{
					Name: "c2",
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{FinishedAt: metav1.NewTime(mainFinished)},
					},
				},
			},
		},
	}
	if got := ExtractPodEndTime(succeededPod, scrapeDeleteNow); !got.Equal(mainFinished) {
		t.Fatalf("expected latest main container FinishedAt %s, got %s", mainFinished, got)
	}
}

func TestResolveWorkloadOwnerPreservesLWSAndStatefulSetOverGroupAnnotation(t *testing.T) {
	hasher := NoopHasher{}

	// 1. LeaderWorkerSet group workload (carries is-group-workload="true" AND non-controller Pod + LWS OwnerReferences)
	lwsWl := &kueuev1beta2.Workload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "lws-serving-0-abcde",
			Namespace: "ai-lab",
			Annotations: map[string]string{
				KueueIsGroupWorkloadAnnotation: "true",
				KueueJobOwnerGVKAnnotation:     "leaderworkerset.x-k8s.io/v1, Kind=LeaderWorkerSet",
				KueueJobOwnerNameAnnotation:    "lws-serving",
			},
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: "v1", Kind: "Pod", Name: "lws-serving-0-0"},
				{APIVersion: "leaderworkerset.x-k8s.io/v1", Kind: "LeaderWorkerSet", Name: "lws-serving"},
			},
		},
	}
	kind, name, apiVer := resolveWorkloadOwner(lwsWl, hasher)
	if kind != "LeaderWorkerSet" || name != "lws-serving" || apiVer != "leaderworkerset.x-k8s.io/v1" {
		t.Fatalf("expected LeaderWorkerSet/lws-serving/leaderworkerset.x-k8s.io/v1, got %q/%q/%q", kind, name, apiVer)
	}

	// 2. StatefulSet group workload (carries is-group-workload="true" AND non-controller Pod + StatefulSet OwnerReferences)
	stsWl := &kueuev1beta2.Workload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "statefulset-db-12345",
			Namespace: "ai-lab",
			Annotations: map[string]string{
				KueueIsGroupWorkloadAnnotation: "true",
			},
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: "v1", Kind: "Pod", Name: "db-0"},
				{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "db"},
			},
		},
	}
	kind, name, apiVer = resolveWorkloadOwner(stsWl, hasher)
	if kind != "StatefulSet" || name != "db" || apiVer != "apps/v1" {
		t.Fatalf("expected StatefulSet/db/apps/v1, got %q/%q/%q", kind, name, apiVer)
	}

	// 3. StatefulSet group workload after GC cleared OwnerReferences (retains job-owner annotations)
	stsGCWl := &kueuev1beta2.Workload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "statefulset-db-12345",
			Namespace: "ai-lab",
			Annotations: map[string]string{
				KueueIsGroupWorkloadAnnotation: "true",
				KueueJobOwnerGVKAnnotation:     "apps/v1, Kind=StatefulSet",
				KueueJobOwnerNameAnnotation:    "db",
			},
		},
	}
	kind, name, apiVer = resolveWorkloadOwner(stsGCWl, hasher)
	if kind != "StatefulSet" || name != "db" || apiVer != "apps/v1" {
		t.Fatalf("expected StatefulSet/db/apps/v1 from job-owner annotations, got %q/%q/%q", kind, name, apiVer)
	}

	// 4. Plain PodGroup Workload (is-group-workload="true" with only member Pod OwnerReferences) stays PodGroup!
	pgWl := &kueuev1beta2.Workload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-pod-group",
			Namespace: "ai-lab",
			Annotations: map[string]string{
				KueueIsGroupWorkloadAnnotation: "true",
			},
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: "v1", Kind: "Pod", Name: "pg-worker-0"},
				{APIVersion: "v1", Kind: "Pod", Name: "pg-worker-1"},
			},
		},
	}
	kind, name, apiVer = resolveWorkloadOwner(pgWl, hasher)
	if kind != "PodGroup" || name != "my-pod-group" || apiVer != "v1" {
		t.Fatalf("expected PodGroup/my-pod-group/v1 for plain pod group, got %q/%q/%q", kind, name, apiVer)
	}
}

func TestDeploymentLifecycleScrapingAndOwnership(t *testing.T) {
	t0 := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	createMeta := metav1.NewTime(t0)

	scraper := newTestScraper(t, Config{}, nil, nil, nil)

	// 1. Index ReplicaSet -> Deployment and 2 running Pods -> ReplicaSet, then emit 2 per-Pod Kueue Workloads.
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "infer-srv-7f8d9c",
			Namespace: "ai-lab",
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: "apps/v1", Kind: "Deployment", Name: "infer-srv", Controller: ptr.To(true)},
			},
		},
	}
	scraper.handleReplicaSetWatchEvent(watch.Added, rs)

	mkPod := func(podName, nodeName string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:              podName,
				Namespace:         "ai-lab",
				CreationTimestamp: createMeta,
				Labels: map[string]string{
					KueueQueueLabel:   "serving-lq",
					KueueManagedLabel: "true",
				},
				Annotations: map[string]string{
					KueueSuspendedByParentAnnot: "deployment",
				},
				OwnerReferences: []metav1.OwnerReference{
					{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "infer-srv-7f8d9c", Controller: ptr.To(true)},
				},
			},
			Spec: corev1.PodSpec{
				NodeName: nodeName,
				Containers: []corev1.Container{
					{
						Name:  "srv",
						Image: "registry.corp.internal/secret-model:v1",
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("2"),
								corev1.ResourceMemory: resource.MustParse("8Gi"),
							},
						},
					},
				},
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
			},
		}
	}

	mkPodWl := func(wlName, podName string, admitted bool, preempted bool, ts time.Time) *kueuev1beta2.Workload {
		var conds []metav1.Condition
		if admitted {
			conds = append(conds, metav1.Condition{
				Type:               kueuev1beta2.WorkloadAdmitted,
				Status:             metav1.ConditionTrue,
				Reason:             "Admitted",
				LastTransitionTime: metav1.NewTime(ts),
			})
		}
		if preempted {
			conds = append(conds, metav1.Condition{
				Type:               kueuev1beta2.WorkloadPreempted,
				Status:             metav1.ConditionTrue,
				Reason:             "InClusterQueue",
				LastTransitionTime: metav1.NewTime(ts),
			})
		}
		return &kueuev1beta2.Workload{
			ObjectMeta: metav1.ObjectMeta{
				Name:              wlName,
				Namespace:         "ai-lab",
				CreationTimestamp: createMeta,
				OwnerReferences: []metav1.OwnerReference{
					{APIVersion: "v1", Kind: "Pod", Name: podName, Controller: ptr.To(true)},
				},
			},
			Spec: kueuev1beta2.WorkloadSpec{
				QueueName: "serving-lq",
				PodSets: []kueuev1beta2.PodSet{
					{Name: "main", Count: 1},
				},
			},
			Status: kueuev1beta2.WorkloadStatus{
				Admission: &kueuev1beta2.Admission{
					ClusterQueue: "serving-cq",
				},
				Conditions: conds,
			},
		}
	}

	scraper.handleWorkloadWatchEventAt(watch.Added, mkPodWl("pod-infer-srv-7f8d9c-aa111-w1", "infer-srv-7f8d9c-aa111", true, false, t0), t0)
	scraper.handleWorkloadWatchEventAt(watch.Added, mkPodWl("pod-infer-srv-7f8d9c-bb222-w2", "infer-srv-7f8d9c-bb222", true, false, t0), t0)
	scraper.handlePodWatchEventAt(watch.Added, mkPod("infer-srv-7f8d9c-aa111", "node-0"), t0, false)
	scraper.handlePodWatchEventAt(watch.Added, mkPod("infer-srv-7f8d9c-bb222", "node-1"), t0, false)

	// 2. Emit t0 Deployment with partial readiness (replicas=4, readyReplicas=2) -> ScaleUp -> Upgrade (image-only!) -> Paused -> Resumed -> ScaleDown(0) -> Deleted.
	mkDeploy := func(replicas, ready int32, paused bool, image, secretEnv string) *appsv1.Deployment {
		return &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "infer-srv",
				Namespace:         "ai-lab",
				UID:               "uid-infer-srv-0",
				CreationTimestamp: createMeta,
				Labels:            map[string]string{KueueQueueLabel: "serving-lq"},
			},
			Spec: appsv1.DeploymentSpec{
				Replicas: ptr.To(replicas),
				Paused:   paused,
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "infer-srv"}},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{"app": "infer-srv", KueueManagedLabel: "true"},
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:  "srv",
								Image: image,
								Env:   []corev1.EnvVar{{Name: "API_SECRET", Value: secretEnv}},
							},
						},
					},
				},
			},
			Status: appsv1.DeploymentStatus{
				Replicas:      replicas,
				ReadyReplicas: ready,
			},
		}
	}

	scraper.handleDeploymentWatchEventAt(watch.Added, mkDeploy(4, 2, false, "registry.corp.internal/model:v1", "SUPER_SECRET_V1"), t0)
	// Status-only ReadyReplicas change (2 -> 3) must NOT emit a spurious workflows.json Update!
	scraper.handleDeploymentWatchEventAt(watch.Modified, mkDeploy(4, 3, false, "registry.corp.internal/model:v1", "SUPER_SECRET_V1"), t0.Add(2*time.Second))
	// ScaleUp 4 -> 6
	scraper.handleDeploymentWatchEventAt(watch.Modified, mkDeploy(6, 4, false, "registry.corp.internal/model:v1", "SUPER_SECRET_V1"), t0.Add(5*time.Second))
	// Rolling Upgrade (image/env only — must change templateHash without leaking image or secret!)
	scraper.handleDeploymentWatchEventAt(watch.Modified, mkDeploy(6, 6, false, "registry.corp.internal/model:v2", "SUPER_SECRET_V2"), t0.Add(10*time.Second))
	// Paused -> Resumed -> ScaleDown to 0 -> Deleted
	scraper.handleDeploymentWatchEventAt(watch.Modified, mkDeploy(6, 6, true, "registry.corp.internal/model:v2", "SUPER_SECRET_V2"), t0.Add(15*time.Second))
	scraper.handleDeploymentWatchEventAt(watch.Modified, mkDeploy(6, 6, false, "registry.corp.internal/model:v2", "SUPER_SECRET_V2"), t0.Add(20*time.Second))
	scraper.handleDeploymentWatchEventAt(watch.Modified, mkDeploy(0, 0, false, "registry.corp.internal/model:v2", "SUPER_SECRET_V2"), t0.Add(25*time.Second))

	// Preempting 1 of the 2 replica Workloads keeps aggregate ownerStates["Deployment/ai-lab/infer-srv"] == Admitted!
	scraper.handleWorkloadWatchEventAt(watch.Modified, mkPodWl("pod-infer-srv-7f8d9c-aa111-w1", "infer-srv-7f8d9c-aa111", false, true, t0.Add(26*time.Second)), t0.Add(26*time.Second))
	if gotOwnerSt := scraper.ownerStates["Deployment/ai-lab/infer-srv"]; gotOwnerSt != WorkloadStateAdmitted {
		t.Fatalf("expected Deployment ownerState to remain Admitted while 2nd replica is Admitted, got %q", gotOwnerSt)
	}
	scraper.handleWorkloadWatchEventAt(watch.Modified, mkPodWl("pod-infer-srv-7f8d9c-bb222-w2", "infer-srv-7f8d9c-bb222", false, true, t0.Add(27*time.Second)), t0.Add(27*time.Second))
	if gotOwnerSt := scraper.ownerStates["Deployment/ai-lab/infer-srv"]; gotOwnerSt != WorkloadStateNotAdmitted {
		t.Fatalf("expected Deployment ownerState to flip to NotAdmitted when all replicas are preempted, got %q", gotOwnerSt)
	}

	scraper.handleDeploymentWatchEventAt(watch.Deleted, mkDeploy(0, 0, false, "registry.corp.internal/model:v2", "SUPER_SECRET_V2"), t0.Add(30*time.Second))

	rawWf, err := os.ReadFile(scraper.cfg.WorkflowsFile)
	if err != nil {
		t.Fatalf("read workflows.json: %v", err)
	}
	if strings.Contains(string(rawWf), "SUPER_SECRET") || strings.Contains(string(rawWf), "registry.corp.internal") {
		t.Fatalf("workflows.json leaked unsanitized container image or env: %s", string(rawWf))
	}
	wfEvents := readJSONLines[WorkflowEvent](t, rawWf)
	if len(wfEvents) != 7 {
		t.Fatalf("expected 7 Deployment workflow events (New, ScaleUp, Upgrade, Paused, Resumed, ScaleDown, Deleted), got %d", len(wfEvents))
	}
	if wfEvents[0].EventType != EventNew || *wfEvents[0].Payload.Replicas != 4 || *wfEvents[0].Payload.ReadyReplicas != 2 {
		t.Fatalf("unexpected t0 New event payload: %+v", wfEvents[0].Payload)
	}
	if !slices.Equal(wfEvents[1].Payload.UpdateTypes, []string{DeploymentUpdateScaleUp}) || *wfEvents[1].Payload.Replicas != 6 {
		t.Fatalf("expected ScaleUp to 6, got %+v", wfEvents[1].Payload)
	}
	if !slices.Equal(wfEvents[2].Payload.UpdateTypes, []string{DeploymentUpdateUpgrade}) ||
		wfEvents[2].Payload.TemplateHash == "" ||
		wfEvents[2].Payload.TemplateHash == wfEvents[1].Payload.TemplateHash {
		t.Fatalf("expected Upgrade with changed TemplateHash, got prev=%q new=%+v", wfEvents[1].Payload.TemplateHash, wfEvents[2].Payload)
	}
	if !slices.Equal(wfEvents[3].Payload.UpdateTypes, []string{DeploymentUpdatePaused}) || !*wfEvents[3].Payload.Paused {
		t.Fatalf("expected Paused=true update, got %+v", wfEvents[3].Payload)
	}
	if !slices.Equal(wfEvents[4].Payload.UpdateTypes, []string{DeploymentUpdateResumed}) || *wfEvents[4].Payload.Paused {
		t.Fatalf("expected Resumed (Paused=false) update, got %+v", wfEvents[4].Payload)
	}
	if !slices.Contains(wfEvents[5].Payload.UpdateTypes, DeploymentUpdateScaleDown) || *wfEvents[5].Payload.Replicas != 0 {
		t.Fatalf("expected ScaleDown to 0, got %+v", wfEvents[5].Payload)
	}
	if wfEvents[6].EventType != EventDeleted {
		t.Fatalf("expected final Deleted event, got %+v", wfEvents[6])
	}

	// Verify execution-time.json resolved ownerKind="Deployment" and ownerName="infer-srv" for both per-Pod Workloads.
	rawExec, err := os.ReadFile(scraper.cfg.ExecutionTimeFile)
	if err != nil {
		t.Fatalf("read execution-time.json: %v", err)
	}
	execEvents := readJSONLines[ExecutionTimeEvent](t, rawExec)
	if len(execEvents) != 4 {
		t.Fatalf("expected 4 execution-time events (2 Admitted + 2 Preempted Partial), got %d", len(execEvents))
	}
	for _, ex := range execEvents {
		if ex.Payload.OwnerKind != "Deployment" || ex.Payload.OwnerName != "infer-srv" || ex.Payload.OwnerAPIVersion != "apps/v1" {
			t.Fatalf("expected Deployment/infer-srv/apps/v1 on Workload %s, got %+v", ex.Payload.WorkloadName, ex.Payload)
		}
	}

	// Verify infra.json kept distinct per-Pod Workload names on the 2 Deployment pods!
	rawInfra, err := os.ReadFile(scraper.cfg.InfraFile)
	if err != nil {
		t.Fatalf("read infra.json: %v", err)
	}
	infraEvents := readJSONLines[InfraEvent](t, rawInfra)
	var podWlNames []string
	for _, ie := range infraEvents {
		pm, ok := ie.Payload.(map[string]any)
		if ok && pm["kind"] == "Pod" {
			if pm["ownerKind"] != "Deployment" || pm["ownerName"] != "infer-srv" {
				t.Fatalf("expected Pod linkage owner Deployment/infer-srv, got %+v", pm)
			}
			podWlNames = append(podWlNames, pm["workloadName"].(string))
		}
	}
	slices.Sort(podWlNames)
	wantWlNames := []string{"pod-infer-srv-7f8d9c-aa111-w1", "pod-infer-srv-7f8d9c-bb222-w2"}
	if !slices.Equal(podWlNames, wantWlNames) {
		t.Fatalf("expected distinct per-pod workloadNames %v, got %v", wantWlNames, podWlNames)
	}
}

func TestDeploymentOwnerNotFoundNonTASAndReplicaSetRace(t *testing.T) {
	scraper := newTestScraper(t, Config{}, nil, nil, nil)

	t0 := time.Date(2026, 6, 1, 13, 0, 0, 0, time.UTC)
	createMeta := metav1.NewTime(t0)

	// 1. Deployment arrives BEFORE ReplicaSet informer has seen new ReplicaSet "srv-pool-5bbdc7598f".
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "srv-pool",
			Namespace:         "ai-lab",
			UID:               "uid-srv-pool",
			CreationTimestamp: createMeta,
			Labels:            map[string]string{KueueQueueLabel: "serving-lq"},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To(int32(1)),
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "srv-pool"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": "srv-pool", KueueManagedLabel: "true"},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "srv", Image: "pause:3.9"}},
				},
			},
		},
	}
	scraper.handleDeploymentWatchEventAt(watch.Added, dep, t0)

	// 2. Pod arrives BEFORE ReplicaSet and Workload watch events; pod-template-hash fallback must link Deployment on EventNew!
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "srv-pool-5bbdc7598f-x9k2p",
			Namespace:         "ai-lab",
			CreationTimestamp: createMeta,
			Labels: map[string]string{
				"app":               "srv-pool",
				"pod-template-hash": "5bbdc7598f",
				KueueQueueLabel:     "serving-lq",
				KueueManagedLabel:   "true",
			},
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "srv-pool-5bbdc7598f", Controller: ptr.To(true)},
			},
		},
		Spec: corev1.PodSpec{
			NodeName:   "node-0",
			Containers: []corev1.Container{{Name: "srv", Image: "pause:3.9"}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	scraper.handlePodWatchEventAt(watch.Added, pod, t0.Add(time.Second), false)

	// 3. Workload has PodIndexLabel-only TopologyRequest (non-TAS!) and later finishes with OwnerNotFound on scale-down.
	mkWl := func(ownerNotFound bool, ts time.Time) *kueuev1beta2.Workload {
		conds := []metav1.Condition{
			{
				Type:               kueuev1beta2.WorkloadAdmitted,
				Status:             metav1.ConditionTrue,
				Reason:             "Admitted",
				LastTransitionTime: metav1.NewTime(t0.Add(time.Second)),
			},
		}
		if ownerNotFound {
			conds = append(conds, metav1.Condition{
				Type:               kueuev1beta2.WorkloadFinished,
				Status:             metav1.ConditionTrue,
				Reason:             kueuev1beta2.WorkloadFinishedReasonOwnerNotFound,
				Message:            "The workload's owner no longer exists",
				LastTransitionTime: metav1.NewTime(ts),
			})
		}
		return &kueuev1beta2.Workload{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "pod-srv-pool-5bbdc7598f-x9k2p-11111",
				Namespace:         "ai-lab",
				CreationTimestamp: createMeta,
				OwnerReferences: []metav1.OwnerReference{
					{APIVersion: "v1", Kind: "Pod", Name: "srv-pool-5bbdc7598f-x9k2p"},
				},
			},
			Spec: kueuev1beta2.WorkloadSpec{
				QueueName: "serving-lq",
				PodSets: []kueuev1beta2.PodSet{
					{
						Name:  "main",
						Count: 1,
						TopologyRequest: &kueuev1beta2.PodSetTopologyRequest{
							PodIndexLabel: ptr.To("batch.kubernetes.io/job-completion-index"),
						},
					},
				},
			},
			Status: kueuev1beta2.WorkloadStatus{
				Admission:  &kueuev1beta2.Admission{ClusterQueue: "serving-cq"},
				Conditions: conds,
			},
		}
	}
	scraper.handleWorkloadWatchEventAt(watch.Added, mkWl(false, t0.Add(time.Second)), t0.Add(time.Second))
	scraper.handlePodWatchEventAt(watch.Modified, pod, t0.Add(2*time.Second), false)
	scraper.handleWorkloadWatchEventAt(watch.Modified, mkWl(true, t0.Add(11*time.Second)), t0.Add(11*time.Second))
	// Subsequent GC watch.Deleted must NOT emit a duplicate PartialExecution event.
	scraper.handleWorkloadWatchEventAt(watch.Deleted, mkWl(true, t0.Add(11*time.Second)), t0.Add(12*time.Second))

	rawInfra, err := os.ReadFile(scraper.cfg.InfraFile)
	if err != nil {
		t.Fatalf("read infra.json: %v", err)
	}
	infraEvents := readJSONLines[InfraEvent](t, rawInfra)
	var sawPodNew bool
	for _, ie := range infraEvents {
		pm, ok := ie.Payload.(map[string]any)
		if !ok || pm["kind"] != "Pod" {
			continue
		}
		if ie.EventType == EventNew {
			sawPodNew = true
			if pm["ownerKind"] != "Deployment" || pm["ownerName"] != "srv-pool" {
				t.Fatalf("expected immediate Deployment/srv-pool linkage on Pod EventNew before ReplicaSet informer, got %+v", pm)
			}
		}
		if _, hasTAS := pm["isTAS"]; hasTAS && pm["isTAS"] == true {
			t.Fatalf("expected non-TAS PodIndexLabel-only Workload to keep isTAS=false, got %+v", pm)
		}
	}
	if !sawPodNew {
		t.Fatalf("expected Pod EventNew in infra.json")
	}

	rawExec, err := os.ReadFile(scraper.cfg.ExecutionTimeFile)
	if err != nil {
		t.Fatalf("read execution-time.json: %v", err)
	}
	execEvents := readJSONLines[ExecutionTimeEvent](t, rawExec)
	if len(execEvents) != 2 {
		t.Fatalf("expected 2 execution-time events (Admitted + single Deleted PartialExecution), got %d", len(execEvents))
	}
	if execEvents[1].EventType != ExecutionEventPartial ||
		execEvents[1].Payload.InterruptionReason != InterruptionReasonDeleted ||
		execEvents[1].Payload.Reason != kueuev1beta2.WorkloadFinishedReasonOwnerNotFound {
		t.Fatalf("expected OwnerNotFound to classify as InterruptionReasonDeleted, got %+v", execEvents[1].Payload)
	}
}

func TestComputePodTemplateRevisionHashUsesPerRunSalt(t *testing.T) {
	tpl := &corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{"app": "infer-srv"},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "srv",
					Image: "registry.corp.internal/secret-model:v1",
					Env:   []corev1.EnvVar{{Name: "API_TOKEN", Value: "secret-123"}},
				},
			},
		},
	}

	saltA := []byte("0123456789abcdef0123456789abcdef")
	saltB := []byte("fedcba9876543210fedcba9876543210")

	hashNoop := ComputePodTemplateRevisionHash(tpl, NewNoopHasher())
	hashSaltA1 := ComputePodTemplateRevisionHash(tpl, NewSensitiveDataHasher(nil, saltA))
	hashSaltA2 := ComputePodTemplateRevisionHash(tpl, NewSensitiveDataHasher(nil, saltA))
	hashSaltB := ComputePodTemplateRevisionHash(tpl, NewSensitiveDataHasher(nil, saltB))

	if !strings.HasPrefix(hashNoop, "th-") || !strings.HasPrefix(hashSaltA1, "th-") || !strings.HasPrefix(hashSaltB, "th-") {
		t.Fatalf("expected th- prefixed revision hashes, got noop=%q saltA=%q saltB=%q", hashNoop, hashSaltA1, hashSaltB)
	}
	if hashSaltA1 != hashSaltA2 {
		t.Fatalf("expected deterministic hash for identical salt, got %q vs %q", hashSaltA1, hashSaltA2)
	}
	if hashSaltA1 == hashNoop {
		t.Fatalf("expected salted templateHash %q to differ from unsalted NoopHasher %q", hashSaltA1, hashNoop)
	}
	if hashSaltA1 == hashSaltB {
		t.Fatalf("expected distinct salts to produce distinct templateHashes, both produced %q", hashSaltA1)
	}

	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "infer-srv", Namespace: "ai-lab"},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "infer-srv"}},
			Template: *tpl.DeepCopy(),
		},
	}
	sanA := SanitizeDeployment(deploy, "pause:3.9", NewSensitiveDataHasher(nil, saltA))
	sanB := SanitizeDeployment(deploy, "pause:3.9", NewSensitiveDataHasher(nil, saltB))
	gotAnnA := sanA["spec"].(map[string]any)["template"].(map[string]any)["metadata"].(map[string]any)["annotations"].(map[string]any)[KueueOptimizerTemplateHashAnnot]
	gotAnnB := sanB["spec"].(map[string]any)["template"].(map[string]any)["metadata"].(map[string]any)["annotations"].(map[string]any)[KueueOptimizerTemplateHashAnnot]
	if gotAnnA != hashSaltA1 || gotAnnB != hashSaltB || gotAnnA == gotAnnB {
		t.Fatalf("expected SanitizeDeployment to stamp distinct salted templateHashes, got A=%v B=%v", gotAnnA, gotAnnB)
	}
}

func TestStandalonePodWorkloadDeletionCleansOwnerStatesOnRecreate(t *testing.T) {
	scraper := newTestScraper(t, Config{}, nil, nil, nil)
	t0 := time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC)

	mkStandalonePod := func(ts time.Time, gated bool) *corev1.Pod {
		var gates []corev1.PodSchedulingGate
		if gated {
			gates = []corev1.PodSchedulingGate{{Name: KueueAdmissionGateName}}
		}
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "solo-worker",
				Namespace:         "ai-lab",
				CreationTimestamp: metav1.NewTime(ts),
				Labels: map[string]string{
					KueueQueueLabel:   "lq-main",
					KueueManagedLabel: "true",
				},
			},
			Spec: corev1.PodSpec{
				SchedulingGates: gates,
				Containers:      []corev1.Container{{Name: "c", Image: "pause:3.9"}},
			},
			Status: corev1.PodStatus{Phase: corev1.PodPending},
		}
	}

	wlInc0 := &kueuev1beta2.Workload{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "pod-solo-worker-inc0",
			Namespace:         "ai-lab",
			CreationTimestamp: metav1.NewTime(t0),
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: "v1", Kind: "Pod", Name: "solo-worker"},
			},
		},
		Spec: kueuev1beta2.WorkloadSpec{
			QueueName: "lq-main",
			PodSets:   []kueuev1beta2.PodSet{{Name: "main", Count: 1}},
		},
		Status: kueuev1beta2.WorkloadStatus{
			Admission: &kueuev1beta2.Admission{ClusterQueue: "cq-main"},
			Conditions: []metav1.Condition{
				{
					Type:               kueuev1beta2.WorkloadAdmitted,
					Status:             metav1.ConditionTrue,
					Reason:             "Admitted",
					LastTransitionTime: metav1.NewTime(t0.Add(time.Second)),
				},
			},
		},
	}

	// Incarnation 0: Pod created gated -> Workload admitted -> Pod deleted -> Workload deleted.
	scraper.handlePodWatchEventAt(watch.Added, mkStandalonePod(t0, true), t0, false)
	scraper.handleWorkloadWatchEventAt(watch.Added, wlInc0, t0.Add(time.Second))
	if _, exists := scraper.ownerStates["Pod/ai-lab/solo-worker"]; !exists {
		t.Fatalf("expected Pod/ai-lab/solo-worker in ownerStates while Workload is admitted")
	}
	scraper.handlePodWatchEventAt(watch.Deleted, mkStandalonePod(t0, false), t0.Add(5*time.Second), false)
	scraper.handleWorkloadWatchEventAt(watch.Deleted, wlInc0, t0.Add(5*time.Second))

	if st, leaked := scraper.ownerStates["Pod/ai-lab/solo-worker"]; leaked {
		t.Fatalf("expected ownerStates[Pod/ai-lab/solo-worker] to be deleted on Workload deletion, got leaked state %q", st)
	}
	if wl, leaked := scraper.ownerToWorkload["Pod/ai-lab/solo-worker"]; leaked {
		t.Fatalf("expected ownerToWorkload[Pod/ai-lab/solo-worker] to be deleted, got %q", wl)
	}

	// Incarnation 1: Same-name standalone Kueue Pod re-created gated -> must emit EventNew with WorkloadStateNotAdmitted!
	scraper.handlePodWatchEventAt(watch.Added, mkStandalonePod(t0.Add(8*time.Second), true), t0.Add(8*time.Second), false)

	rawWf, err := os.ReadFile(scraper.cfg.WorkflowsFile)
	if err != nil {
		t.Fatalf("read workflows.json: %v", err)
	}
	wfEvents := readJSONLines[WorkflowEvent](t, rawWf)
	var newStates []WorkloadState
	for _, ev := range wfEvents {
		if ev.EventType == EventNew && ev.Payload.ObjectKind == "Pod" && ev.Payload.Name == "solo-worker" {
			newStates = append(newStates, ev.WorkloadState)
		}
	}
	wantNewStates := []WorkloadState{WorkloadStateNotAdmitted, WorkloadStateNotAdmitted}
	if !slices.Equal(newStates, wantNewStates) {
		t.Fatalf("expected both standalone Pod incarnations to start as NotAdmitted, got %v", newStates)
	}
}

func TestSanitizeKueueConfigurationV1Beta1OmittedEnableDefaults(t *testing.T) {
	// 1. v1beta1 with omitted enable on both fairSharing and waitForPodsReady -> both disabled.
	omittedEnableYAML := `apiVersion: config.kueue.x-k8s.io/v1beta1
kind: Configuration
fairSharing:
  preemptionStrategies:
  - LessThanOrEqualToFinalShare
waitForPodsReady:
  timeout: 5m
`
	san, ok := SanitizeKueueConfigurationYAML(omittedEnableYAML)
	if !ok {
		t.Fatalf("expected v1beta1 Configuration to parse")
	}
	if _, hasFS := san["fairSharing"]; hasFS {
		t.Errorf("expected v1beta1 fairSharing without enable=true to be omitted, got %+v", san)
	}
	if _, hasWFPR := san["waitForPodsReady"]; hasWFPR {
		t.Errorf("expected v1beta1 waitForPodsReady without enable=true to be omitted, got %+v", san)
	}
	fg, _ := san["featureGates"].(map[string]any)
	if fg["DisableWaitForPodsReady"] != true {
		t.Errorf("expected v1beta1 waitForPodsReady without enable=true to set DisableWaitForPodsReady=true, got %+v", san)
	}

	// 2. v1beta1 with no waitForPodsReady block at all -> DisableWaitForPodsReady=true.
	minimalV1Beta1 := `apiVersion: config.kueue.x-k8s.io/v1beta1
kind: Configuration
`
	sanMin, ok := SanitizeKueueConfigurationYAML(minimalV1Beta1)
	if !ok {
		t.Fatalf("expected minimal v1beta1 Configuration to parse")
	}
	fgMin, _ := sanMin["featureGates"].(map[string]any)
	if fgMin["DisableWaitForPodsReady"] != true {
		t.Errorf("expected minimal v1beta1 Configuration to set DisableWaitForPodsReady=true, got %+v", sanMin)
	}

	// 3. v1beta1 with explicit enable: true -> both retained cleanly and DisableWaitForPodsReady not set.
	enabledV1Beta1 := `apiVersion: config.kueue.x-k8s.io/v1beta1
kind: Configuration
fairSharing:
  enable: true
waitForPodsReady:
  enable: true
  timeout: 5m
`
	sanEn, ok := SanitizeKueueConfigurationYAML(enabledV1Beta1)
	if !ok {
		t.Fatalf("expected enabled v1beta1 Configuration to parse")
	}
	if _, hasFS := sanEn["fairSharing"]; !hasFS {
		t.Errorf("expected fairSharing.enable=true to retain fairSharing, got %+v", sanEn)
	}
	wfprEn, _ := sanEn["waitForPodsReady"].(map[string]any)
	if wfprEn["timeout"] != "5m" {
		t.Errorf("expected waitForPodsReady.enable=true to retain timeout=5m, got %+v", sanEn)
	}
	if _, hasEnableKey := wfprEn["enable"]; hasEnableKey {
		t.Errorf("expected deprecated enable key stripped from v1beta2 output, got %+v", wfprEn)
	}
	if fgEn, _ := sanEn["featureGates"].(map[string]any); fgEn["DisableWaitForPodsReady"] == true {
		t.Errorf("did not expect DisableWaitForPodsReady=true when enable=true, got %+v", sanEn)
	}
}

func TestSyncInitialWorkloadsSkipsPreFinishedWorkloads(t *testing.T) {
	t0 := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	preFinTime := metav1.NewTime(t0.Add(-10 * time.Minute))
	admitTime := metav1.NewTime(t0.Add(-1 * time.Minute))
	ctrlTrue := true

	preFinishedWl := &kueuev1beta2.Workload{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "job-old-failed-11111",
			Namespace:         "ai-lab",
			UID:               "uid-old-finished",
			CreationTimestamp: metav1.NewTime(t0.Add(-20 * time.Minute)),
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: "batch/v1", Kind: "Job", Name: "old-failed", Controller: &ctrlTrue},
			},
		},
		Spec: kueuev1beta2.WorkloadSpec{QueueName: "lq-a"},
		Status: kueuev1beta2.WorkloadStatus{
			Admission: &kueuev1beta2.Admission{ClusterQueue: "cq-a"},
			Conditions: []metav1.Condition{
				{Type: kueuev1beta2.WorkloadAdmitted, Status: metav1.ConditionTrue, LastTransitionTime: metav1.NewTime(t0.Add(-19 * time.Minute))},
				{Type: kueuev1beta2.WorkloadFinished, Status: metav1.ConditionTrue, Reason: kueuev1beta2.WorkloadFinishedReasonFailed, LastTransitionTime: preFinTime},
			},
		},
	}
	activeWl := &kueuev1beta2.Workload{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "job-active-22222",
			Namespace:         "ai-lab",
			UID:               "uid-active",
			CreationTimestamp: admitTime,
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: "batch/v1", Kind: "Job", Name: "active-job", Controller: &ctrlTrue},
			},
		},
		Spec: kueuev1beta2.WorkloadSpec{QueueName: "lq-a"},
		Status: kueuev1beta2.WorkloadStatus{
			Admission: &kueuev1beta2.Admission{ClusterQueue: "cq-a"},
			Conditions: []metav1.Condition{
				{Type: kueuev1beta2.WorkloadAdmitted, Status: metav1.ConditionTrue, LastTransitionTime: admitTime},
			},
		},
	}

	kueueClient := kueuefake.NewSimpleClientset(preFinishedWl, activeWl)
	s := newTestScraper(t, Config{}, nil, kueueClient, nil)
	s.syncInitialWorkloads(context.Background(), t0)

	// Subsequent Modified and Deleted events for the same pre-finished UID must also be ignored.
	s.handleWorkloadWatchEventAt(watch.Modified, preFinishedWl, t0.Add(5*time.Second))
	s.handleWorkloadWatchEventAt(watch.Deleted, preFinishedWl, t0.Add(10*time.Second))

	rawExec, err := os.ReadFile(s.cfg.ExecutionTimeFile)
	if err != nil {
		t.Fatalf("read execution-time.json: %v", err)
	}
	events := readJSONLines[ExecutionTimeEvent](t, rawExec)
	if len(events) != 1 {
		t.Fatalf("expected exactly 1 execution-time event (only active workload), got %d: %+v", len(events), events)
	}
	if events[0].Payload.WorkloadName != "job-active-22222" {
		t.Fatalf("expected active workload event, got %q", events[0].Payload.WorkloadName)
	}

	// Recreating the same Workload name with a new UID must resume tracking normally.
	recreatedWl := activeWl.DeepCopy()
	recreatedWl.Name = preFinishedWl.Name
	recreatedWl.UID = "uid-recreated-new"
	s.handleWorkloadWatchEventAt(watch.Added, recreatedWl, t0.Add(15*time.Second))

	rawExec, err = os.ReadFile(s.cfg.ExecutionTimeFile)
	if err != nil {
		t.Fatalf("re-read execution-time.json: %v", err)
	}
	events = readJSONLines[ExecutionTimeEvent](t, rawExec)
	if len(events) != 2 || events[1].Payload.WorkloadName != preFinishedWl.Name {
		t.Fatalf("expected recreated workload with new UID to be tracked, got %+v", events)
	}
}

func TestResolvePodWorkloadLinkagePodGroupNameAnnotation(t *testing.T) {
	s := newTestScraper(t, Config{}, nil, nil, nil)

	// Annotation-only pod-group-name (Kueue v0.18+ standard).
	podAnn := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pg-pod-0",
			Namespace: "ai-lab",
			Labels: map[string]string{
				KueueQueueLabel:   "lq-a",
				KueueManagedLabel: "true",
			},
			Annotations: map[string]string{
				KueuePodGroupNameLabel: "pg-from-annotation",
			},
		},
	}
	s.mu.Lock()
	link := s.resolvePodWorkloadLinkageLocked(podAnn)
	s.mu.Unlock()
	if link.RawWorkloadName != "pg-from-annotation" || link.OwnerKind != "PodGroup" || link.RawOwnerName != "pg-from-annotation" {
		t.Fatalf("expected PodGroup/pg-from-annotation from annotation, got %+v", link)
	}

	// Annotation takes precedence over legacy label when both are present.
	podBoth := podAnn.DeepCopy()
	podBoth.Labels[KueuePodGroupNameLabel] = "pg-from-label"
	s.mu.Lock()
	linkBoth := s.resolvePodWorkloadLinkageLocked(podBoth)
	s.mu.Unlock()
	if linkBoth.RawWorkloadName != "pg-from-annotation" || linkBoth.RawOwnerName != "pg-from-annotation" {
		t.Fatalf("expected annotation pod-group-name to take precedence over label, got %+v", linkBoth)
	}
}

func TestDeploymentDeleteAndRelistClearsStaleOwnershipAndAllowsCleanRecreate(t *testing.T) {
	s := newTestScraper(t, Config{}, nil, nil, nil)
	t0 := time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC)

	mkDep := func(uid string, ts time.Time, ready int32) *appsv1.Deployment {
		return &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "web",
				Namespace:         "ai-lab",
				UID:               k8stypes.UID("uid-" + uid),
				CreationTimestamp: metav1.NewTime(ts),
				Labels:            map[string]string{KueueQueueLabel: "lq-main"},
			},
			Spec: appsv1.DeploymentSpec{
				Replicas: ptr.To[int32](2),
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "web"}},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "app:v1"}}},
				},
			},
			Status: appsv1.DeploymentStatus{Replicas: 2, ReadyReplicas: ready},
		}
	}

	dep0 := mkDep("inc0", t0, 2)
	s.handleDeploymentWatchEventAt(watch.Added, dep0, t0)
	s.mu.Lock()
	s.deploymentWlStates["Deployment/ai-lab/web"] = map[string]WorkloadState{"pod-web-rs0-aaa": WorkloadStateAdmitted}
	s.ownerStates["Deployment/ai-lab/web"] = WorkloadStateAdmitted
	s.ownerToWorkload["Deployment/ai-lab/web"] = "pod-web-rs0-aaa"
	s.mu.Unlock()

	// Delete Deployment while old replica Workload has not yet been GC'd.
	s.handleDeploymentWatchEventAt(watch.Deleted, dep0, t0.Add(10*time.Second))

	s.mu.Lock()
	if _, ok := s.deploymentWlStates["Deployment/ai-lab/web"]; ok {
		s.mu.Unlock()
		t.Fatalf("expected deploymentWlStates to be cleared on Deployment deletion")
	}
	if _, ok := s.ownerStates["Deployment/ai-lab/web"]; ok {
		s.mu.Unlock()
		t.Fatalf("expected ownerStates to be cleared on Deployment deletion")
	}
	s.mu.Unlock()

	// Recreate same-name Deployment with 0 ready replicas -> must emit New NotAdmitted, not stale Admitted.
	dep1 := mkDep("inc1", t0.Add(10*time.Second+100*time.Millisecond), 0)
	s.handleDeploymentWatchEventAt(watch.Added, dep1, t0.Add(10*time.Second+100*time.Millisecond))

	rawWf, err := os.ReadFile(s.cfg.WorkflowsFile)
	if err != nil {
		t.Fatalf("read workflows.json: %v", err)
	}
	evs := readJSONLines[WorkflowEvent](t, rawWf)
	var newStates []WorkloadState
	for _, ev := range evs {
		if ev.EventType == EventNew && ev.Payload.ObjectKind == "Deployment" && ev.Payload.Name == "web" {
			newStates = append(newStates, ev.WorkloadState)
		}
	}
	if len(newStates) != 2 || newStates[1] != WorkloadStateNotAdmitted {
		t.Fatalf("expected recreated Deployment to start as NotAdmitted, got newStates=%v", newStates)
	}

	// Relist deletion must also clean deploymentUIDToKey, deploymentWlStates, ownerStates, and ownerToWorkload.
	s.mu.Lock()
	s.deploymentWlStates["Deployment/ai-lab/web"] = map[string]WorkloadState{"pod-web-rs1-bbb": WorkloadStateAdmitted}
	s.ownerStates["Deployment/ai-lab/web"] = WorkloadStateAdmitted
	s.ownerToWorkload["Deployment/ai-lab/web"] = "pod-web-rs1-bbb"
	s.mu.Unlock()
	s.reconcileWorkflowDeletionsOnRelist("Deployment", map[string]bool{}, t0.Add(20*time.Second))
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.deploymentUIDToKey["uid-inc1"]; ok {
		t.Fatalf("expected relist deletion to clear deploymentUIDToKey")
	}
	if _, ok := s.deploymentWlStates["Deployment/ai-lab/web"]; ok {
		t.Fatalf("expected relist deletion to clear deploymentWlStates")
	}
	if _, ok := s.ownerStates["Deployment/ai-lab/web"]; ok {
		t.Fatalf("expected relist deletion to clear ownerStates")
	}
}

func TestDeploymentOwnershipNoPoisonOnTransientRSGetErrorAndPodTemplateHashFallback(t *testing.T) {
	t0 := time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-5bbdc7598f-ccccc",
			Namespace: "ai-lab",
			Labels:    map[string]string{"pod-template-hash": "5bbdc7598f"},
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "web-5bbdc7598f"},
			},
		},
	}
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-5bbdc7598f",
			Namespace: "ai-lab",
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: "apps/v1", Kind: "Deployment", Name: "web"},
			},
		},
	}
	kubeClient := kubefake.NewSimpleClientset(pod, rs)
	var rsGetCalls int
	kubeClient.PrependReactor("get", "replicasets", func(action k8stesting.Action) (bool, runtime.Object, error) {
		rsGetCalls++
		if rsGetCalls == 1 {
			return true, nil, errors.New("transient apiserver timeout")
		}
		return false, nil, nil
	})
	s := newTestScraper(t, Config{}, kubeClient, nil, nil)

	// First call hits transient ReplicaSets.Get error -> must NOT poison nonDeploymentPods.
	if got := s.resolveDeploymentOwnerForPodWorkload("ai-lab", pod.Name, nil); got != "" {
		t.Fatalf("expected empty owner on transient RS error, got %q", got)
	}
	s.mu.Lock()
	poisoned := s.nonDeploymentPods["ai-lab/"+pod.Name]
	s.mu.Unlock()
	if poisoned {
		t.Fatalf("expected transient RS Get error NOT to poison nonDeploymentPods")
	}

	// Second call retries ReplicaSets.Get after API recovers and resolves Deployment/web.
	if got := s.resolveDeploymentOwnerForPodWorkload("ai-lab", pod.Name, nil); got != "web" {
		t.Fatalf("expected retry to resolve Deployment 'web', got %q (rsGetCalls=%d)", got, rsGetCalls)
	}
	if rsGetCalls != 2 {
		t.Fatalf("expected 2 ReplicaSets.Get calls across transient error + retry, got %d", rsGetCalls)
	}

	// Already-deleted RS (NotFound) falls back to pod-template-hash against tracked Deployment.
	podGoneRS := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "api-7f8b9c-ddddd",
			Namespace: "ai-lab",
			Labels:    map[string]string{"pod-template-hash": "7f8b9c"},
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "api-7f8b9c"},
			},
		},
	}
	kubeClient2 := kubefake.NewSimpleClientset(podGoneRS)
	s2 := newTestScraper(t, Config{}, kubeClient2, nil, nil)
	s2.handleDeploymentWatchEventAt(watch.Added, &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "api",
			Namespace:         "ai-lab",
			CreationTimestamp: metav1.NewTime(t0),
			Labels:            map[string]string{KueueQueueLabel: "lq-main"},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To[int32](1),
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "pause:3.9"}}}},
		},
	}, t0)
	if got := s2.resolveDeploymentOwnerForPodWorkload("ai-lab", podGoneRS.Name, nil); got != "api" {
		t.Fatalf("expected pod-template-hash fallback to resolve Deployment 'api' when RS is NotFound, got %q", got)
	}
}

func TestSanitizeDeploymentStripsRevisionAnnotationAndManageWithoutQueueName(t *testing.T) {
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web",
			Namespace: "ai-lab",
			Annotations: map[string]string{
				"deployment.kubernetes.io/revision": "3",
				"kueue.x-k8s.io/custom":             "keep",
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To[int32](1),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "pause:3.9"}}},
			},
		},
	}
	san := SanitizeDeployment(deploy, "pause:3.9", NoopHasher{})
	meta, _ := san["metadata"].(map[string]any)
	anns, _ := meta["annotations"].(map[string]any)
	if _, hasRev := anns["deployment.kubernetes.io/revision"]; hasRev {
		t.Fatalf("expected deployment.kubernetes.io/revision to be stripped, got annotations=%+v", anns)
	}
	if anns["kueue.x-k8s.io/custom"] != "keep" {
		t.Fatalf("expected non-denied kueue annotation to be preserved, got %+v", anns)
	}

	s := newTestScraper(t, Config{}, nil, nil, nil)
	if !s.shouldSkipDeployment(deploy) {
		t.Fatalf("expected Deployment without queue label to be skipped when manageJobsWithoutQueueName=false")
	}
	s.mu.Lock()
	s.manageJobsWithoutQueueName = true
	s.mu.Unlock()
	if s.shouldSkipDeployment(deploy) {
		t.Fatalf("expected non-system Deployment without queue label to be tracked when manageJobsWithoutQueueName=true")
	}
	sysDeploy := deploy.DeepCopy()
	sysDeploy.Namespace = "kube-system"
	if !s.shouldSkipDeployment(sysDeploy) {
		t.Fatalf("expected kube-system Deployment to stay skipped even when manageJobsWithoutQueueName=true")
	}
}

func TestDeploymentTrailingWorkloadEventsDoNotReviveDeletedIncarnationState(t *testing.T) {
	s := newTestScraper(t, Config{}, nil, nil, nil)
	t0 := time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC)
	const oKey = "Deployment/ai-lab/web"

	mkDep := func(uid string, created time.Time, ready int32) *appsv1.Deployment {
		return &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "web",
				Namespace:         "ai-lab",
				UID:               k8stypes.UID(uid),
				CreationTimestamp: metav1.NewTime(created),
				Labels:            map[string]string{KueueQueueLabel: "lq-main"},
			},
			Spec: appsv1.DeploymentSpec{
				Replicas: ptr.To[int32](1),
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "web"}},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "app:v1"}}},
				},
			},
			Status: appsv1.DeploymentStatus{Replicas: 1, ReadyReplicas: ready},
		}
	}
	// Both incarnations use the same pod template, so both get the ReplicaSet name "web-7d9f".
	mkWl := func(podSuffix string, created time.Time, ownerNotFound bool) *kueuev1beta2.Workload {
		conds := []metav1.Condition{{
			Type:               kueuev1beta2.WorkloadAdmitted,
			Status:             metav1.ConditionTrue,
			Reason:             "Admitted",
			LastTransitionTime: metav1.NewTime(created),
		}}
		if ownerNotFound {
			conds = append(conds, metav1.Condition{
				Type:               kueuev1beta2.WorkloadFinished,
				Status:             metav1.ConditionTrue,
				Reason:             kueuev1beta2.WorkloadFinishedReasonOwnerNotFound,
				LastTransitionTime: metav1.NewTime(created.Add(11 * time.Second)),
			})
		}
		return &kueuev1beta2.Workload{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "pod-web-7d9f-" + podSuffix,
				Namespace:         "ai-lab",
				CreationTimestamp: metav1.NewTime(created),
				OwnerReferences:   []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: "web-7d9f-" + podSuffix}},
			},
			Spec: kueuev1beta2.WorkloadSpec{
				QueueName: "lq-main",
				PodSets:   []kueuev1beta2.PodSet{{Name: "main", Count: 1}},
			},
			Status: kueuev1beta2.WorkloadStatus{
				Admission:  &kueuev1beta2.Admission{ClusterQueue: "cq-main"},
				Conditions: conds,
			},
		}
	}
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "web-7d9f",
			Namespace:       "ai-lab",
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: "web"}},
		},
	}
	ownerState := func() (WorkloadState, bool) {
		s.mu.Lock()
		defer s.mu.Unlock()
		st, ok := s.ownerStates[oKey]
		return st, ok
	}

	// Incarnation 0 runs one admitted replica.
	s.handleDeploymentWatchEventAt(watch.Added, mkDep("uid-inc0", t0, 1), t0)
	s.handleReplicaSetWatchEvent(watch.Added, rs)
	s.handleWorkloadWatchEventAt(watch.Added, mkWl("aaaaa", t0.Add(time.Second), false), t0.Add(time.Second))
	if st, ok := ownerState(); !ok || st != WorkloadStateAdmitted {
		t.Fatalf("expected incarnation 0 owner state Admitted, got %q (present=%v)", st, ok)
	}

	// Kubernetes deletes the Deployment before its ReplicaSet, Pod and Workload.
	delAt := t0.Add(10 * time.Second)
	s.handleDeploymentWatchEventAt(watch.Deleted, mkDep("uid-inc0", t0, 1), delAt)

	// 1. A trailing event of the old Workload (still Admitted=True) must not bring the owner state back.
	s.handleWorkloadWatchEventAt(watch.Modified, mkWl("aaaaa", t0.Add(time.Second), false), delAt.Add(200*time.Millisecond))
	if st, ok := ownerState(); ok {
		t.Fatalf("trailing Workload event revived ownerStates[%s]=%q after Deployment deletion", oKey, st)
	}
	s.mu.Lock()
	_, wlMapRevived := s.deploymentWlStates[oKey]
	_, ownerWlRevived := s.ownerToWorkload[oKey]
	s.mu.Unlock()
	if wlMapRevived || ownerWlRevived {
		t.Fatalf("trailing Workload event revived state: deploymentWlStates=%v ownerToWorkload=%v", wlMapRevived, ownerWlRevived)
	}

	// 2. A Deployment with the same name starts with 0 ready replicas: it is NotAdmitted.
	recreatedAt := t0.Add(12 * time.Second)
	s.handleDeploymentWatchEventAt(watch.Added, mkDep("uid-inc1", recreatedAt, 0), recreatedAt)

	// 3. The dying incarnation-0 Workload still reports Admitted=True, then Finished (OwnerNotFound).
	//    Neither event may mark incarnation 1 as Admitted.
	s.handleWorkloadWatchEventAt(watch.Modified, mkWl("aaaaa", t0.Add(time.Second), false), recreatedAt.Add(100*time.Millisecond))
	s.handleWorkloadWatchEventAt(watch.Modified, mkWl("aaaaa", t0.Add(time.Second), true), recreatedAt.Add(200*time.Millisecond))
	if st, ok := ownerState(); ok {
		t.Fatalf("incarnation-0 Workload leaked into incarnation 1: ownerStates[%s]=%q", oKey, st)
	}

	// 4. A Workload of incarnation 1 sets the owner state as before.
	s.handleWorkloadWatchEventAt(watch.Added, mkWl("bbbbb", recreatedAt.Add(time.Second), false), recreatedAt.Add(time.Second))
	if st, ok := ownerState(); !ok || st != WorkloadStateAdmitted {
		t.Fatalf("expected incarnation 1 owner state Admitted from its own Workload, got %q (present=%v)", st, ok)
	}
	s.mu.Lock()
	wlMap := s.deploymentWlStates[oKey]
	s.mu.Unlock()
	if len(wlMap) != 1 || wlMap["pod-web-7d9f-bbbbb"] != WorkloadStateAdmitted {
		t.Fatalf("expected only the incarnation-1 Workload in deploymentWlStates, got %v", wlMap)
	}

	rawWf, err := os.ReadFile(s.cfg.WorkflowsFile)
	if err != nil {
		t.Fatalf("read workflows.json: %v", err)
	}
	var newStates []WorkloadState
	for _, ev := range readJSONLines[WorkflowEvent](t, rawWf) {
		if ev.EventType == EventNew && ev.Payload.ObjectKind == "Deployment" && ev.Payload.Name == "web" {
			newStates = append(newStates, ev.WorkloadState)
		}
	}
	if len(newStates) != 2 || newStates[1] != WorkloadStateNotAdmitted {
		t.Fatalf("expected recreated Deployment to start as NotAdmitted, got New states %v", newStates)
	}
}

func TestUnlabeledDeploymentPendingStateWithManageJobsWithoutQueueName(t *testing.T) {
	t0 := time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC)
	mkDep := func(ns string) *appsv1.Deployment {
		return &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: ns, UID: "uid-web", CreationTimestamp: metav1.NewTime(t0)},
			Spec: appsv1.DeploymentSpec{
				Replicas: ptr.To[int32](2),
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "app:v1"}}},
				},
			},
			// The gated pods exist but none is ready, and no Workload was seen yet.
			Status: appsv1.DeploymentStatus{Replicas: 2, ReadyReplicas: 0},
		}
	}
	newState := func(s *Scraper) WorkloadState {
		t.Helper()
		rawWf, err := os.ReadFile(s.cfg.WorkflowsFile)
		if err != nil {
			t.Fatalf("read workflows.json: %v", err)
		}
		evs := readJSONLines[WorkflowEvent](t, rawWf)
		if len(evs) != 1 || evs[0].EventType != EventNew {
			t.Fatalf("expected exactly one New Deployment event, got %+v", evs)
		}
		return evs[0].WorkloadState
	}

	s := newTestScraper(t, Config{}, nil, nil, nil)
	s.mu.Lock()
	s.manageJobsWithoutQueueName = true
	s.mu.Unlock()
	s.handleDeploymentWatchEventAt(watch.Added, mkDep("ai-lab"), t0)
	if got := newState(s); got != WorkloadStateNotAdmitted {
		t.Fatalf("expected unlabeled pending Deployment under manageJobsWithoutQueueName to be NotAdmitted, got %q", got)
	}
}
