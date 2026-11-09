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
	"encoding/json"
	"errors"
	"flag"
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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"
	jobsetfake "sigs.k8s.io/jobset/client-go/clientset/versioned/fake"
	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"
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
		"ClusterQueues",
		"LocalQueues",
		"ResourceFlavors",
		"Topologies",
		"Scheduled & Running Pods",
		"Information Omitted / Sanitized",
		"SHA-256 Name Replacement",
		"Enabled                            : true",
		"Workloads, Pods, Jobs, JobSets, LeaderWorkerSets, Deployments, RayJobs, RayClusters",
		"Namespaces, Nodes, ClusterQueues, LocalQueues, ResourceFlavors, Topologies",
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
	}
	sanitized := SanitizeNode(node)

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
	for _, k := range deniedNodeLabelKeys {
		if k != strings.ToLower(k) {
			t.Errorf("deniedNodeLabelKeys entry %q must be lowercase", k)
		}
	}
	for _, k := range deniedAnnotationKeys {
		if k != strings.ToLower(k) {
			t.Errorf("deniedAnnotationKeys entry %q must be lowercase", k)
		}
	}
	for _, d := range allowedAnnotationDomains {
		if d != strings.ToLower(d) {
			t.Errorf("allowedAnnotationDomains entry %q must be lowercase", d)
		}
	}
}

func TestFilterAnnotations(t *testing.T) {
	secretJobName := testSecretNamePrefix + "job"
	ann := map[string]string{
		"business.corp.com/cost-center":                     testSecretValue,
		"team.internal/owner":                               testSecretEmail,
		"k8s.client-domain.com/owner":                       testSecretEmail,
		"kubernetes.client-domain.com/team":                 testSecretEmail,
		"client-domain.com/k8s-owner":                       testSecretEmail,
		"k8s-unprefixed-owner":                              testSecretEmail,
		corev1.LastAppliedConfigAnnotation:                  `{"image":"` + testSecretImage + `","secret":"` + testSecretValue + `"}`,
		"kubernetes.io/change-cause":                        "kubectl set image job/" + secretJobName + " main=" + testSecretImage + " --record",
		"kueue.x-k8s.io/queue-name":                         "main-lq",
		"kueue-optimizer.gke.io/pod-execution-time-seconds": "15",
		"kubernetes.io/psp":                                 "restricted",
		"k8s.io/custom":                                     "yes",
		"kueue.x-k8s.io/job-owner-name":                     secretJobName,
	}

	filtered := FilterAnnotations(ann, true)
	for _, strippedKey := range []string{
		"business.corp.com/cost-center",
		"team.internal/owner",
		"k8s.client-domain.com/owner",
		"kubernetes.client-domain.com/team",
		"client-domain.com/k8s-owner",
		"k8s-unprefixed-owner",
		corev1.LastAppliedConfigAnnotation,
		"kubernetes.io/change-cause",
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
	if filtered["kueue.x-k8s.io/job-owner-name"] != HashName(secretJobName, true) {
		t.Errorf("expected job-owner-name annotation value to be SHA-256 hashed")
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

	SanitizePodSpec(spec, DefaultPauseImage)

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

func TestInfraScrapeAndPodWatchEvents(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := Config{
		Interval:          60 * time.Second,
		HashNames:         true,
		PauseImage:        DefaultPauseImage,
		InfraFile:         filepath.Join(tmpDir, "infra.json"),
		WorkflowsFile:     filepath.Join(tmpDir, "workflows.json"),
		ExecutionTimeFile: filepath.Join(tmpDir, "execution-time.json"),
	}

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
			Labels: map[string]string{"team": "ai"},
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

	kubeClient := kubefake.NewSimpleClientset(node, ns, runningPod)
	kueueClient := kueuefake.NewSimpleClientset(cq, lq, rf, topo)
	jobsetClient := jobsetfake.NewSimpleClientset()

	infraLog, err := NewJSONEventLog(cfg.InfraFile)
	if err != nil {
		t.Fatalf("create infraLog: %v", err)
	}
	defer func() { _ = infraLog.Close() }()

	workflowsLog, err := NewJSONEventLog(cfg.WorkflowsFile)
	if err != nil {
		t.Fatalf("create workflowsLog: %v", err)
	}
	defer func() { _ = workflowsLog.Close() }()

	execTimeLog, err := NewJSONEventLog(cfg.ExecutionTimeFile)
	if err != nil {
		t.Fatalf("create execTimeLog: %v", err)
	}
	defer func() { _ = execTimeLog.Close() }()

	scraper := NewScraper(cfg, kubeClient, kueueClient, jobsetClient, nil, infraLog, workflowsLog, execTimeLog)

	ctx := context.Background()
	scraper.ScrapeOnce(ctx)

	// Simulate watch deletion of the running pod.
	scraper.handlePodWatchEvent(watch.Deleted, runningPod)

	rawInfra, err := os.ReadFile(cfg.InfraFile)
	if err != nil {
		t.Fatalf("read infra.json: %v", err)
	}
	events := readJSONLines[map[string]any](t, rawInfra)

	if len(events) != 8 {
		t.Fatalf("expected 8 infra events (6 infra objects + 1 pod new + 1 pod deleted), got %d", len(events))
	}

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

		switch kind {
		case "Node":
			meta := payload["metadata"].(map[string]any)
			name := meta["name"].(string)
			if name != "kwok-node-0" {
				t.Errorf("Node name must not be hashed, got %q", name)
			}
			if _, hasStatus := payload["status"]; !hasStatus {
				t.Errorf("Node payload must include status")
			}
		case "Namespace", "ClusterQueue", "LocalQueue", "ResourceFlavor", "Topology":
			meta := payload["metadata"].(map[string]any)
			name := meta["name"].(string)
			if _, hasStatus := payload["status"]; hasStatus {
				t.Errorf("%s payload must NOT include status", kind)
			}
			if len(name) == 64 {
				t.Errorf("%s name must not be SHA-256 hashed, got %q", kind, name)
			}
		case "Pod":
			name := payload["name"].(string)
			expectedHash := HashName(rawPodName, true)
			if name != expectedHash {
				t.Errorf("Pod name should be SHA-256 hashed to %q, got %q", expectedHash, name)
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

	// Ensure scheduling-relevant Node/Namespace fields and allowed annotations WERE preserved in infra.json.
	for _, expected := range []string{
		"kwok.x-k8s.io/node",
		"nvidia-tesla-a100",
		"KubeletReady",
		"keep-ns-annot",
		"keep-cq-annot",
		`"team":"ai"`,
	} {
		if !strings.Contains(infraStr, expected) {
			t.Errorf("infra.json missing expected sanitized field/value %q:\n%s", expected, infraStr)
		}
	}
}

func TestWorkflowsSanitizationAndRecording(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := Config{
		Interval:          60 * time.Second,
		HashNames:         true,
		PauseImage:        DefaultPauseImage,
		InfraFile:         filepath.Join(tmpDir, "infra.json"),
		WorkflowsFile:     filepath.Join(tmpDir, "workflows.json"),
		ExecutionTimeFile: filepath.Join(tmpDir, "execution-time.json"),
	}

	infraLog, _ := NewJSONEventLog(cfg.InfraFile)
	defer func() { _ = infraLog.Close() }()
	workflowsLog, _ := NewJSONEventLog(cfg.WorkflowsFile)
	defer func() { _ = workflowsLog.Close() }()
	execTimeLog, _ := NewJSONEventLog(cfg.ExecutionTimeFile)
	defer func() { _ = execTimeLog.Close() }()

	scheme := runtime.NewScheme()
	_ = leaderworkersetv1.AddToScheme(scheme)
	_ = rayv1.AddToScheme(scheme)
	dynClient := dynamicfake.NewSimpleDynamicClient(scheme)

	scraper := NewScraper(
		cfg,
		kubefake.NewSimpleClientset(),
		kueuefake.NewSimpleClientset(),
		jobsetfake.NewSimpleClientset(),
		dynClient,
		infraLog,
		workflowsLog,
		execTimeLog,
	)

	createTime := metav1.NewTime(time.Date(2026, 10, 30, 10, 5, 0, 0, time.UTC))

	secretPodTemplate := corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				"kueue-optimizer.gke.io/pod-execution-time-seconds": "20",
				"corp.internal/secret-note":                         testSecretValue,
			},
			Labels: map[string]string{
				KueueQueueLabel: "team-a-lq",
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
				"business.org/owner":                 testSecretEmail,
				"kueue-optimizer.gke.io/pod-count":   "4",
				"kueue.x-k8s.io/workload-slice-name": testSecretNamePrefix + "jobset",
			},
		},
		Spec: jobsetv1alpha2.JobSetSpec{
			VolumeClaimPolicies: []jobsetv1alpha2.VolumeClaimPolicy{{}},
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
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:              testSecretNamePrefix + "deployment",
			Namespace:         "ai-lab",
			CreationTimestamp: createTime,
			Labels:            map[string]string{KueueQueueLabel: "team-a-lq"},
		},
		Spec: appsv1.DeploymentSpec{
			Template: secretPodTemplate,
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
			SubmitterPodTemplate: secretPodTemplate.DeepCopy(),
			RayClusterSpec: &rayv1.RayClusterSpec{
				HeadGroupSpec: rayv1.HeadGroupSpec{
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

	// 6. RayCluster
	rc := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:              testSecretNamePrefix + "raycluster",
			Namespace:         "ai-lab",
			CreationTimestamp: createTime,
			Labels:            map[string]string{KueueQueueLabel: "team-a-lq"},
		},
		Spec: rayv1.RayClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{
				RayStartParams: map[string]string{"redis-password": testSecretValue},
				Template:       secretPodTemplate,
			},
			WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
				{
					GroupName:      "gpu-group",
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

	rawWorkflows, err := os.ReadFile(cfg.WorkflowsFile)
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
		if len(ev.Payload.Name) != 64 {
			t.Errorf("%s expected 64-char SHA-256 hashed name, got %q", ev.Payload.ObjectKind, ev.Payload.Name)
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
	} {
		if !strings.Contains(wfStr, expected) {
			t.Errorf("workflows.json missing expected sanitized value %q:\n%s", expected, wfStr)
		}
	}
}

func TestWorkloadExecutionTimeTracking(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := Config{
		Interval:          60 * time.Second,
		HashNames:         true,
		PauseImage:        DefaultPauseImage,
		InfraFile:         filepath.Join(tmpDir, "infra.json"),
		WorkflowsFile:     filepath.Join(tmpDir, "workflows.json"),
		ExecutionTimeFile: filepath.Join(tmpDir, "execution-time.json"),
	}

	infraLog, _ := NewJSONEventLog(cfg.InfraFile)
	defer func() { _ = infraLog.Close() }()
	workflowsLog, _ := NewJSONEventLog(cfg.WorkflowsFile)
	defer func() { _ = workflowsLog.Close() }()
	execTimeLog, _ := NewJSONEventLog(cfg.ExecutionTimeFile)
	defer func() { _ = execTimeLog.Close() }()

	scraper := NewScraper(
		cfg,
		kubefake.NewSimpleClientset(),
		kueuefake.NewSimpleClientset(),
		jobsetfake.NewSimpleClientset(),
		nil,
		infraLog,
		workflowsLog,
		execTimeLog,
	)

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

	rawExec, err := os.ReadFile(cfg.ExecutionTimeFile)
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
		if ev.Payload.WorkloadName != HashName("jobset-train-01-wl", true) {
			t.Errorf("expected hashed workloadName, got %q", ev.Payload.WorkloadName)
		}
		if ev.Payload.OwnerName != HashName("jobset-train-01", true) {
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

	rawWf, err := os.ReadFile(cfg.WorkflowsFile)
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
		"--qps", "150",
		"--burst", "300",
	}, &errBuf)
	if err != nil {
		t.Fatalf("parseFlags returned unexpected error: %v", err)
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

	payloadV1 := SanitizeNode(nodeV1)
	payloadV2 := SanitizeNode(nodeV2)

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
	digestV3 := canonicalDigest(SanitizeNode(nodeV3))
	if digestV3 == digestV1 {
		t.Errorf("expected digest to change when metadata.labels changes")
	}
}
