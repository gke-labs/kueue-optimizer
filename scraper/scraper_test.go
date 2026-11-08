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

func TestAnnotationFilteringAndPodSanitization(t *testing.T) {
	ann := map[string]string{
		"business.corp.com/cost-center":                     "secret-finance-code",
		"team.internal/owner":                               "alice",
		"kueue.x-k8s.io/queue-name":                         "main-lq",
		"kueue-optimizer.gke.io/pod-execution-time-seconds": "15",
		"kubernetes.io/psp":                                 "restricted",
		"k8s.io/custom":                                     "yes",
		"kueue.x-k8s.io/job-owner-name":                     "my-secret-job",
	}

	filtered := FilterAnnotations(ann, true)
	if _, exists := filtered["business.corp.com/cost-center"]; exists {
		t.Errorf("expected business annotation to be stripped")
	}
	if _, exists := filtered["team.internal/owner"]; exists {
		t.Errorf("expected internal annotation to be stripped")
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
	if filtered["kueue.x-k8s.io/job-owner-name"] != HashName("my-secret-job", true) {
		t.Errorf("expected job-owner-name annotation value to be SHA-256 hashed")
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

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "kwok-node-0"},
		Spec:       corev1.NodeSpec{PodCIDR: "10.0.0.0/24"},
		Status: corev1.NodeStatus{
			Capacity: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("8"),
				corev1.ResourceMemory: resource.MustParse("32Gi"),
			},
		},
	}
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "ai-lab"},
		Status:     corev1.NamespaceStatus{Phase: corev1.NamespaceActive},
	}
	runningPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "train-pod-0",
			Namespace: "ai-lab",
			Labels:    map[string]string{"secret-label": "do-not-save"},
			Annotations: map[string]string{
				"secret-annotation": "do-not-save",
			},
		},
		Spec: corev1.PodSpec{
			NodeName:         "kwok-node-0",
			ImagePullSecrets: []corev1.LocalObjectReference{{Name: "regcred"}},
			Volumes:          []corev1.Volume{{Name: "secret-vol"}},
			Containers: []corev1.Container{
				{
					Name:    "trainer",
					Image:   "gcr.io/proprietary/model:v1",
					Command: []string{"python", "train.py"},
					Args:    []string{"--lr=0.001"},
					Env:     []corev1.EnvVar{{Name: "API_KEY", Value: "top-secret"}},
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
		ObjectMeta: metav1.ObjectMeta{Name: "ai-lab-cq"},
		Spec: kueuev1beta2.ClusterQueueSpec{
			CohortName: "main-cohort",
		},
		Status: kueuev1beta2.ClusterQueueStatus{
			AdmittedWorkloads: 5,
		},
	}
	lq := &kueuev1beta2.LocalQueue{
		ObjectMeta: metav1.ObjectMeta{Name: "ai-lab-lq", Namespace: "ai-lab"},
		Spec: kueuev1beta2.LocalQueueSpec{
			ClusterQueue: "ai-lab-cq",
		},
		Status: kueuev1beta2.LocalQueueStatus{
			AdmittedWorkloads: 5,
		},
	}
	rf := &kueuev1beta2.ResourceFlavor{
		ObjectMeta: metav1.ObjectMeta{Name: "default-flavor"},
		Spec: kueuev1beta2.ResourceFlavorSpec{
			NodeLabels: map[string]string{"instance": "gpu"},
		},
	}
	topo := &kueuev1beta2.Topology{
		ObjectMeta: metav1.ObjectMeta{Name: "default-topology"},
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
			expectedHash := HashName("train-pod-0", true)
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

	// Ensure no secret strings leaked anywhere into infra.json.
	infraStr := string(rawInfra)
	for _, secret := range []string{"do-not-save", "gcr.io/proprietary", "train.py", "--lr=0.001", "top-secret", "regcred", "secret-vol", "train-pod-0"} {
		if strings.Contains(infraStr, secret) {
			t.Errorf("infra.json leaked sensitive pod information %q:\n%s", secret, infraStr)
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
				"corp.internal/secret-note":                         "proprietary-dataset-path",
			},
			Labels: map[string]string{
				KueueQueueLabel: "team-a-lq",
			},
		},
		Spec: corev1.PodSpec{
			Volumes:          []corev1.Volume{{Name: "data-vol"}},
			ImagePullSecrets: []corev1.LocalObjectReference{{Name: "pull-secret"}},
			Containers: []corev1.Container{
				{
					Name:         "worker",
					Image:        "registry.internal/secret-llm:v9",
					Command:      []string{"/bin/run_secret_training"},
					Args:         []string{"--weights=/mnt/secret"},
					Env:          []corev1.EnvVar{{Name: "SECRET_TOKEN", Value: "xyz123"}},
					EnvFrom:      []corev1.EnvFromSource{{Prefix: "SEC_"}},
					VolumeMounts: []corev1.VolumeMount{{Name: "data-vol", MountPath: "/mnt"}},
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
			Name:              "my-batch-job",
			Namespace:         "ai-lab",
			CreationTimestamp: createTime,
			Labels:            map[string]string{KueueQueueLabel: "team-a-lq"},
			Annotations: map[string]string{
				"business.org/project": "confidential-project",
				"k8s.io/keep-me":       "true",
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
			Name:              "my-jobset",
			Namespace:         "ai-lab",
			CreationTimestamp: createTime,
			Labels:            map[string]string{KueueQueueLabel: "team-a-lq"},
			Annotations: map[string]string{
				"business.org/owner":                 "secret-owner",
				"kueue-optimizer.gke.io/pod-count":   "4",
				"kueue.x-k8s.io/workload-slice-name": "my-jobset",
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
								"business.corp/custom": "strip-me",
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
			Name:              "my-lws",
			Namespace:         "ai-lab",
			CreationTimestamp: createTime,
			Labels:            map[string]string{KueueQueueLabel: "team-a-lq"},
		},
		Spec: leaderworkersetv1.LeaderWorkerSetSpec{
			Replicas: ptr.To[int32](2),
			LeaderWorkerTemplate: leaderworkersetv1.LeaderWorkerTemplate{
				LeaderTemplate:       secretPodTemplate.DeepCopy(),
				WorkerTemplate:       secretPodTemplate,
				VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: "pvc"}}},
			},
		},
	}
	scraper.handleLeaderWorkerSetWatchEvent(watch.Added, lws)

	// 4. Deployment
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "my-deployment",
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
			Name:              "my-rayjob",
			Namespace:         "ai-lab",
			CreationTimestamp: createTime,
			Labels:            map[string]string{KueueQueueLabel: "team-a-lq"},
		},
		Spec: rayv1.RayJobSpec{
			Entrypoint:           "python secret_entrypoint.py",
			RuntimeEnvYAML:       "pip: [secret-pkg]",
			SubmitterPodTemplate: secretPodTemplate.DeepCopy(),
			RayClusterSpec: &rayv1.RayClusterSpec{
				HeadGroupSpec: rayv1.HeadGroupSpec{
					RayStartParams: map[string]string{"secret-param": "1"},
					Template:       secretPodTemplate,
				},
			},
		},
	}
	scraper.handleRayJobWatchEvent(watch.Added, rj)

	// 6. RayCluster
	rc := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "my-raycluster",
			Namespace:         "ai-lab",
			CreationTimestamp: createTime,
			Labels:            map[string]string{KueueQueueLabel: "team-a-lq"},
		},
		Spec: rayv1.RayClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{
				RayStartParams: map[string]string{"redis-password": "secret"},
				Template:       secretPodTemplate,
			},
			WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
				{
					GroupName:      "gpu-group",
					RayStartParams: map[string]string{"num-gpus": "8"},
					Template:       secretPodTemplate,
				},
			},
		},
	}
	scraper.handleRayClusterWatchEvent(watch.Added, rc)

	// 7. Standalone Kueue-managed Pod (should be recorded) vs Job-owned Pod (should NOT be recorded in workflows.json)
	standalonePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "standalone-kueue-pod",
			Namespace:         "ai-lab",
			CreationTimestamp: createTime,
			Labels:            map[string]string{KueueQueueLabel: "team-a-lq"},
			Annotations:       secretPodTemplate.Annotations,
		},
		Spec: secretPodTemplate.Spec,
	}
	scraper.handlePodWatchEvent(watch.Added, standalonePod)

	jobOwnedPod := standalonePod.DeepCopy()
	jobOwnedPod.Name = "job-owned-pod"
	jobOwnedPod.OwnerReferences = []metav1.OwnerReference{
		{APIVersion: "batch/v1", Kind: "Job", Name: "my-batch-job"},
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
	// Verify all business-specific details and unhashed names are completely absent.
	forbiddenStrings := []string{
		"registry.internal/secret-llm:v9",
		"/bin/run_secret_training",
		"--weights=/mnt/secret",
		"SECRET_TOKEN",
		"xyz123",
		"SEC_",
		"data-vol",
		"pull-secret",
		"proprietary-dataset-path",
		"confidential-project",
		"secret-owner",
		"strip-me",
		"secret_entrypoint.py",
		"secret-pkg",
		"redis-password",
		"my-batch-job",
		"my-jobset",
		"my-lws",
		"my-deployment",
		"my-rayjob",
		"my-raycluster",
		"standalone-kueue-pod",
		"job-owned-pod",
	}
	for _, s := range forbiddenStrings {
		if strings.Contains(wfStr, s) {
			t.Errorf("workflows.json leaked forbidden string %q:\n%s", s, wfStr)
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
