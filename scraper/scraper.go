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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"
	jobsetclientset "sigs.k8s.io/jobset/client-go/clientset/versioned"
	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	kueueclientset "sigs.k8s.io/kueue/client-go/clientset/versioned"
	leaderworkersetv1 "sigs.k8s.io/lws/api/leaderworkerset/v1"
	"sigs.k8s.io/yaml"
)

var (
	lwsGVR = schema.GroupVersionResource{
		Group:    leaderworkersetv1.GroupVersion.Group,
		Version:  leaderworkersetv1.GroupVersion.Version,
		Resource: "leaderworkersets",
	}
	rayJobGVR = schema.GroupVersionResource{
		Group:    rayv1.GroupVersion.Group,
		Version:  rayv1.GroupVersion.Version,
		Resource: "rayjobs",
	}
	rayClusterGVR = schema.GroupVersionResource{
		Group:    rayv1.GroupVersion.Group,
		Version:  rayv1.GroupVersion.Version,
		Resource: "rayclusters",
	}
)

type cachedObject struct {
	name      string
	namespace string
	digest    string
	payload   any
}

type trackedPodState struct {
	firstNoticeTime time.Time
	lastPayload     ScrapedPodPayload
	lastDigest      string
	emittedNew      bool
	deleted         bool
}

type trackedWorkflowState struct {
	kind           string
	name           string
	namespace      string
	creationTime   metav1.Time
	localQueueName string
	workloadState  WorkloadState
	lastDigest     string
	lastPayload    any
}

type workloadExecState struct {
	currentlyAdmitted         bool
	currentAdmittedTime       time.Time
	lastAdmittedTime          time.Time
	lastRecordedAdmittedTime  time.Time
	preemptionCount           int
	interruptionCount         int
	recordedPartialKeys       map[string]bool
	recordedFull              bool
	recordedTerminalFailure   bool
	clusterQueueName          string
	workloadName              string
	namespace                 string
	ownerKind                 string
	ownerName                 string
	ownerAPIVersion           string
	localQueueName            string
	priorityClassName         string
	priority                  *int32
	podSetAssignments         []kueuev1beta2.PodSetAssignment
	unhealthyNodes            []string
	creationTime              metav1.Time
	trueDurationSeconds       *float64
	missingDurationMultiplier *float64
}

type workloadOwnerRef struct {
	kind string
	name string
}

// Scraper coordinates periodic infrastructure/pod scraping and real-time watches for
// pods, workflows, and Kueue workload execution times.
type Scraper struct {
	cfg           Config
	hasher        SensitiveDataHasher
	kubeClient    kubernetes.Interface
	kueueClient   kueueclientset.Interface
	jobsetClient  jobsetclientset.Interface
	dynamicClient dynamic.Interface

	infraLog     *JSONEventLog
	workflowsLog *JSONEventLog
	execTimeLog  *JSONEventLog

	mu                     sync.Mutex
	infraCache             map[string]map[string]cachedObject // kind -> key -> cachedObject
	podStates              map[string]*trackedPodState        // namespace/rawName -> trackedPodState
	workflowStates         map[string]*trackedWorkflowState   // kind/namespace/rawName -> trackedWorkflowState
	ownerStates            map[string]WorkloadState           // kind/namespace/rawName -> WorkloadState from Kueue Workload
	ownerToWorkload        map[string]string                  // kind/namespace/rawOwnerName -> rawWorkloadName
	workloadToOwner        map[string]workloadOwnerRef        // namespace/rawWorkloadName -> raw owner
	workloadIsTAS          map[string]bool                    // namespace/rawWorkloadName -> uses TAS
	workloadPodSets        map[string][]string                // namespace/rawWorkloadName -> podSet names
	jobSetQueues           map[string]string                  // namespace/rawJobSetName -> localQueueName
	jobToJobSet            map[string]string                  // namespace/rawJobName -> rawJobSetName
	rayClusterToRayJob     map[string]string                  // namespace/rawRayClusterName -> rawRayJobName
	replicaSetToDeployment     map[string]string                  // namespace/rawReplicaSetName -> rawDeploymentName
	workloadStates             map[string]*workloadExecState      // namespace/rawWorkloadName -> workloadExecState
	manageJobsWithoutQueueName bool                               // cached from Kueue Configuration
}

// NewScraper initializes a new Scraper with the given clients and configuration.
func NewScraper(
	cfg Config,
	kubeClient kubernetes.Interface,
	kueueClient kueueclientset.Interface,
	jobsetClient jobsetclientset.Interface,
	dynamicClient dynamic.Interface,
	infraLog *JSONEventLog,
	workflowsLog *JSONEventLog,
	execTimeLog *JSONEventLog,
) *Scraper {
	if cfg.PauseImage == "" {
		cfg.PauseImage = DefaultPauseImage
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 60 * time.Second
	}
	if cfg.KueueNamespace == "" {
		cfg.KueueNamespace = DefaultKueueNamespace
	}
	if cfg.KueueConfigMapName == "" {
		cfg.KueueConfigMapName = DefaultKueueConfigMapName
	}
	var hasher SensitiveDataHasher = NoopHasher{}
	if cfg.HashNames {
		hasher = NewSensitiveDataHasher(cfg.ExtraHashLabels, cfg.HashSalt)
	}
	return &Scraper{
		cfg:                    cfg,
		hasher:                 hasher,
		kubeClient:             kubeClient,
		kueueClient:            kueueClient,
		jobsetClient:           jobsetClient,
		dynamicClient:          dynamicClient,
		infraLog:               infraLog,
		workflowsLog:           workflowsLog,
		execTimeLog:            execTimeLog,
		infraCache:             make(map[string]map[string]cachedObject),
		podStates:              make(map[string]*trackedPodState),
		workflowStates:         make(map[string]*trackedWorkflowState),
		ownerStates:            make(map[string]WorkloadState),
		ownerToWorkload:        make(map[string]string),
		workloadToOwner:        make(map[string]workloadOwnerRef),
		workloadIsTAS:          make(map[string]bool),
		workloadPodSets:        make(map[string][]string),
		jobSetQueues:           make(map[string]string),
		jobToJobSet:            make(map[string]string),
		rayClusterToRayJob:     make(map[string]string),
		replicaSetToDeployment: make(map[string]string),
		workloadStates:         make(map[string]*workloadExecState),
	}
}

// Run performs a deterministic synchronous initial List() barrier across Kueue Workloads,
// parent/child workflows, and Pods so t0 ownership/queue/TAS linkage maps are populated,
// executes an initial scrape immediately, starts real-time watch loops resuming from the
// captured ResourceVersions, and then scrapes periodically every cfg.Interval until ctx is canceled.
func (s *Scraper) Run(ctx context.Context) error {
	// Capture a single unified t0 timestamp across the entire synchronous startup List()
	// barrier and initial ScrapeOnce so all t0 infrastructure, pod, workflow, and
	// execution-time events share identical t0 alignment even on large (>1,000-workload) clusters.
	startupTime := time.Now().UTC()

	// 0. Scrape Kueue controller Configuration up front so manageJobsWithoutQueueName is warm
	// before t0 workflow discovery runs.
	s.scrapeKueueConfiguration(ctx, startupTime)

	// 1. Synchronously index existing Kueue Workloads first so ownerStates, ownerToWorkload,
	// workloadToOwner, workloadIsTAS, and Admitted records exist before workflow and pod events.
	workloadsRV := s.syncInitialWorkloads(ctx, startupTime)

	// 2. Synchronously index top-level parent workflows (JobSets, RayJobs) so child Jobs/RayClusters
	// and Pods inherit LocalQueue and parent owner mappings deterministically.
	jobSetsRV := s.syncInitialJobSets(ctx, startupTime)
	var rayJobsRV, lwsRV, rayClustersRV string
	if s.dynamicClient != nil {
		rayJobsRV = s.syncInitialDynamicResource(ctx, rayJobGVR, "RayJob", func(et watch.EventType, u *unstructured.Unstructured) {
			s.handleRayJobUnstructuredAt(et, u, startupTime, true)
		})
	}

	// 3. Synchronously index single-tier and child workflows (Jobs, Deployments, ReplicaSets, LWS, RayClusters).
	jobsRV := s.syncInitialJobs(ctx, startupTime)
	deploymentsRV := s.syncInitialDeployments(ctx, startupTime)
	replicaSetsRV := s.syncInitialReplicaSets(ctx)
	if s.dynamicClient != nil {
		lwsRV = s.syncInitialDynamicResource(ctx, lwsGVR, "LeaderWorkerSet", func(et watch.EventType, u *unstructured.Unstructured) {
			s.handleLeaderWorkerSetUnstructuredAt(et, u, startupTime, true)
		})
		rayClustersRV = s.syncInitialDynamicResource(ctx, rayClusterGVR, "RayCluster", func(et watch.EventType, u *unstructured.Unstructured) {
			s.handleRayClusterUnstructuredAt(et, u, startupTime, true)
		})
	}

	// 4. Synchronously index existing Pods after all Workload and workflow ownership maps are warm.
	podsRV, initialPods := s.syncInitialPods(ctx, startupTime)

	// 5. Run initial periodic infrastructure & scheduled-pod scrape immediately at startupTime,
	// reusing initialPods to avoid a redundant cluster-wide Pod List() at startup.
	rVs := s.scrapeOnceAt(ctx, startupTime, initialPods)

	var wg sync.WaitGroup
	startWatcher := func(fn func(context.Context)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fn(ctx)
		}()
	}

	// 6. Start real-time watchers resuming from the initial ResourceVersions captured above.
	startWatcher(func(ctx context.Context) { s.watchNodesLoop(ctx, rVs.nodesRV) })
	startWatcher(func(ctx context.Context) { s.watchPodsLoop(ctx, podsRV) })
	startWatcher(func(ctx context.Context) { s.watchJobsLoop(ctx, jobsRV) })
	startWatcher(func(ctx context.Context) { s.watchDeploymentsLoop(ctx, deploymentsRV) })
	startWatcher(func(ctx context.Context) { s.watchReplicaSetsLoop(ctx, replicaSetsRV) })
	if s.jobsetClient != nil {
		startWatcher(func(ctx context.Context) { s.watchJobSetsLoop(ctx, jobSetsRV) })
	}
	if s.kueueClient != nil {
		startWatcher(func(ctx context.Context) { s.watchWorkloadsLoop(ctx, workloadsRV) })
		startWatcher(func(ctx context.Context) { s.watchClusterQueuesLoop(ctx, rVs.cqsRV) })
		startWatcher(func(ctx context.Context) { s.watchCohortsLoop(ctx, rVs.cohortsRV) })
		startWatcher(func(ctx context.Context) { s.watchLocalQueuesLoop(ctx, rVs.lqsRV) })
		startWatcher(func(ctx context.Context) { s.watchResourceFlavorsLoop(ctx, rVs.flavorsRV) })
		startWatcher(func(ctx context.Context) { s.watchTopologiesLoop(ctx, rVs.topologiesRV) })
		startWatcher(func(ctx context.Context) { s.watchWorkloadPriorityClassesLoop(ctx, rVs.wpcsRV) })
	}
	if s.dynamicClient != nil {
		startWatcher(func(ctx context.Context) {
			s.watchDynamicResourceLoop(ctx, lwsGVR, "LeaderWorkerSet", lwsRV, func(et watch.EventType, u *unstructured.Unstructured, isRelist bool) {
				s.handleLeaderWorkerSetUnstructuredAt(et, u, time.Now().UTC(), isRelist)
			})
		})
		startWatcher(func(ctx context.Context) {
			s.watchDynamicResourceLoop(ctx, rayJobGVR, "RayJob", rayJobsRV, func(et watch.EventType, u *unstructured.Unstructured, isRelist bool) {
				s.handleRayJobUnstructuredAt(et, u, time.Now().UTC(), isRelist)
			})
		})
		startWatcher(func(ctx context.Context) {
			s.watchDynamicResourceLoop(ctx, rayClusterGVR, "RayCluster", rayClustersRV, func(et watch.EventType, u *unstructured.Unstructured, isRelist bool) {
				s.handleRayClusterUnstructuredAt(et, u, time.Now().UTC(), isRelist)
			})
		})
	}

	ticker := time.NewTicker(s.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			s.flushInFlightWorkloadsAsPartial(time.Now().UTC())
			s.syncLogs()
			return nil
		case <-ticker.C:
			s.ScrapeOnce(ctx)
		}
	}
}

func (s *Scraper) syncLogs() {
	if s.infraLog != nil {
		_ = s.infraLog.Sync()
	}
	if s.workflowsLog != nil {
		_ = s.workflowsLog.Sync()
	}
	if s.execTimeLog != nil {
		_ = s.execTimeLog.Sync()
	}
}

type infraScrapeRVs struct {
	nodesRV      string
	cqsRV        string
	cohortsRV    string
	lqsRV        string
	flavorsRV    string
	topologiesRV string
	wpcsRV       string
}

// ScrapeOnce executes a single periodic scrape of Nodes, Namespaces, RuntimeClasses, PriorityClasses,
// Kueue Configuration, ClusterQueues, Cohorts, LocalQueues, ResourceFlavors, Topologies,
// WorkloadPriorityClasses, and Scheduled/Running Pods.
func (s *Scraper) ScrapeOnce(ctx context.Context) {
	_ = s.scrapeOnceAt(ctx, time.Now().UTC(), nil)
}

func (s *Scraper) scrapeOnceAt(ctx context.Context, now time.Time, prelistedPods []corev1.Pod) infraScrapeRVs {
	var out infraScrapeRVs
	out.nodesRV = s.scrapeNodes(ctx, now)
	s.scrapeNamespaces(ctx, now)
	s.scrapeRuntimeClasses(ctx, now)
	s.scrapePriorityClasses(ctx, now)
	s.scrapeKueueConfiguration(ctx, now)
	if s.kueueClient != nil {
		out.cqsRV = s.scrapeClusterQueues(ctx, now)
		out.cohortsRV = s.scrapeCohorts(ctx, now)
		out.lqsRV = s.scrapeLocalQueues(ctx, now)
		out.flavorsRV = s.scrapeResourceFlavors(ctx, now)
		out.topologiesRV = s.scrapeTopologies(ctx, now)
		out.wpcsRV = s.scrapeWorkloadPriorityClasses(ctx, now)
	}
	s.scrapeScheduledAndRunningPodsWithList(ctx, now, prelistedPods)
	s.syncLogs()
	return out
}

func canonicalDigest(payload any) string {
	// Normalize payload to map and drop volatile resourceVersion / generation / condition heartbeat timestamps
	// before hashing for diff detection so periodic kubelet heartbeats do not emit spurious Node Update events.
	raw, err := json.Marshal(payload)
	if err != nil {
		return ""
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err == nil {
		if meta, ok := m["metadata"].(map[string]any); ok {
			delete(meta, "resourceVersion")
			delete(meta, "generation")
		}
		if st, ok := m["status"].(map[string]any); ok {
			if conds, ok := st["conditions"].([]any); ok {
				for _, item := range conds {
					if cm, ok := item.(map[string]any); ok {
						delete(cm, "lastHeartbeatTime")
					}
				}
			}
		}
		if norm, err := json.Marshal(m); err == nil {
			raw = norm
		}
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func (s *Scraper) reconcileInfraKind(kind string, now time.Time, current map[string]cachedObject) {
	s.mu.Lock()
	prevMap, ok := s.infraCache[kind]
	if !ok {
		prevMap = make(map[string]cachedObject)
		s.infraCache[kind] = prevMap
	}

	var events []InfraEvent

	// Deterministic ordering of keys.
	keys := make([]string, 0, len(current))
	for k := range current {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		cur := current[k]
		prev, existed := prevMap[k]
		if !existed {
			events = append(events, InfraEvent{
				Timestamp: now,
				EventType: EventNew,
				Payload:   cur.payload,
			})
			prevMap[k] = cur
		} else if prev.digest != cur.digest {
			events = append(events, InfraEvent{
				Timestamp: now,
				EventType: EventUpdate,
				Payload:   cur.payload,
			})
			prevMap[k] = cur
		}
	}

	var deletedKeys []string
	for k := range prevMap {
		if _, stillExists := current[k]; !stillExists {
			deletedKeys = append(deletedKeys, k)
		}
	}
	sort.Strings(deletedKeys)
	for _, k := range deletedKeys {
		prev := prevMap[k]
		events = append(events, InfraEvent{
			Timestamp: now,
			EventType: EventDeleted,
			Payload:   prev.payload,
		})
		delete(prevMap, k)
	}
	s.mu.Unlock()

	for _, ev := range events {
		if err := s.infraLog.Append(ev); err != nil {
			log.Printf("ERROR writing %s event to %s: %v", kind, s.cfg.InfraFile, err)
		}
	}
}

func (s *Scraper) scrapeNodes(ctx context.Context, now time.Time) string {
	if s.kubeClient == nil {
		return ""
	}
	list, err := s.kubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Printf("WARN failed to list Nodes: %v", err)
		return ""
	}
	current := make(map[string]cachedObject, len(list.Items))
	for i := range list.Items {
		node := &list.Items[i]
		payload := SanitizeNode(node, s.hasher)
		current[node.Name] = cachedObject{
			name:    node.Name,
			digest:  canonicalDigest(payload),
			payload: payload,
		}
	}
	s.reconcileInfraKind("Node", now, current)
	if list.ResourceVersion != "" {
		return list.ResourceVersion
	}
	return "0"
}

func (s *Scraper) handleNodeWatchEvent(eventType watch.EventType, node *corev1.Node) {
	s.handleNodeWatchEventAt(eventType, node, time.Now().UTC())
}

func (s *Scraper) handleNodeWatchEventAt(eventType watch.EventType, node *corev1.Node, now time.Time) {
	if node == nil || node.Name == "" {
		return
	}
	var ev *InfraEvent
	s.mu.Lock()
	nodeCache, ok := s.infraCache["Node"]
	if !ok {
		nodeCache = make(map[string]cachedObject)
		s.infraCache["Node"] = nodeCache
	}
	switch eventType {
	case watch.Added, watch.Modified:
		payload := SanitizeNode(node, s.hasher)
		digest := canonicalDigest(payload)
		prev, existed := nodeCache[node.Name]
		if !existed {
			nodeCache[node.Name] = cachedObject{
				name:    node.Name,
				digest:  digest,
				payload: payload,
			}
			ev = &InfraEvent{
				Timestamp: now,
				EventType: EventNew,
				Payload:   payload,
			}
		} else if prev.digest != digest {
			nodeCache[node.Name] = cachedObject{
				name:    node.Name,
				digest:  digest,
				payload: payload,
			}
			ev = &InfraEvent{
				Timestamp: now,
				EventType: EventUpdate,
				Payload:   payload,
			}
		}
	case watch.Deleted:
		if prev, existed := nodeCache[node.Name]; existed {
			delete(nodeCache, node.Name)
			ev = &InfraEvent{
				Timestamp: now,
				EventType: EventDeleted,
				Payload:   prev.payload,
			}
		} else {
			payload := SanitizeNode(node, s.hasher)
			ev = &InfraEvent{
				Timestamp: now,
				EventType: EventDeleted,
				Payload:   payload,
			}
		}
	}
	s.mu.Unlock()

	if ev != nil && s.infraLog != nil {
		if err := s.infraLog.Append(*ev); err != nil {
			log.Printf("ERROR writing Node watch event to %s: %v", s.cfg.InfraFile, err)
		}
	}
}

func (s *Scraper) scrapeNamespaces(ctx context.Context, now time.Time) {
	if s.kubeClient == nil {
		return
	}
	list, err := s.kubeClient.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Printf("WARN failed to list Namespaces: %v", err)
		return
	}
	current := make(map[string]cachedObject, len(list.Items))
	for i := range list.Items {
		ns := &list.Items[i]
		payload := SanitizeNamespace(ns, s.hasher)
		current[ns.Name] = cachedObject{
			name:    ns.Name,
			digest:  canonicalDigest(payload),
			payload: payload,
		}
	}
	s.reconcileInfraKind("Namespace", now, current)
}

// SanitizeKueueConfigurationYAML parses a Kueue controller_manager_config.yaml document
// and retains strictly scheduler/admission policy fields (fairSharing, admissionFairSharing,
// waitForPodsReady, featureGates, resources, manageJobsWithoutQueueName), stripping all
// cluster-internal networking, webhook, TLS, leader-election, and client-connection settings.
func SanitizeKueueConfigurationYAML(rawYAML string) (map[string]any, bool) {
	trimmed := strings.TrimSpace(rawYAML)
	if trimmed == "" {
		return nil, false
	}
	jsonBytes, err := yaml.YAMLToJSON([]byte(trimmed))
	if err != nil {
		return nil, false
	}
	var raw map[string]any
	if err := json.Unmarshal(jsonBytes, &raw); err != nil {
		return nil, false
	}
	kind, _ := raw["kind"].(string)
	apiVer, _ := raw["apiVersion"].(string)
	if kind != "Configuration" || !strings.HasPrefix(apiVer, "config.kueue.x-k8s.io/") {
		return nil, false
	}

	out := map[string]any{
		"apiVersion": "config.kueue.x-k8s.io/v1beta2",
		"kind":       "Configuration",
	}

	if fsVal, exists := raw["fairSharing"]; exists && fsVal != nil {
		if fsMap, ok := fsVal.(map[string]any); ok {
			enabled := true
			if en, hasEn := fsMap["enable"].(bool); hasEn && !en {
				enabled = false
			}
			if enabled {
				cleanFS := map[string]any{}
				if strats, ok := fsMap["preemptionStrategies"].([]any); ok && len(strats) > 0 {
					cleanFS["preemptionStrategies"] = strats
				} else {
					cleanFS["preemptionStrategies"] = []any{
						"LessThanOrEqualToFinalShare",
						"LessThanInitialShare",
					}
				}
				out["fairSharing"] = cleanFS
			}
		}
	}

	if afsVal, exists := raw["admissionFairSharing"]; exists && afsVal != nil {
		if afsMap, ok := afsVal.(map[string]any); ok {
			out["admissionFairSharing"] = afsMap
		}
	}

	if wfprVal, exists := raw["waitForPodsReady"]; exists && wfprVal != nil {
		if wfprMap, ok := wfprVal.(map[string]any); ok && len(wfprMap) > 0 {
			out["waitForPodsReady"] = wfprMap
		}
	}

	if fgVal, exists := raw["featureGates"]; exists && fgVal != nil {
		if fgMap, ok := fgVal.(map[string]any); ok && len(fgMap) > 0 {
			out["featureGates"] = fgMap
		}
	}

	if resVal, exists := raw["resources"]; exists && resVal != nil {
		if resMap, ok := resVal.(map[string]any); ok && len(resMap) > 0 {
			out["resources"] = resMap
		}
	}

	if mjwqn, ok := raw["manageJobsWithoutQueueName"].(bool); ok && mjwqn {
		out["manageJobsWithoutQueueName"] = true
	}

	return out, true
}

func extractKueueDeploymentOverrides(deploy *appsv1.Deployment) (cmName string, cliFeatureGates map[string]bool) {
	if deploy == nil {
		return "", nil
	}
	mountToCM := make(map[string]string)
	for _, vol := range deploy.Spec.Template.Spec.Volumes {
		if vol.ConfigMap != nil && vol.ConfigMap.Name != "" {
			mountToCM[vol.Name] = vol.ConfigMap.Name
		}
	}
	for _, c := range deploy.Spec.Template.Spec.Containers {
		pathToCM := make(map[string]string)
		for _, vm := range c.VolumeMounts {
			if cm, ok := mountToCM[vm.Name]; ok {
				pathToCM[strings.TrimRight(vm.MountPath, "/")] = cm
			}
		}
		tokens := append(append([]string{}, c.Command...), c.Args...)
		for idx, tok := range tokens {
			var cfgPath string
			if strings.HasPrefix(tok, "--config=") {
				cfgPath = strings.TrimPrefix(tok, "--config=")
			} else if tok == "--config" && idx+1 < len(tokens) {
				cfgPath = tokens[idx+1]
			}
			if cfgPath != "" && cmName == "" {
				for mp, cm := range pathToCM {
					if mp != "" && (cfgPath == mp || strings.HasPrefix(cfgPath, mp+"/")) {
						cmName = cm
						break
					}
				}
			}
			var fgRaw string
			if strings.HasPrefix(tok, "--feature-gates=") {
				fgRaw = strings.TrimPrefix(tok, "--feature-gates=")
			} else if tok == "--feature-gates" && idx+1 < len(tokens) {
				fgRaw = tokens[idx+1]
			}
			if fgRaw != "" {
				for _, entry := range strings.Split(fgRaw, ",") {
					kv := strings.SplitN(strings.TrimSpace(entry), "=", 2)
					if len(kv) != 2 || kv[0] == "" {
						continue
					}
					val := strings.ToLower(strings.TrimSpace(kv[1]))
					if cliFeatureGates == nil {
						cliFeatureGates = make(map[string]bool)
					}
					switch val {
					case "true", "1":
						cliFeatureGates[strings.TrimSpace(kv[0])] = true
					case "false", "0":
						cliFeatureGates[strings.TrimSpace(kv[0])] = false
					}
				}
			}
		}
	}
	return cmName, cliFeatureGates
}

func (s *Scraper) scrapeKueueConfiguration(ctx context.Context, now time.Time) {
	if s.kubeClient == nil || s.cfg.KueueNamespace == "" || s.cfg.KueueConfigMapName == "" {
		return
	}
	var cliFeatureGates map[string]bool
	var discoveredCMName string
	if deploy, dErr := s.kubeClient.AppsV1().Deployments(s.cfg.KueueNamespace).Get(ctx, "kueue-controller-manager", metav1.GetOptions{}); dErr == nil {
		discoveredCMName, cliFeatureGates = extractKueueDeploymentOverrides(deploy)
	} else if dList, lErr := s.kubeClient.AppsV1().Deployments(s.cfg.KueueNamespace).List(ctx, metav1.ListOptions{}); lErr == nil {
		for i := range dList.Items {
			cmCand, fgCand := extractKueueDeploymentOverrides(&dList.Items[i])
			if discoveredCMName == "" && cmCand != "" {
				discoveredCMName = cmCand
			}
			for gk, gv := range fgCand {
				if cliFeatureGates == nil {
					cliFeatureGates = make(map[string]bool)
				}
				cliFeatureGates[gk] = gv
			}
		}
	}

	targetCM := s.cfg.KueueConfigMapName
	cm, err := s.kubeClient.CoreV1().ConfigMaps(s.cfg.KueueNamespace).Get(ctx, targetCM, metav1.GetOptions{})
	if err != nil && apierrors.IsNotFound(err) && discoveredCMName != "" && discoveredCMName != targetCM {
		cm, err = s.kubeClient.CoreV1().ConfigMaps(s.cfg.KueueNamespace).Get(ctx, discoveredCMName, metav1.GetOptions{})
	}
	if err != nil {
		if apierrors.IsNotFound(err) {
			if len(cliFeatureGates) > 0 {
				fgAny := make(map[string]any, len(cliFeatureGates))
				for k, v := range cliFeatureGates {
					fgAny[k] = v
				}
				payload := map[string]any{
					"apiVersion":   "config.kueue.x-k8s.io/v1beta2",
					"kind":         "Configuration",
					"featureGates": fgAny,
				}
				s.reconcileInfraKind("Configuration", now, map[string]cachedObject{
					"kueue-configuration": {
						name:    "kueue-configuration",
						digest:  canonicalDigest(payload),
						payload: payload,
					},
				})
				return
			}
			s.reconcileInfraKind("Configuration", now, nil)
			return
		}
		if !apierrors.IsForbidden(err) {
			log.Printf("WARN failed to get ConfigMap %s/%s: %v", s.cfg.KueueNamespace, s.cfg.KueueConfigMapName, err)
		}
		return
	}
	rawYAML := cm.Data["controller_manager_config.yaml"]
	if strings.TrimSpace(rawYAML) == "" {
		keys := make([]string, 0, len(cm.Data))
		for k := range cm.Data {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if strings.HasSuffix(k, ".yaml") || strings.HasSuffix(k, ".yml") {
				rawYAML = cm.Data[k]
				break
			}
		}
	}
	payload, ok := SanitizeKueueConfigurationYAML(rawYAML)
	if !ok {
		return
	}
	if len(cliFeatureGates) > 0 {
		fgMap, _ := payload["featureGates"].(map[string]any)
		if fgMap == nil {
			fgMap = make(map[string]any, len(cliFeatureGates))
		}
		for k, v := range cliFeatureGates {
			fgMap[k] = v
		}
		payload["featureGates"] = fgMap
	}
	mjwqn, _ := payload["manageJobsWithoutQueueName"].(bool)
	s.mu.Lock()
	s.manageJobsWithoutQueueName = mjwqn
	s.mu.Unlock()

	current := map[string]cachedObject{
		"kueue-configuration": {
			name:    "kueue-configuration",
			digest:  canonicalDigest(payload),
			payload: payload,
		},
	}
	s.reconcileInfraKind("Configuration", now, current)
}

func (s *Scraper) handleInfraObjectWatchEventAt(
	eventType watch.EventType,
	kind string,
	cacheKey string,
	name string,
	namespace string,
	payload any,
	now time.Time,
) {
	if cacheKey == "" || payload == nil {
		return
	}
	var ev *InfraEvent
	s.mu.Lock()
	kindCache, ok := s.infraCache[kind]
	if !ok {
		kindCache = make(map[string]cachedObject)
		s.infraCache[kind] = kindCache
	}
	switch eventType {
	case watch.Added, watch.Modified:
		digest := canonicalDigest(payload)
		prev, existed := kindCache[cacheKey]
		if !existed {
			kindCache[cacheKey] = cachedObject{
				name:      name,
				namespace: namespace,
				digest:    digest,
				payload:   payload,
			}
			ev = &InfraEvent{
				Timestamp: now,
				EventType: EventNew,
				Payload:   payload,
			}
		} else if prev.digest != digest {
			kindCache[cacheKey] = cachedObject{
				name:      name,
				namespace: namespace,
				digest:    digest,
				payload:   payload,
			}
			ev = &InfraEvent{
				Timestamp: now,
				EventType: EventUpdate,
				Payload:   payload,
			}
		}
	case watch.Deleted:
		if prev, existed := kindCache[cacheKey]; existed {
			delete(kindCache, cacheKey)
			ev = &InfraEvent{
				Timestamp: now,
				EventType: EventDeleted,
				Payload:   prev.payload,
			}
		} else {
			ev = &InfraEvent{
				Timestamp: now,
				EventType: EventDeleted,
				Payload:   payload,
			}
		}
	}
	s.mu.Unlock()

	if ev != nil && s.infraLog != nil {
		if err := s.infraLog.Append(*ev); err != nil {
			log.Printf("ERROR writing %s watch event to %s: %v", kind, s.cfg.InfraFile, err)
		}
	}
}

func listRVOrZero(rv string) string {
	if rv != "" {
		return rv
	}
	return "0"
}

func (s *Scraper) scrapeClusterQueues(ctx context.Context, now time.Time) string {
	list, err := s.kueueClient.KueueV1beta2().ClusterQueues().List(ctx, metav1.ListOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			log.Printf("WARN failed to list ClusterQueues: %v", err)
		}
		return ""
	}
	current := make(map[string]cachedObject, len(list.Items))
	for i := range list.Items {
		cq := &list.Items[i]
		payload := SanitizeClusterQueue(cq, s.hasher)
		current[cq.Name] = cachedObject{
			name:    cq.Name,
			digest:  canonicalDigest(payload),
			payload: payload,
		}
	}
	s.reconcileInfraKind("ClusterQueue", now, current)
	return listRVOrZero(list.ResourceVersion)
}

func (s *Scraper) scrapeCohorts(ctx context.Context, now time.Time) string {
	list, err := s.kueueClient.KueueV1beta2().Cohorts().List(ctx, metav1.ListOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			log.Printf("WARN failed to list Cohorts: %v", err)
		}
		return ""
	}
	current := make(map[string]cachedObject, len(list.Items))
	for i := range list.Items {
		cohort := &list.Items[i]
		payload := SanitizeCohort(cohort, s.hasher)
		current[cohort.Name] = cachedObject{
			name:    cohort.Name,
			digest:  canonicalDigest(payload),
			payload: payload,
		}
	}
	s.reconcileInfraKind("Cohort", now, current)
	return listRVOrZero(list.ResourceVersion)
}

func (s *Scraper) scrapeLocalQueues(ctx context.Context, now time.Time) string {
	list, err := s.kueueClient.KueueV1beta2().LocalQueues("").List(ctx, metav1.ListOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			log.Printf("WARN failed to list LocalQueues: %v", err)
		}
		return ""
	}
	current := make(map[string]cachedObject, len(list.Items))
	for i := range list.Items {
		lq := &list.Items[i]
		payload := SanitizeLocalQueue(lq, s.hasher)
		key := lq.Namespace + "/" + lq.Name
		current[key] = cachedObject{
			name:      lq.Name,
			namespace: lq.Namespace,
			digest:    canonicalDigest(payload),
			payload:   payload,
		}
	}
	s.reconcileInfraKind("LocalQueue", now, current)
	return listRVOrZero(list.ResourceVersion)
}

func (s *Scraper) scrapeResourceFlavors(ctx context.Context, now time.Time) string {
	list, err := s.kueueClient.KueueV1beta2().ResourceFlavors().List(ctx, metav1.ListOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			log.Printf("WARN failed to list ResourceFlavors: %v", err)
		}
		return ""
	}
	current := make(map[string]cachedObject, len(list.Items))
	for i := range list.Items {
		rf := &list.Items[i]
		payload := SanitizeResourceFlavor(rf, s.hasher)
		current[rf.Name] = cachedObject{
			name:    rf.Name,
			digest:  canonicalDigest(payload),
			payload: payload,
		}
	}
	s.reconcileInfraKind("ResourceFlavor", now, current)
	return listRVOrZero(list.ResourceVersion)
}

func (s *Scraper) scrapeTopologies(ctx context.Context, now time.Time) string {
	list, err := s.kueueClient.KueueV1beta2().Topologies().List(ctx, metav1.ListOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			log.Printf("WARN failed to list Topologies: %v", err)
		}
		return ""
	}
	current := make(map[string]cachedObject, len(list.Items))
	for i := range list.Items {
		topo := &list.Items[i]
		payload := SanitizeTopology(topo, s.hasher)
		current[topo.Name] = cachedObject{
			name:    topo.Name,
			digest:  canonicalDigest(payload),
			payload: payload,
		}
	}
	s.reconcileInfraKind("Topology", now, current)
	return listRVOrZero(list.ResourceVersion)
}

func (s *Scraper) scrapeWorkloadPriorityClasses(ctx context.Context, now time.Time) string {
	list, err := s.kueueClient.KueueV1beta2().WorkloadPriorityClasses().List(ctx, metav1.ListOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			log.Printf("WARN failed to list WorkloadPriorityClasses: %v", err)
		}
		return ""
	}
	current := make(map[string]cachedObject, len(list.Items))
	for i := range list.Items {
		wpc := &list.Items[i]
		payload := SanitizeWorkloadPriorityClass(wpc, s.hasher)
		current[wpc.Name] = cachedObject{
			name:    wpc.Name,
			digest:  canonicalDigest(payload),
			payload: payload,
		}
	}
	s.reconcileInfraKind("WorkloadPriorityClass", now, current)
	return listRVOrZero(list.ResourceVersion)
}

func (s *Scraper) scrapeRuntimeClasses(ctx context.Context, now time.Time) {
	if s.kubeClient == nil {
		return
	}
	list, err := s.kubeClient.NodeV1().RuntimeClasses().List(ctx, metav1.ListOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			log.Printf("WARN failed to list RuntimeClasses: %v", err)
		}
		return
	}
	current := make(map[string]cachedObject, len(list.Items))
	for i := range list.Items {
		rc := &list.Items[i]
		payload := SanitizeRuntimeClass(rc, s.hasher)
		current[rc.Name] = cachedObject{
			name:    rc.Name,
			digest:  canonicalDigest(payload),
			payload: payload,
		}
	}
	s.reconcileInfraKind("RuntimeClass", now, current)
}

func (s *Scraper) scrapePriorityClasses(ctx context.Context, now time.Time) {
	if s.kubeClient == nil {
		return
	}
	list, err := s.kubeClient.SchedulingV1().PriorityClasses().List(ctx, metav1.ListOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			log.Printf("WARN failed to list PriorityClasses: %v", err)
		}
		return
	}
	current := make(map[string]cachedObject)
	for i := range list.Items {
		pc := &list.Items[i]
		if pc.Name == "system-cluster-critical" || pc.Name == "system-node-critical" {
			continue
		}
		payload := SanitizePriorityClass(pc, s.hasher)
		current[pc.Name] = cachedObject{
			name:    pc.Name,
			digest:  canonicalDigest(payload),
			payload: payload,
		}
	}
	s.reconcileInfraKind("PriorityClass", now, current)
}

func (s *Scraper) scrapeScheduledAndRunningPods(ctx context.Context, now time.Time) {
	s.scrapeScheduledAndRunningPodsWithList(ctx, now, nil)
}

func (s *Scraper) scrapeScheduledAndRunningPodsWithList(ctx context.Context, now time.Time, prelistedPods []corev1.Pod) {
	if s.kubeClient == nil && prelistedPods == nil {
		return
	}
	listStartedAt := now
	var items []corev1.Pod
	if prelistedPods != nil {
		items = prelistedPods
	} else {
		beforeList := time.Now().UTC()
		if beforeList.Before(listStartedAt) {
			listStartedAt = beforeList
		}
		list, err := s.kubeClient.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
		if err != nil {
			log.Printf("WARN failed to list Pods: %v", err)
			return
		}
		items = list.Items
	}

	var events []InfraEvent
	presentActive := make(map[string]bool, len(items))
	terminalPods := make(map[string]*corev1.Pod)

	s.mu.Lock()
	for i := range items {
		pod := &items[i]
		key := pod.Namespace + "/" + pod.Name
		if IsScheduledAndRunningPod(pod) {
			presentActive[key] = true
			ev := s.observeScheduledRunningPodLocked(pod, now)
			if ev != nil {
				events = append(events, *ev)
			}
		} else if IsTerminalPod(pod) || (pod.DeletionTimestamp != nil && !pod.DeletionTimestamp.IsZero()) {
			terminalPods[key] = pod
		}
	}
	// Emit "new" for any pods observed scheduled & running by the watch before completing,
	// and emit "deleted" for any previously emitted pods that terminated or disappeared across watch reconnects.
	podKeys := make([]string, 0, len(s.podStates))
	for k := range s.podStates {
		podKeys = append(podKeys, k)
	}
	sort.Strings(podKeys)
	for _, k := range podKeys {
		st := s.podStates[k]
		if st.deleted {
			continue
		}
		// Guard against the stale periodic List() race: if a pod was bound by kube-scheduler and
		// observed via the real-time Pod watch *after* Pods().List() started, its absence from
		// presentActive is solely due to the older List() snapshot—do NOT mark it deleted!
		if !presentActive[k] {
			if _, isTerm := terminalPods[k]; !isTerm && st.firstNoticeTime.After(listStartedAt) {
				continue
			}
		}
		if !st.emittedNew && st.lastPayload.Placement != "" {
			st.emittedNew = true
			st.lastDigest = canonicalDigest(st.lastPayload)
			events = append(events, InfraEvent{
				Timestamp: st.firstNoticeTime,
				EventType: EventNew,
				Payload:   st.lastPayload,
			})
		}
		if st.emittedNew && !presentActive[k] {
			endTime := now
			var delPayload ScrapedPodPayload
			if termPod, ok := terminalPods[k]; ok {
				endTime = ExtractPodEndTime(termPod, now)
				linkage := s.resolvePodWorkloadLinkageLocked(termPod)
				delPayload = BuildScrapedPodPayloadWithLinkage(termPod, st.firstNoticeTime, &endTime, linkage, s.hasher)
				if delPayload.Placement == "" && st.lastPayload.Placement != "" {
					delPayload.Placement = st.lastPayload.Placement
				}
				if delPayload.SchedulingTime == nil && st.lastPayload.SchedulingTime != nil {
					delPayload.SchedulingTime = st.lastPayload.SchedulingTime
				}
				mergePreservedPodLinkage(&delPayload, st.lastPayload)
			} else {
				delPayload = st.lastPayload
				t := metav1.NewTime(endTime.UTC())
				delPayload.DeletionTime = &t
			}
			st.deleted = true
			st.lastPayload = ScrapedPodPayload{}
			events = append(events, InfraEvent{
				Timestamp: now,
				EventType: EventDeleted,
				Payload:   delPayload,
			})
		}
	}
	s.mu.Unlock()

	for _, ev := range events {
		if err := s.infraLog.Append(ev); err != nil {
			log.Printf("ERROR writing Pod event to %s: %v", s.cfg.InfraFile, err)
		}
	}
}

func (s *Scraper) resolvePodWorkloadLinkageLocked(pod *corev1.Pod) PodWorkloadLinkage {
	if pod == nil {
		return PodWorkloadLinkage{}
	}
	ns := pod.Namespace
	var rawWorkloadName, ownerKind, rawOwnerName, podSetName string

	// 1. Direct Kueue Workload annotations/labels on the Pod.
	if pod.Annotations != nil {
		if wl := pod.Annotations[KueueWorkloadAnnot]; wl != "" {
			rawWorkloadName = wl
		} else if wl := pod.Annotations[KueueWorkloadSliceNameAnnot]; wl != "" {
			rawWorkloadName = wl
		}
		if rh := pod.Annotations[KueueRoleHashAnnotation]; rh != "" {
			podSetName = rh
		}
	}
	if rawWorkloadName == "" && pod.Labels != nil {
		if wl := pod.Labels[KueuePrebuiltWorkloadLabel]; wl != "" {
			rawWorkloadName = wl
		} else if pg := pod.Labels[KueuePodGroupNameLabel]; pg != "" {
			rawWorkloadName = pg
			ownerKind = "PodGroup"
			rawOwnerName = pg
		}
	}

	// 2. Resolve top-level workflow owner via Pod labels and OwnerReferences.
	if pod.Labels != nil {
		if jsName := pod.Labels["jobset.sigs.k8s.io/jobset-name"]; jsName != "" {
			ownerKind = "JobSet"
			rawOwnerName = jsName
		} else if lwsName := pod.Labels["leaderworkerset.sigs.k8s.io/name"]; lwsName != "" {
			ownerKind = "LeaderWorkerSet"
			rawOwnerName = lwsName
		} else if rcName := pod.Labels["ray.io/cluster"]; rcName != "" {
			if rjName, ok := s.rayClusterToRayJob[ns+"/"+rcName]; ok && rjName != "" {
				ownerKind = "RayJob"
				rawOwnerName = rjName
			} else {
				ownerKind = "RayCluster"
				rawOwnerName = rcName
			}
		} else {
			jobName := pod.Labels["batch.kubernetes.io/job-name"]
			if jobName == "" {
				jobName = pod.Labels["job-name"]
			}
			if jobName != "" {
				if jsName, ok := s.jobToJobSet[ns+"/"+jobName]; ok && jsName != "" {
					ownerKind = "JobSet"
					rawOwnerName = jsName
				} else {
					ownerKind = "Job"
					rawOwnerName = jobName
				}
			}
		}
		if podSetName == "" {
			if rj := pod.Labels["jobset.sigs.k8s.io/replicatedjob-name"]; rj != "" {
				podSetName = rj
			} else if rg := pod.Labels["ray.io/group"]; rg != "" {
				podSetName = rg
			} else if rgType := pod.Labels["ray.io/node-type"]; rgType == "head" {
				podSetName = "head"
			}
		}
	}

	if ownerKind == "" {
		for _, ref := range pod.OwnerReferences {
			switch ref.Kind {
			case "JobSet":
				ownerKind = "JobSet"
				rawOwnerName = ref.Name
			case "Job":
				if jsName, ok := s.jobToJobSet[ns+"/"+ref.Name]; ok && jsName != "" {
					ownerKind = "JobSet"
					rawOwnerName = jsName
				} else {
					ownerKind = "Job"
					rawOwnerName = ref.Name
				}
			case "LeaderWorkerSet":
				ownerKind = "LeaderWorkerSet"
				rawOwnerName = ref.Name
			case "RayJob":
				ownerKind = "RayJob"
				rawOwnerName = ref.Name
			case "RayCluster":
				if rjName, ok := s.rayClusterToRayJob[ns+"/"+ref.Name]; ok && rjName != "" {
					ownerKind = "RayJob"
					rawOwnerName = rjName
				} else {
					ownerKind = "RayCluster"
					rawOwnerName = ref.Name
				}
			case "ReplicaSet":
				if depName, ok := s.replicaSetToDeployment[ns+"/"+ref.Name]; ok && depName != "" {
					ownerKind = "Deployment"
					rawOwnerName = depName
				}
			case "Deployment":
				if _, tracked := s.workflowStates["Deployment/"+ns+"/"+ref.Name]; tracked {
					ownerKind = "Deployment"
					rawOwnerName = ref.Name
				}
			}
			if ownerKind != "" {
				break
			}
		}
	}

	if ownerKind == "" && IsStandaloneKueuePod(pod) {
		if pod.Labels != nil && pod.Labels[KueuePodGroupNameLabel] != "" {
			ownerKind = "PodGroup"
			rawOwnerName = pod.Labels[KueuePodGroupNameLabel]
		} else {
			ownerKind = "Pod"
			rawOwnerName = pod.Name
		}
	}

	// 3. Cross-link between owner and Kueue Workload maps.
	if rawWorkloadName == "" && ownerKind != "" && rawOwnerName != "" {
		if wl, ok := s.ownerToWorkload[ownerKind+"/"+ns+"/"+rawOwnerName]; ok {
			rawWorkloadName = wl
		}
	}
	if rawWorkloadName == "" {
		if wl, ok := s.ownerToWorkload["Pod/"+ns+"/"+pod.Name]; ok {
			rawWorkloadName = wl
		}
	}
	if rawWorkloadName != "" && (ownerKind == "" || rawOwnerName == "") {
		if ow, ok := s.workloadToOwner[ns+"/"+rawWorkloadName]; ok {
			ownerKind = ow.kind
			rawOwnerName = ow.name
		}
	}

	// Only attach linkage if this Pod actually belongs to a Kueue-managed workflow or Workload.
	// Unmanaged system/DaemonSet/ReplicaSet/standalone Jobs without Kueue management stay unlinked
	// so chamber replay classifies them as non-Kueue infra-pods.
	if rawWorkloadName == "" && ownerKind != "" && rawOwnerName != "" {
		wfKey := ownerKind + "/" + ns + "/" + rawOwnerName
		_, hasOwnerWl := s.ownerStates[wfKey]
		var hasQueue bool
		if wfSt, ok := s.workflowStates[wfKey]; ok && wfSt.localQueueName != "" {
			hasQueue = true
		}
		if !hasOwnerWl && !hasQueue && !IsKueueManagedPod(pod) {
			ownerKind = ""
			rawOwnerName = ""
			podSetName = ""
		}
	}

	if rawWorkloadName == "" && ownerKind == "" {
		return PodWorkloadLinkage{}
	}

	if podSetName == "" && rawWorkloadName != "" {
		if psNames := s.workloadPodSets[ns+"/"+rawWorkloadName]; len(psNames) == 1 {
			podSetName = psNames[0]
		}
	}
	isTAS := IsTASPod(pod) || (rawWorkloadName != "" && s.workloadIsTAS[ns+"/"+rawWorkloadName])

	return PodWorkloadLinkage{
		RawWorkloadName: rawWorkloadName,
		OwnerKind:       ownerKind,
		RawOwnerName:    rawOwnerName,
		PodSetName:      podSetName,
		IsTAS:           isTAS,
	}
}

func mergePreservedPodLinkage(dst *ScrapedPodPayload, prev ScrapedPodPayload) {
	if dst.WorkloadName == "" && prev.WorkloadName != "" {
		dst.WorkloadName = prev.WorkloadName
	}
	if dst.OwnerKind == "" && prev.OwnerKind != "" {
		dst.OwnerKind = prev.OwnerKind
	}
	if dst.OwnerName == "" && prev.OwnerName != "" {
		dst.OwnerName = prev.OwnerName
	}
	if dst.PodSetName == "" && prev.PodSetName != "" {
		dst.PodSetName = prev.PodSetName
	}
	if !dst.IsTAS && prev.IsTAS {
		dst.IsTAS = true
	}
}

func (s *Scraper) observeScheduledRunningPodLocked(pod *corev1.Pod, now time.Time) *InfraEvent {
	key := pod.Namespace + "/" + pod.Name
	st, ok := s.podStates[key]
	if !ok {
		st = &trackedPodState{
			firstNoticeTime: now,
		}
		s.podStates[key] = st
	}
	if st.deleted {
		return nil
	}

	linkage := s.resolvePodWorkloadLinkageLocked(pod)
	payload := BuildScrapedPodPayloadWithLinkage(pod, st.firstNoticeTime, nil, linkage, s.hasher)
	if st.lastPayload.SchedulingTime != nil {
		payload.SchedulingTime = st.lastPayload.SchedulingTime
	}
	mergePreservedPodLinkage(&payload, st.lastPayload)
	digest := canonicalDigest(payload)

	if !st.emittedNew {
		st.emittedNew = true
		st.lastPayload = payload
		st.lastDigest = digest
		return &InfraEvent{
			Timestamp: now,
			EventType: EventNew,
			Payload:   payload,
		}
	}

	if st.lastDigest != digest {
		st.lastPayload = payload
		st.lastDigest = digest
		return &InfraEvent{
			Timestamp: now,
			EventType: EventUpdate,
			Payload:   payload,
		}
	}
	return nil
}

func (s *Scraper) handlePodWatchEvent(eventType watch.EventType, pod *corev1.Pod) {
	s.handlePodWatchEventAt(eventType, pod, time.Now().UTC(), false)
}

func (s *Scraper) handlePodWatchEventAt(eventType watch.EventType, pod *corev1.Pod, now time.Time, isInitialSync bool) {
	if pod == nil {
		return
	}

	// 1. If this Pod is a standalone Kueue-managed Pod (not owned by Job/JobSet/LWS/Deployment/Ray),
	// record it in workflows.json (skipping pre-t0 historical Succeeded/Failed tombstones during initial sync).
	if IsStandaloneKueuePod(pod) && !(isInitialSync && IsTerminalPod(pod)) {
		var hasAdmissionGate, hasTopologyGate bool
		for _, g := range pod.Spec.SchedulingGates {
			if g.Name == KueueAdmissionGateName {
				hasAdmissionGate = true
			} else if g.Name == KueueTopologyGateName {
				hasTopologyGate = true
			}
		}
		s.mu.Lock()
		wlState := s.determinePodWorkloadStateLocked(pod.Namespace, pod.Name, hasAdmissionGate, hasTopologyGate)
		s.mu.Unlock()
		payload := SanitizeStandaloneKueuePod(pod, s.cfg.PauseImage, s.hasher)
		s.recordWorkflowEvent(
			eventType,
			"Pod",
			pod.Name,
			pod.Namespace,
			pod.CreationTimestamp,
			ExtractLocalQueueName(pod),
			wlState,
			payload,
			now,
		)
	}

	// 2. Track scheduled/running Pod lifecycle and completion/deletion time for infra.json.
	key := pod.Namespace + "/" + pod.Name
	var infraEvents []InfraEvent

	s.mu.Lock()
	switch eventType {
	case watch.Added, watch.Modified:
		if IsScheduledAndRunningPod(pod) {
			if isInitialSync {
				st, ok := s.podStates[key]
				if !ok {
					st = &trackedPodState{
						firstNoticeTime: now,
					}
					s.podStates[key] = st
				}
				if !st.deleted {
					prev := st.lastPayload
					linkage := s.resolvePodWorkloadLinkageLocked(pod)
					st.lastPayload = BuildScrapedPodPayloadWithLinkage(pod, st.firstNoticeTime, nil, linkage, s.hasher)
					if prev.SchedulingTime != nil {
						st.lastPayload.SchedulingTime = prev.SchedulingTime
					}
					mergePreservedPodLinkage(&st.lastPayload, prev)
				}
			} else {
				// Real-time post-t0 pod watch: emit EventNew (and EventUpdate on placement/request change)
				// immediately at exact observation timestamp `now` instead of waiting for the next periodic tick.
				if ev := s.observeScheduledRunningPodLocked(pod, now); ev != nil {
					infraEvents = append(infraEvents, *ev)
				}
			}
		} else if !isInitialSync && (IsTerminalPod(pod) || (pod.DeletionTimestamp != nil && !pod.DeletionTimestamp.IsZero())) {
			// In both vanilla Kubernetes and chambers, Job/JobSet/PodGroup pods transition to
			// Succeeded/Failed when done while remaining in etcd; emit EventDeleted immediately
			// when a tracked running pod releases its node resources.
			st, ok := s.podStates[key]
			if ok && !st.deleted && (st.emittedNew || st.lastPayload.Placement != "") {
				endTime := ExtractPodEndTime(pod, now)
				linkage := s.resolvePodWorkloadLinkageLocked(pod)
				if !st.emittedNew {
					newPayload := st.lastPayload
					if newPayload.Name == "" {
						newPayload = BuildScrapedPodPayloadWithLinkage(pod, st.firstNoticeTime, nil, linkage, s.hasher)
					}
					st.emittedNew = true
					st.lastPayload = newPayload
					infraEvents = append(infraEvents, InfraEvent{
						Timestamp: st.firstNoticeTime,
						EventType: EventNew,
						Payload:   newPayload,
					})
				}
				delPayload := BuildScrapedPodPayloadWithLinkage(pod, st.firstNoticeTime, &endTime, linkage, s.hasher)
				if delPayload.Placement == "" && st.lastPayload.Placement != "" {
					delPayload.Placement = st.lastPayload.Placement
				}
				if delPayload.SchedulingTime == nil && st.lastPayload.SchedulingTime != nil {
					delPayload.SchedulingTime = st.lastPayload.SchedulingTime
				}
				mergePreservedPodLinkage(&delPayload, st.lastPayload)
				st.deleted = true
				st.lastPayload = ScrapedPodPayload{}
				infraEvents = append(infraEvents, InfraEvent{
					Timestamp: now,
					EventType: EventDeleted,
					Payload:   delPayload,
				})
			}
		}
	case watch.Deleted:
		st, ok := s.podStates[key]
		if !ok && pod.Spec.NodeName != "" && !IsTerminalPod(pod) {
			st = &trackedPodState{
				firstNoticeTime: now,
			}
			s.podStates[key] = st
		}
		if st != nil && !st.deleted && (st.emittedNew || pod.Spec.NodeName != "" || st.lastPayload.Placement != "") {
			delTime := ExtractPodEndTime(pod, now)
			linkage := s.resolvePodWorkloadLinkageLocked(pod)
			// If the pod ran and was deleted between periodic scrapes, emit its "New" event first
			// so its creation/placement is also present in infra.json.
			if !st.emittedNew {
				newPayload := st.lastPayload
				if newPayload.Name == "" {
					newPayload = BuildScrapedPodPayloadWithLinkage(pod, st.firstNoticeTime, nil, linkage, s.hasher)
				}
				st.emittedNew = true
				st.lastPayload = newPayload
				infraEvents = append(infraEvents, InfraEvent{
					Timestamp: st.firstNoticeTime,
					EventType: EventNew,
					Payload:   newPayload,
				})
			}

			delPayload := BuildScrapedPodPayloadWithLinkage(pod, st.firstNoticeTime, &delTime, linkage, s.hasher)
			if delPayload.Placement == "" && st.lastPayload.Placement != "" {
				delPayload.Placement = st.lastPayload.Placement
			}
			if delPayload.SchedulingTime == nil && st.lastPayload.SchedulingTime != nil {
				delPayload.SchedulingTime = st.lastPayload.SchedulingTime
			}
			mergePreservedPodLinkage(&delPayload, st.lastPayload)
			st.deleted = true
			infraEvents = append(infraEvents, InfraEvent{
				Timestamp: now,
				EventType: EventDeleted,
				Payload:   delPayload,
			})
		}
		delete(s.podStates, key)
	}
	s.mu.Unlock()

	for _, ev := range infraEvents {
		if err := s.infraLog.Append(ev); err != nil {
			log.Printf("ERROR writing Pod watch event to %s: %v", s.cfg.InfraFile, err)
		}
	}
}

func (s *Scraper) determinePodWorkloadStateLocked(namespace, rawName string, hasAdmissionGate, hasTopologyGate bool) WorkloadState {
	if !hasAdmissionGate && !hasTopologyGate {
		return WorkloadStateAdmitted
	}
	if !hasAdmissionGate && hasTopologyGate {
		return WorkloadStateAdmissionInProgress
	}
	if st, ok := s.ownerStates["Pod/"+namespace+"/"+rawName]; ok && (st == WorkloadStateAdmitted || st == WorkloadStateAdmissionInProgress) {
		return WorkloadStateAdmissionInProgress
	}
	return WorkloadStateNotAdmitted
}

func (s *Scraper) determineSuspendableWorkloadStateLocked(kind, namespace, rawName string, suspended bool, parentJobSetName string) WorkloadState {
	if !suspended {
		return WorkloadStateAdmitted
	}
	if st, ok := s.ownerStates[kind+"/"+namespace+"/"+rawName]; ok && (st == WorkloadStateAdmitted || st == WorkloadStateAdmissionInProgress) {
		return WorkloadStateAdmissionInProgress
	}
	if parentJobSetName != "" {
		if st, ok := s.ownerStates["JobSet/"+namespace+"/"+parentJobSetName]; ok && (st == WorkloadStateAdmitted || st == WorkloadStateAdmissionInProgress) {
			return WorkloadStateAdmissionInProgress
		}
		if jsSt, ok := s.workflowStates["JobSet/"+namespace+"/"+parentJobSetName]; ok && (jsSt.workloadState == WorkloadStateAdmitted || jsSt.workloadState == WorkloadStateAdmissionInProgress) {
			return WorkloadStateAdmissionInProgress
		}
	}
	return WorkloadStateNotAdmitted
}

func (s *Scraper) determineGroupWorkloadStateLocked(kind, namespace, rawName string, readyReplicas, totalReplicas int32) WorkloadState {
	if st, ok := s.ownerStates[kind+"/"+namespace+"/"+rawName]; ok && st != "" {
		return st
	}
	if readyReplicas > 0 {
		return WorkloadStateAdmitted
	}
	if totalReplicas > 0 {
		return WorkloadStateAdmissionInProgress
	}
	return WorkloadStateNotAdmitted
}

func (s *Scraper) recordWorkflowEvent(
	watchType watch.EventType,
	kind string,
	rawName string,
	namespace string,
	creationTime metav1.Time,
	localQueueName string,
	workloadState WorkloadState,
	payload any,
	now time.Time,
) {
	key := kind + "/" + namespace + "/" + rawName
	displayName := s.hasher.HashNameForKind(kind, rawName)
	digest := canonicalDigest(payload)

	var ev *WorkflowEvent

	s.mu.Lock()
	st, existed := s.workflowStates[key]
	switch watchType {
	case watch.Added, watch.Modified:
		if !existed {
			s.workflowStates[key] = &trackedWorkflowState{
				kind:           kind,
				name:           displayName,
				namespace:      namespace,
				creationTime:   creationTime,
				localQueueName: localQueueName,
				workloadState:  workloadState,
				lastDigest:     digest,
				lastPayload:    payload,
			}
			ev = &WorkflowEvent{
				Timestamp:     now,
				EventType:     EventNew,
				WorkloadState: workloadState,
				Payload: WorkflowPayload{
					ObjectKind:     kind,
					Name:           displayName,
					Namespace:      namespace,
					CreationTime:   creationTime,
					LocalQueueName: localQueueName,
					Object:         payload,
				},
			}
		} else if st.lastDigest != digest || st.workloadState != workloadState {
			if localQueueName == "" {
				localQueueName = st.localQueueName
			}
			st.lastDigest = digest
			st.lastPayload = payload
			st.localQueueName = localQueueName
			st.workloadState = workloadState
			ev = &WorkflowEvent{
				Timestamp:     now,
				EventType:     EventUpdate,
				WorkloadState: workloadState,
				Payload: WorkflowPayload{
					ObjectKind:     kind,
					Name:           displayName,
					Namespace:      namespace,
					CreationTime:   creationTime,
					LocalQueueName: localQueueName,
					Object:         payload,
				},
			}
		}
	case watch.Deleted:
		if existed {
			if localQueueName == "" {
				localQueueName = st.localQueueName
			}
			if workloadState == "" {
				workloadState = st.workloadState
			}
			delete(s.workflowStates, key)
		}
		if workloadState == "" {
			workloadState = WorkloadStateNotAdmitted
		}
		ev = &WorkflowEvent{
			Timestamp:     now,
			EventType:     EventDeleted,
			WorkloadState: workloadState,
			Payload: WorkflowPayload{
				ObjectKind:     kind,
				Name:           displayName,
				Namespace:      namespace,
				CreationTime:   creationTime,
				LocalQueueName: localQueueName,
				Object:         payload,
			},
		}
	}
	s.mu.Unlock()

	if ev != nil {
		if err := s.workflowsLog.Append(*ev); err != nil {
			log.Printf("ERROR writing %s workflow event to %s: %v", kind, s.cfg.WorkflowsFile, err)
		}
	}
}

func isTerminalBatchJob(job *batchv1.Job) bool {
	if job == nil {
		return false
	}
	for _, c := range job.Status.Conditions {
		if (c.Type == batchv1.JobComplete || c.Type == batchv1.JobFailed) && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func isTerminalJobSet(js *jobsetv1alpha2.JobSet) bool {
	if js == nil {
		return false
	}
	for _, c := range js.Status.Conditions {
		if (c.Type == string(jobsetv1alpha2.JobSetCompleted) || c.Type == string(jobsetv1alpha2.JobSetFailed)) &&
			c.Status == metav1.ConditionTrue {
			return true
		}
	}
	return false
}

func isTerminalRayJob(rj *rayv1.RayJob) bool {
	if rj == nil {
		return false
	}
	return rj.Status.JobDeploymentStatus == rayv1.JobDeploymentStatusComplete ||
		rj.Status.JobDeploymentStatus == rayv1.JobDeploymentStatusFailed
}

func isSystemNamespaceName(ns string) bool {
	switch ns {
	case "kube-system", "kube-public", "kube-node-lease", "kueue-system", "jobset-system":
		return true
	default:
		return false
	}
}

func hasKueueDomainEntry(m map[string]string) bool {
	for k := range m {
		if strings.HasPrefix(strings.ToLower(k), "kueue.x-k8s.io/") {
			return true
		}
	}
	return false
}

// isKueueManagedWorkflowLocked returns true if the workflow is managed by or targeted at Kueue
// (carries a queue name, any kueue.x-k8s.io/* label/annotation, a tracked Kueue parent or Workload,
// was already tracked, or manageJobsWithoutQueueName is enabled in a non-system namespace).
func (s *Scraper) isKueueManagedWorkflowLocked(
	kind string,
	namespace string,
	rawName string,
	lq string,
	parentManaged bool,
	meta *metav1.ObjectMeta,
	extraMaps ...map[string]string,
) bool {
	if lq != "" || parentManaged {
		return true
	}
	wfKey := kind + "/" + namespace + "/" + rawName
	if _, tracked := s.workflowStates[wfKey]; tracked {
		return true
	}
	if _, hasWl := s.ownerToWorkload[wfKey]; hasWl {
		return true
	}
	if meta != nil {
		if hasKueueDomainEntry(meta.Labels) || hasKueueDomainEntry(meta.Annotations) {
			return true
		}
	}
	for _, em := range extraMaps {
		if hasKueueDomainEntry(em) {
			return true
		}
	}
	if s.manageJobsWithoutQueueName && !isSystemNamespaceName(namespace) {
		return true
	}
	return false
}

func (s *Scraper) handleJobWatchEvent(eventType watch.EventType, job *batchv1.Job) {
	s.handleJobWatchEventAt(eventType, job, time.Now().UTC(), false)
}

func (s *Scraper) handleJobWatchEventAt(eventType watch.EventType, job *batchv1.Job, now time.Time, isInitialSync bool) {
	if job == nil {
		return
	}
	lq := ExtractLocalQueueName(job)
	var parentJobSet string
	for _, ref := range job.OwnerReferences {
		if ref.Kind == "JobSet" {
			parentJobSet = ref.Name
			break
		}
	}
	if parentJobSet == "" && job.Labels != nil {
		parentJobSet = job.Labels["jobset.sigs.k8s.io/jobset-name"]
	}
	suspended := job.Spec.Suspend != nil && *job.Spec.Suspend
	wfKey := "Job/" + job.Namespace + "/" + job.Name

	s.mu.Lock()
	if parentJobSet != "" {
		if eventType == watch.Deleted {
			delete(s.jobToJobSet, job.Namespace+"/"+job.Name)
		} else {
			s.jobToJobSet[job.Namespace+"/"+job.Name] = parentJobSet
		}
	}
	_, alreadyTracked := s.workflowStates[wfKey]
	if isInitialSync && !alreadyTracked && isTerminalBatchJob(job) {
		s.mu.Unlock()
		return
	}
	parentManaged := false
	if parentJobSet != "" {
		if q, ok := s.jobSetQueues[job.Namespace+"/"+parentJobSet]; ok {
			parentManaged = true
			if lq == "" {
				lq = q
			}
		} else if _, parentTracked := s.workflowStates["JobSet/"+job.Namespace+"/"+parentJobSet]; parentTracked {
			parentManaged = true
		} else if _, parentWl := s.ownerToWorkload["JobSet/"+job.Namespace+"/"+parentJobSet]; parentWl {
			parentManaged = true
		}
	}
	if !s.isKueueManagedWorkflowLocked("Job", job.Namespace, job.Name, lq, parentManaged, &job.ObjectMeta, job.Spec.Template.Labels, job.Spec.Template.Annotations) {
		s.mu.Unlock()
		return
	}
	wlState := s.determineSuspendableWorkloadStateLocked("Job", job.Namespace, job.Name, suspended, parentJobSet)
	s.mu.Unlock()

	payload := SanitizeJob(job, s.cfg.PauseImage, s.hasher)
	s.recordWorkflowEvent(eventType, "Job", job.Name, job.Namespace, job.CreationTimestamp, lq, wlState, payload, now)
}

func (s *Scraper) handleJobSetWatchEvent(eventType watch.EventType, js *jobsetv1alpha2.JobSet) {
	s.handleJobSetWatchEventAt(eventType, js, time.Now().UTC(), false)
}

func (s *Scraper) handleJobSetWatchEventAt(eventType watch.EventType, js *jobsetv1alpha2.JobSet, now time.Time, isInitialSync bool) {
	if js == nil {
		return
	}
	lq := ExtractLocalQueueName(js)
	suspended := js.Spec.Suspend != nil && *js.Spec.Suspend
	wfKey := "JobSet/" + js.Namespace + "/" + js.Name

	var extraMaps []map[string]string
	for i := range js.Spec.ReplicatedJobs {
		rj := &js.Spec.ReplicatedJobs[i]
		extraMaps = append(extraMaps, rj.Template.Labels, rj.Template.Annotations, rj.Template.Spec.Template.Labels, rj.Template.Spec.Template.Annotations)
	}

	s.mu.Lock()
	if lq != "" {
		if eventType == watch.Deleted {
			delete(s.jobSetQueues, js.Namespace+"/"+js.Name)
		} else {
			s.jobSetQueues[js.Namespace+"/"+js.Name] = lq
		}
	}
	_, alreadyTracked := s.workflowStates[wfKey]
	if isInitialSync && !alreadyTracked && isTerminalJobSet(js) {
		s.mu.Unlock()
		return
	}
	if !s.isKueueManagedWorkflowLocked("JobSet", js.Namespace, js.Name, lq, false, &js.ObjectMeta, extraMaps...) {
		s.mu.Unlock()
		return
	}
	wlState := s.determineSuspendableWorkloadStateLocked("JobSet", js.Namespace, js.Name, suspended, "")
	s.mu.Unlock()

	payload := SanitizeJobSet(js, s.cfg.PauseImage, s.hasher)
	s.recordWorkflowEvent(eventType, "JobSet", js.Name, js.Namespace, js.CreationTimestamp, lq, wlState, payload, now)
}

func (s *Scraper) shouldSkipDeployment(deploy *appsv1.Deployment) bool {
	if IsKueueManagedDeployment(deploy) {
		return false
	}
	s.mu.Lock()
	_, alreadyTracked := s.workflowStates["Deployment/"+deploy.Namespace+"/"+deploy.Name]
	s.mu.Unlock()
	return !alreadyTracked
}

func (s *Scraper) handleDeploymentWatchEvent(eventType watch.EventType, deploy *appsv1.Deployment) {
	s.handleDeploymentWatchEventAt(eventType, deploy, time.Now().UTC())
}

func (s *Scraper) handleDeploymentWatchEventAt(eventType watch.EventType, deploy *appsv1.Deployment, now time.Time) {
	if deploy == nil || s.shouldSkipDeployment(deploy) {
		return
	}
	lq := ExtractDeploymentLocalQueueName(deploy)
	s.mu.Lock()
	wlState := s.determineGroupWorkloadStateLocked("Deployment", deploy.Namespace, deploy.Name, deploy.Status.ReadyReplicas, deploy.Status.Replicas)
	s.mu.Unlock()

	payload := SanitizeDeployment(deploy, s.cfg.PauseImage, s.hasher)
	s.recordWorkflowEvent(eventType, "Deployment", deploy.Name, deploy.Namespace, deploy.CreationTimestamp, lq, wlState, payload, now)
}

func (s *Scraper) handleLeaderWorkerSetWatchEvent(eventType watch.EventType, lws *leaderworkersetv1.LeaderWorkerSet) {
	s.handleLeaderWorkerSetWatchEventAt(eventType, lws, time.Now().UTC())
}

func (s *Scraper) handleLeaderWorkerSetWatchEventAt(eventType watch.EventType, lws *leaderworkersetv1.LeaderWorkerSet, now time.Time) {
	if lws == nil {
		return
	}
	lq := ExtractLocalQueueName(lws)
	if lq == "" && lws.Spec.LeaderWorkerTemplate.WorkerTemplate.Labels != nil {
		lq = lws.Spec.LeaderWorkerTemplate.WorkerTemplate.Labels[KueueQueueLabel]
	}
	var extraMaps []map[string]string
	extraMaps = append(extraMaps, lws.Spec.LeaderWorkerTemplate.WorkerTemplate.Labels, lws.Spec.LeaderWorkerTemplate.WorkerTemplate.Annotations)
	if lws.Spec.LeaderWorkerTemplate.LeaderTemplate != nil {
		extraMaps = append(extraMaps, lws.Spec.LeaderWorkerTemplate.LeaderTemplate.Labels, lws.Spec.LeaderWorkerTemplate.LeaderTemplate.Annotations)
	}
	s.mu.Lock()
	if !s.isKueueManagedWorkflowLocked("LeaderWorkerSet", lws.Namespace, lws.Name, lq, false, &lws.ObjectMeta, extraMaps...) {
		s.mu.Unlock()
		return
	}
	wlState := s.determineGroupWorkloadStateLocked("LeaderWorkerSet", lws.Namespace, lws.Name, lws.Status.ReadyReplicas, lws.Status.Replicas)
	s.mu.Unlock()

	payload := SanitizeLeaderWorkerSet(lws, s.cfg.PauseImage, s.hasher)
	s.recordWorkflowEvent(eventType, "LeaderWorkerSet", lws.Name, lws.Namespace, lws.CreationTimestamp, lq, wlState, payload, now)
}

func (s *Scraper) handleRayJobWatchEvent(eventType watch.EventType, rj *rayv1.RayJob) {
	s.handleRayJobWatchEventAt(eventType, rj, time.Now().UTC(), false)
}

func (s *Scraper) handleRayJobWatchEventAt(eventType watch.EventType, rj *rayv1.RayJob, now time.Time, isInitialSync bool) {
	if rj == nil {
		return
	}
	lq := ExtractLocalQueueName(rj)
	wfKey := "RayJob/" + rj.Namespace + "/" + rj.Name
	s.mu.Lock()
	_, alreadyTracked := s.workflowStates[wfKey]
	if isInitialSync && !alreadyTracked && isTerminalRayJob(rj) {
		s.mu.Unlock()
		return
	}
	if !s.isKueueManagedWorkflowLocked("RayJob", rj.Namespace, rj.Name, lq, false, &rj.ObjectMeta) {
		s.mu.Unlock()
		return
	}
	wlState := s.determineSuspendableWorkloadStateLocked("RayJob", rj.Namespace, rj.Name, rj.Spec.Suspend, "")
	s.mu.Unlock()

	payload := SanitizeRayJob(rj, s.cfg.PauseImage, s.hasher)
	s.recordWorkflowEvent(eventType, "RayJob", rj.Name, rj.Namespace, rj.CreationTimestamp, lq, wlState, payload, now)
}

func (s *Scraper) handleReplicaSetWatchEvent(eventType watch.EventType, rs *appsv1.ReplicaSet) {
	if rs == nil {
		return
	}
	var parentDeploy string
	for _, ref := range rs.OwnerReferences {
		if ref.Kind == "Deployment" {
			parentDeploy = ref.Name
			break
		}
	}
	if parentDeploy == "" {
		return
	}
	key := rs.Namespace + "/" + rs.Name
	s.mu.Lock()
	if eventType == watch.Deleted {
		delete(s.replicaSetToDeployment, key)
	} else if _, tracked := s.workflowStates["Deployment/"+rs.Namespace+"/"+parentDeploy]; tracked ||
		ExtractLocalQueueName(rs) != "" ||
		(rs.Labels != nil && rs.Labels[KueueManagedLabel] == "true") ||
		(rs.Spec.Template.Labels != nil && (rs.Spec.Template.Labels[KueueQueueLabel] != "" || rs.Spec.Template.Labels[KueueManagedLabel] == "true")) {
		s.replicaSetToDeployment[key] = parentDeploy
	}
	s.mu.Unlock()
}

func (s *Scraper) handleRayClusterWatchEvent(eventType watch.EventType, rc *rayv1.RayCluster) {
	s.handleRayClusterWatchEventAt(eventType, rc, time.Now().UTC())
}

func (s *Scraper) handleRayClusterWatchEventAt(eventType watch.EventType, rc *rayv1.RayCluster, now time.Time) {
	if rc == nil {
		return
	}
	lq := ExtractLocalQueueName(rc)
	var parentRayJob string
	for _, ref := range rc.OwnerReferences {
		if ref.Kind == "RayJob" {
			parentRayJob = ref.Name
			break
		}
	}
	suspended := rc.Spec.Suspend != nil && *rc.Spec.Suspend
	s.mu.Lock()
	parentManaged := false
	if parentRayJob != "" {
		if eventType == watch.Deleted {
			delete(s.rayClusterToRayJob, rc.Namespace+"/"+rc.Name)
		} else {
			s.rayClusterToRayJob[rc.Namespace+"/"+rc.Name] = parentRayJob
		}
		if _, pTracked := s.workflowStates["RayJob/"+rc.Namespace+"/"+parentRayJob]; pTracked {
			parentManaged = true
		} else if _, pWl := s.ownerToWorkload["RayJob/"+rc.Namespace+"/"+parentRayJob]; pWl {
			parentManaged = true
		}
	}
	if !s.isKueueManagedWorkflowLocked("RayCluster", rc.Namespace, rc.Name, lq, parentManaged, &rc.ObjectMeta) {
		s.mu.Unlock()
		return
	}
	wlState := s.determineSuspendableWorkloadStateLocked("RayCluster", rc.Namespace, rc.Name, suspended, "")
	s.mu.Unlock()

	payload := SanitizeRayCluster(rc, s.cfg.PauseImage, s.hasher)
	s.recordWorkflowEvent(eventType, "RayCluster", rc.Name, rc.Namespace, rc.CreationTimestamp, lq, wlState, payload, now)
}

func (s *Scraper) handleLeaderWorkerSetUnstructured(eventType watch.EventType, u *unstructured.Unstructured) {
	s.handleLeaderWorkerSetUnstructuredAt(eventType, u, time.Now().UTC(), false)
}

func (s *Scraper) handleLeaderWorkerSetUnstructuredAt(eventType watch.EventType, u *unstructured.Unstructured, now time.Time, _ bool) {
	if u == nil {
		return
	}
	var lws leaderworkersetv1.LeaderWorkerSet
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, &lws); err != nil {
		log.Printf("WARN failed to convert unstructured LeaderWorkerSet %s/%s: %v", u.GetNamespace(), u.GetName(), err)
		return
	}
	s.handleLeaderWorkerSetWatchEventAt(eventType, &lws, now)
}

func (s *Scraper) handleRayJobUnstructured(eventType watch.EventType, u *unstructured.Unstructured) {
	s.handleRayJobUnstructuredAt(eventType, u, time.Now().UTC(), false)
}

func (s *Scraper) handleRayJobUnstructuredAt(eventType watch.EventType, u *unstructured.Unstructured, now time.Time, isInitialSync bool) {
	if u == nil {
		return
	}
	var rj rayv1.RayJob
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, &rj); err != nil {
		log.Printf("WARN failed to convert unstructured RayJob %s/%s: %v", u.GetNamespace(), u.GetName(), err)
		return
	}
	s.handleRayJobWatchEventAt(eventType, &rj, now, isInitialSync)
}

func (s *Scraper) handleRayClusterUnstructured(eventType watch.EventType, u *unstructured.Unstructured) {
	s.handleRayClusterUnstructuredAt(eventType, u, time.Now().UTC(), false)
}

func (s *Scraper) handleRayClusterUnstructuredAt(eventType watch.EventType, u *unstructured.Unstructured, now time.Time, _ bool) {
	if u == nil {
		return
	}
	var rc rayv1.RayCluster
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, &rc); err != nil {
		log.Printf("WARN failed to convert unstructured RayCluster %s/%s: %v", u.GetNamespace(), u.GetName(), err)
		return
	}
	s.handleRayClusterWatchEventAt(eventType, &rc, now)
}

func resolveWorkloadOwner(wl *kueuev1beta2.Workload, hasher SensitiveDataHasher) (ownerKind, ownerName, ownerAPIVersion string) {
	if wl == nil {
		return "", "", ""
	}
	if wl.Annotations != nil && wl.Annotations[KueueIsGroupWorkloadAnnotation] == "true" {
		return "PodGroup", hasher.HashName(wl.Name), "v1"
	}
	for _, ref := range wl.OwnerReferences {
		if ref.Controller != nil && *ref.Controller {
			return ref.Kind, hasher.HashNameForKind(ref.Kind, ref.Name), ref.APIVersion
		}
	}
	if len(wl.OwnerReferences) > 0 {
		ref := wl.OwnerReferences[0]
		return ref.Kind, hasher.HashNameForKind(ref.Kind, ref.Name), ref.APIVersion
	}
	if wl.Annotations != nil {
		ownerName = wl.Annotations[KueueJobOwnerNameAnnotation]
		ownerGVK := wl.Annotations[KueueJobOwnerGVKAnnotation]
		if ownerGVK != "" {
			parts := strings.Split(ownerGVK, ".")
			ownerKind = parts[0]
			ownerAPIVersion = ownerGVK
		}
		if ownerName != "" {
			ownerName = hasher.HashNameForKind(ownerKind, ownerName)
		}
	}
	return ownerKind, ownerName, ownerAPIVersion
}

func extractWorkloadPriorityClassName(wl *kueuev1beta2.Workload) string {
	if wl == nil {
		return ""
	}
	if wl.Spec.PriorityClassRef != nil && wl.Spec.PriorityClassRef.Name != "" {
		return wl.Spec.PriorityClassRef.Name
	}
	if wl.Labels != nil && wl.Labels[KueuePriorityClassLabel] != "" {
		return wl.Labels[KueuePriorityClassLabel]
	}
	return ""
}

func workloadUsesTAS(wl *kueuev1beta2.Workload) bool {
	if wl == nil {
		return false
	}
	for i := range wl.Spec.PodSets {
		if wl.Spec.PodSets[i].TopologyRequest != nil {
			return true
		}
	}
	if wl.Status.Admission != nil {
		for i := range wl.Status.Admission.PodSetAssignments {
			if wl.Status.Admission.PodSetAssignments[i].TopologyAssignment != nil {
				return true
			}
		}
	}
	return false
}

// handleWorkloadWatchEvent processes a Kueue Workload event and emits Admitted,
// PartialExecution (when preempted), and FullExecution (when finished without preemption,
// or when clean completion wins a race against in-flight preemption) records into execution-time.json.
func (s *Scraper) handleWorkloadWatchEvent(eventType watch.EventType, wl *kueuev1beta2.Workload) {
	s.handleWorkloadWatchEventAt(eventType, wl, time.Now().UTC())
}

func (s *Scraper) handleDeletedWorkloadKeyLocked(key string, deletionTime *metav1.Time, now time.Time) *ExecutionTimeEvent {
	ownerRef, hasOwnerRef := s.workloadToOwner[key]
	st, hasSt := s.workloadStates[key]

	var delEv *ExecutionTimeEvent
	if hasSt && !st.recordedFull && !st.recordedTerminalFailure && st.currentlyAdmitted && !st.currentAdmittedTime.IsZero() {
		admittedAt := st.currentAdmittedTime
		delEnd := now
		if deletionTime != nil && !deletionTime.IsZero() {
			cand := deletionTime.Time.UTC()
			if !cand.Before(admittedAt) && !cand.After(now) {
				delEnd = cand
			}
		}
		durSec := delEnd.Sub(admittedAt).Seconds()
		if durSec < 0 {
			durSec = 0
		}
		durStr := time.Duration(durSec * float64(time.Second)).Round(time.Millisecond).String()
		endMeta := metav1.NewTime(delEnd)
		st.interruptionCount++
		st.currentlyAdmitted = false
		st.currentAdmittedTime = time.Time{}

		delEv = &ExecutionTimeEvent{
			Timestamp:     now,
			EventType:     ExecutionEventPartial,
			WorkloadState: WorkloadStateNotAdmitted,
			Payload: ExecutionTimePayload{
				ExecutionType:             "Partial",
				WorkloadName:              st.workloadName,
				Namespace:                 st.namespace,
				OwnerKind:                 st.ownerKind,
				OwnerName:                 st.ownerName,
				OwnerAPIVersion:           st.ownerAPIVersion,
				LocalQueueName:            st.localQueueName,
				ClusterQueueName:          st.clusterQueueName,
				PriorityClassName:         st.priorityClassName,
				Priority:                  st.priority,
				PodSetAssignments:         st.podSetAssignments,
				UnhealthyNodes:            st.unhealthyNodes,
				CreationTime:              st.creationTime,
				AdmittedTime:              metav1.NewTime(admittedAt),
				EndTime:                   &endMeta,
				ExecutionDurationSeconds:  &durSec,
				ExecutionDuration:         durStr,
				Preempted:                 false,
				PreemptionCount:           st.preemptionCount,
				InterruptionReason:        InterruptionReasonDeleted,
				InterruptionCount:         st.interruptionCount,
				TrueDurationSeconds:       st.trueDurationSeconds,
				MissingDurationMultiplier: st.missingDurationMultiplier,
				Reason:                    "WorkloadDeleted",
				Message:                   "Workload object was deleted while admitted and executing",
			},
		}
	}

	delete(s.workloadStates, key)
	delete(s.workloadToOwner, key)
	delete(s.workloadIsTAS, key)
	delete(s.workloadPodSets, key)
	if hasOwnerRef && ownerRef.kind != "" && ownerRef.name != "" {
		parts := strings.SplitN(key, "/", 2)
		if len(parts) == 2 {
			ns, rawWlName := parts[0], parts[1]
			oKey := ownerRef.kind + "/" + ns + "/" + ownerRef.name
			if s.ownerToWorkload[oKey] == rawWlName {
				delete(s.ownerToWorkload, oKey)
				delete(s.ownerStates, oKey)
			}
		}
	}
	return delEv
}

func (s *Scraper) handleWorkloadWatchEventAt(eventType watch.EventType, wl *kueuev1beta2.Workload, now time.Time) {
	if wl == nil {
		return
	}
	key := wl.Namespace + "/" + wl.Name
	if eventType == watch.Deleted {
		s.mu.Lock()
		delEv := s.handleDeletedWorkloadKeyLocked(key, wl.DeletionTimestamp, now)
		s.mu.Unlock()
		if delEv != nil && s.execTimeLog != nil {
			if err := s.execTimeLog.Append(*delEv); err != nil {
				log.Printf("ERROR writing deleted workload PartialExecution event to %s: %v", s.cfg.ExecutionTimeFile, err)
			}
		}
		return
	}

	rawOwnerKind, rawOwnerName, _ := resolveWorkloadOwner(wl, NoopHasher{})
	if rawOwnerKind == "" || rawOwnerName == "" {
		return
	}
	ownerKey := rawOwnerKind + "/" + wl.Namespace + "/" + rawOwnerName
	ownerKind, ownerName, ownerAPIVersion := resolveWorkloadOwner(wl, s.hasher)
	workloadName := s.hasher.HashName(wl.Name)
	localQueueName := string(wl.Spec.QueueName)
	priorityClassName := extractWorkloadPriorityClassName(wl)
	var priorityCopy *int32
	if wl.Spec.Priority != nil {
		p := *wl.Spec.Priority
		priorityCopy = &p
	}
	var podSetAssignments []kueuev1beta2.PodSetAssignment
	if wl.Status.Admission != nil && len(wl.Status.Admission.PodSetAssignments) > 0 {
		podSetAssignments = SanitizePodSetAssignments(wl.Status.Admission.PodSetAssignments, s.hasher)
	}
	var unhealthyNodes []string
	if len(wl.Status.UnhealthyNodes) > 0 {
		unhealthyNodes = make([]string, 0, len(wl.Status.UnhealthyNodes))
		for _, un := range wl.Status.UnhealthyNodes {
			if un.Name == "" {
				continue
			}
			nName := un.Name
			if s.hasher.ShouldHashLabel("kubernetes.io/hostname") {
				nName = s.hasher.HashName(nName)
			}
			unhealthyNodes = append(unhealthyNodes, nName)
		}
	}
	trueDurSec := parsePositiveFloatAnnotation(wl.Annotations, "kueue-optimizer.gke.io/true-execution-time-seconds")
	missingMult := parsePositiveFloatAnnotation(wl.Annotations, "kueue-optimizer.gke.io/missing-duration-multiplier")

	usesTAS := workloadUsesTAS(wl)
	var podSetNames []string
	if len(wl.Spec.PodSets) > 0 {
		podSetNames = make([]string, len(wl.Spec.PodSets))
		for i := range wl.Spec.PodSets {
			podSetNames[i] = string(wl.Spec.PodSets[i].Name)
		}
	}

	admittedCond := apimeta.FindStatusCondition(wl.Status.Conditions, kueuev1beta2.WorkloadAdmitted)
	quotaReservedCond := apimeta.FindStatusCondition(wl.Status.Conditions, kueuev1beta2.WorkloadQuotaReserved)
	preemptedCond := apimeta.FindStatusCondition(wl.Status.Conditions, kueuev1beta2.WorkloadPreempted)
	evictedCond := apimeta.FindStatusCondition(wl.Status.Conditions, kueuev1beta2.WorkloadEvicted)
	finishedCond := apimeta.FindStatusCondition(wl.Status.Conditions, kueuev1beta2.WorkloadFinished)

	isAdmitted := admittedCond != nil && admittedCond.Status == metav1.ConditionTrue
	isEvicted := evictedCond != nil && evictedCond.Status == metav1.ConditionTrue
	isPreempted := false
	var interruptTime time.Time
	var interruptReason, interruptMsg, interruptionCategory string

	if preemptedCond != nil && preemptedCond.Status == metav1.ConditionTrue {
		isPreempted = true
		interruptTime = preemptedCond.LastTransitionTime.Time.UTC()
		interruptReason = preemptedCond.Reason
		interruptMsg = preemptedCond.Message
		interruptionCategory = InterruptionReasonPreempted
	} else if isEvicted &&
		(evictedCond.Reason == kueuev1beta2.WorkloadEvictedByPreemption || strings.Contains(evictedCond.Reason, "Preempt")) {
		isPreempted = true
		interruptTime = evictedCond.LastTransitionTime.Time.UTC()
		interruptReason = evictedCond.Reason
		interruptMsg = evictedCond.Message
		interruptionCategory = InterruptionReasonPreempted
	} else if isEvicted {
		interruptTime = evictedCond.LastTransitionTime.Time.UTC()
		interruptReason = evictedCond.Reason
		interruptMsg = evictedCond.Message
		switch {
		case evictedCond.Reason == kueuev1beta2.WorkloadEvictedDueToNodeFailures ||
			strings.Contains(evictedCond.Reason, "NodeFailure") ||
			strings.Contains(evictedCond.Reason, "FailedNode"):
			interruptionCategory = InterruptionReasonNodeFailure
		case evictedCond.Reason == kueuev1beta2.WorkloadEvictedByPodsReadyTimeout ||
			strings.Contains(evictedCond.Reason, "PodsReadyTimeout") ||
			strings.Contains(evictedCond.Reason, "RequeuingLimitExceeded"):
			interruptionCategory = InterruptionReasonPodsReadyTimeout
		default:
			interruptionCategory = InterruptionReasonEvicted
		}
	}
	isFinished := finishedCond != nil && finishedCond.Status == metav1.ConditionTrue

	var events []ExecutionTimeEvent

	s.mu.Lock()
	s.ownerToWorkload[ownerKey] = wl.Name
	for _, ref := range wl.OwnerReferences {
		if ref.Kind != "" && ref.Name != "" {
			s.ownerToWorkload[ref.Kind+"/"+wl.Namespace+"/"+ref.Name] = wl.Name
		}
	}
	s.workloadToOwner[key] = workloadOwnerRef{kind: rawOwnerKind, name: rawOwnerName}
	s.workloadIsTAS[key] = usesTAS
	if len(podSetNames) > 0 {
		s.workloadPodSets[key] = podSetNames
	}
	ownerUnsuspended := isAdmitted
	if wfSt, ok := s.workflowStates[ownerKey]; ok {
		if wfSt.workloadState != WorkloadStateAdmitted {
			ownerUnsuspended = false
		}
		if trueDurSec == nil {
			trueDurSec = extractPositiveFloatAnnotationFromPayload(wfSt.lastPayload, "kueue-optimizer.gke.io/true-execution-time-seconds")
		}
		if missingMult == nil {
			missingMult = extractPositiveFloatAnnotationFromPayload(wfSt.lastPayload, "kueue-optimizer.gke.io/missing-duration-multiplier")
		}
	}
	// Allow FullExecution when Finished=True transitioned cleanly (Reason == Succeeded, or Workload
	// completed without pods/owner being suspended) even when Preempted=True remains latched on wl.Status.Conditions.
	finishedCleanly := isFinished && (finishedCond.Reason == kueuev1beta2.WorkloadFinishedReasonSucceeded ||
		(ownerUnsuspended &&
			finishedCond.Reason != kueuev1beta2.WorkloadFinishedReasonFailed &&
			finishedCond.Reason != kueuev1beta2.WorkloadFinishedReasonOwnerNotFound &&
			finishedCond.Reason != kueuev1beta2.WorkloadFinishedReasonOutOfSync))

	switch {
	case finishedCleanly:
		s.ownerStates[ownerKey] = WorkloadStateAdmitted
	case isPreempted || isEvicted || (isFinished && !finishedCleanly):
		s.ownerStates[ownerKey] = WorkloadStateNotAdmitted
	case isAdmitted:
		s.ownerStates[ownerKey] = WorkloadStateAdmitted
	case (quotaReservedCond != nil && quotaReservedCond.Status == metav1.ConditionTrue) || wl.Status.Admission != nil:
		s.ownerStates[ownerKey] = WorkloadStateAdmissionInProgress
	default:
		s.ownerStates[ownerKey] = WorkloadStateNotAdmitted
	}

	st, ok := s.workloadStates[key]
	if !ok {
		st = &workloadExecState{
			recordedPartialKeys: make(map[string]bool),
		}
		s.workloadStates[key] = st
	}
	st.workloadName = workloadName
	st.namespace = wl.Namespace
	if ownerKind != "" {
		st.ownerKind = ownerKind
	}
	if ownerName != "" {
		st.ownerName = ownerName
	}
	if ownerAPIVersion != "" {
		st.ownerAPIVersion = ownerAPIVersion
	}
	if localQueueName != "" {
		st.localQueueName = localQueueName
	}
	if priorityClassName != "" {
		st.priorityClassName = priorityClassName
	}
	if priorityCopy != nil {
		st.priority = priorityCopy
	}
	if len(podSetAssignments) > 0 {
		st.podSetAssignments = podSetAssignments
	}
	if len(unhealthyNodes) > 0 {
		st.unhealthyNodes = unhealthyNodes
	}
	if !wl.CreationTimestamp.IsZero() {
		st.creationTime = wl.CreationTimestamp
	}
	if trueDurSec != nil {
		st.trueDurationSeconds = trueDurSec
	}
	if missingMult != nil {
		st.missingDurationMultiplier = missingMult
	}
	if wl.Status.Admission != nil && wl.Status.Admission.ClusterQueue != "" {
		st.clusterQueueName = string(wl.Status.Admission.ClusterQueue)
	}
	clusterQueueName := st.clusterQueueName
	effectivePodSets := podSetAssignments
	if len(effectivePodSets) == 0 {
		effectivePodSets = st.podSetAssignments
	}
	effectiveUnhealthy := unhealthyNodes
	if len(effectiveUnhealthy) == 0 {
		effectiveUnhealthy = st.unhealthyNodes
	}

	hasAccumulatedPast := wl.Status.AccumulatedPastExecutionTimeSeconds != nil && *wl.Status.AccumulatedPastExecutionTimeSeconds > 0
	evictedFromExecution := isEvicted && (evictedCond.Reason == kueuev1beta2.WorkloadEvictedDueToNodeFailures ||
		evictedCond.Reason == kueuev1beta2.WorkloadEvictedByPodsReadyTimeout ||
		evictedCond.Reason == kueuev1beta2.WorkloadEvictedByPreemption ||
		strings.Contains(evictedCond.Reason, "NodeFailure") ||
		strings.Contains(evictedCond.Reason, "FailedNode") ||
		strings.Contains(evictedCond.Reason, "PodsReadyTimeout") ||
		strings.Contains(evictedCond.Reason, "Preempt"))
	hadRevokedReservation := admittedCond != nil && admittedCond.Reason == kueuev1beta2.WorkloadAdmittedReasonNoReservation

	wasEverAdmitted := isAdmitted ||
		!st.currentAdmittedTime.IsZero() ||
		!st.lastAdmittedTime.IsZero() ||
		(admittedCond != nil && admittedCond.Status == metav1.ConditionTrue && !admittedCond.LastTransitionTime.IsZero()) ||
		hasAccumulatedPast ||
		isPreempted ||
		evictedFromExecution ||
		hadRevokedReservation

	if interruptionCategory == "" && isFinished && !finishedCleanly && wasEverAdmitted {
		interruptionCategory = InterruptionReasonFailed
		interruptTime = finishedCond.LastTransitionTime.Time.UTC()
		interruptReason = finishedCond.Reason
		interruptMsg = finishedCond.Message
	}

	// 1. Check if Workload is currently Admitted.
	// During Kueue's two-phase eviction, Evicted=True / Preempted=True are set before pods
	// terminate and before Admitted flips to False; skip emitting Admitted while eviction is active.
	evictionActive := isPreempted || isEvicted || (isFinished && !finishedCleanly)
	if isAdmitted && !evictionActive && !st.recordedFull && !st.recordedTerminalFailure {
		admittedAt := admittedCond.LastTransitionTime.Time.UTC()
		if admittedAt.IsZero() {
			admittedAt = now
		}
		alreadyRecorded := !st.lastRecordedAdmittedTime.IsZero() && admittedAt.Equal(st.lastRecordedAdmittedTime)
		alreadyConsumedByInterruption := st.interruptionCount > 0 && !st.lastAdmittedTime.IsZero() && admittedAt.Equal(st.lastAdmittedTime)
		if !alreadyRecorded && !alreadyConsumedByInterruption {
			st.currentlyAdmitted = true
			st.currentAdmittedTime = admittedAt
			st.lastAdmittedTime = admittedAt
			st.lastRecordedAdmittedTime = admittedAt
			events = append(events, ExecutionTimeEvent{
				Timestamp:     now,
				EventType:     ExecutionEventAdmitted,
				WorkloadState: WorkloadStateAdmitted,
				Payload: ExecutionTimePayload{
					WorkloadName:              workloadName,
					Namespace:                 wl.Namespace,
					OwnerKind:                 ownerKind,
					OwnerName:                 ownerName,
					OwnerAPIVersion:           ownerAPIVersion,
					LocalQueueName:            localQueueName,
					ClusterQueueName:          clusterQueueName,
					PriorityClassName:         priorityClassName,
					Priority:                  priorityCopy,
					PodSetAssignments:         effectivePodSets,
					UnhealthyNodes:            effectiveUnhealthy,
					CreationTime:              wl.CreationTimestamp,
					AdmittedTime:              metav1.NewTime(admittedAt),
					Preempted:                 false,
					PreemptionCount:           st.preemptionCount,
					InterruptionCount:         st.interruptionCount,
					TrueDurationSeconds:       st.trueDurationSeconds,
					MissingDurationMultiplier: st.missingDurationMultiplier,
					Reason:                    admittedCond.Reason,
					Message:                   admittedCond.Message,
				},
			})
		}
	}

	// 2. Check if Workload execution was interrupted (Preempted, NodeFailure, PodsReadyTimeout, Evicted, or Failed)
	// -> record Partial Execution ONLY when the workload was actually admitted (never for pending AdmissionCheck/Deactivated evictions).
	if interruptionCategory != "" && wasEverAdmitted && !finishedCleanly && !st.recordedFull && !st.recordedTerminalFailure {
		if interruptTime.IsZero() {
			interruptTime = now
		}
		admittedAt := st.currentAdmittedTime
		observedAdmittedAt := !admittedAt.IsZero()
		if !observedAdmittedAt && isAdmitted && !admittedCond.LastTransitionTime.IsZero() {
			cand := admittedCond.LastTransitionTime.Time.UTC()
			if st.interruptionCount == 0 || !cand.Equal(st.lastAdmittedTime) {
				admittedAt = cand
				observedAdmittedAt = true
			}
		}
		canMeasureInterruption := observedAdmittedAt || hasAccumulatedPast || isFinished
		partialKey := interruptTime.Format(time.RFC3339Nano) + "/" + interruptionCategory + "/" + interruptReason
		if !st.recordedPartialKeys[partialKey] {
			st.recordedPartialKeys[partialKey] = true
			if canMeasureInterruption {
				st.interruptionCount++
				if isPreempted {
					st.preemptionCount++
				}
				if isFinished && !finishedCleanly {
					st.recordedTerminalFailure = true
				}
				if observedAdmittedAt && st.currentAdmittedTime.IsZero() {
					st.lastAdmittedTime = admittedAt
				}
				if !observedAdmittedAt {
					if st.interruptionCount == 1 {
						if hasAccumulatedPast {
							admittedAt = interruptTime.Add(-time.Duration(*wl.Status.AccumulatedPastExecutionTimeSeconds) * time.Second)
						} else if admittedCond != nil && admittedCond.Status == metav1.ConditionTrue && !admittedCond.LastTransitionTime.IsZero() && !admittedCond.LastTransitionTime.Time.After(interruptTime) {
							admittedAt = admittedCond.LastTransitionTime.Time.UTC()
						} else if isFinished && !wl.CreationTimestamp.IsZero() && !wl.CreationTimestamp.Time.After(interruptTime) {
							admittedAt = wl.CreationTimestamp.Time.UTC()
						} else {
							admittedAt = interruptTime
						}
					} else {
						admittedAt = interruptTime
					}
				}

				durSec := interruptTime.Sub(admittedAt).Seconds()
				if durSec < 0 {
					durSec = 0
				}
				if durSec == 0 && !observedAdmittedAt && st.interruptionCount == 1 && hasAccumulatedPast {
					durSec = float64(*wl.Status.AccumulatedPastExecutionTimeSeconds)
				}
				durStr := time.Duration(durSec * float64(time.Second)).Round(time.Millisecond).String()
				endMetaTime := metav1.NewTime(interruptTime)

				st.currentlyAdmitted = isAdmitted
				st.currentAdmittedTime = time.Time{}

				events = append(events, ExecutionTimeEvent{
					Timestamp:     now,
					EventType:     ExecutionEventPartial,
					WorkloadState: WorkloadStateNotAdmitted,
					Payload: ExecutionTimePayload{
						ExecutionType:             "Partial",
						WorkloadName:              workloadName,
						Namespace:                 wl.Namespace,
						OwnerKind:                 ownerKind,
						OwnerName:                 ownerName,
						OwnerAPIVersion:           ownerAPIVersion,
						LocalQueueName:            localQueueName,
						ClusterQueueName:          clusterQueueName,
						PriorityClassName:         priorityClassName,
						Priority:                  priorityCopy,
						PodSetAssignments:         effectivePodSets,
						UnhealthyNodes:            effectiveUnhealthy,
						CreationTime:              wl.CreationTimestamp,
						AdmittedTime:              metav1.NewTime(admittedAt),
						EndTime:                   &endMetaTime,
						ExecutionDurationSeconds:  &durSec,
						ExecutionDuration:         durStr,
						Preempted:                 isPreempted,
						PreemptionCount:           st.preemptionCount,
						InterruptionReason:        interruptionCategory,
						InterruptionCount:         st.interruptionCount,
						TrueDurationSeconds:       st.trueDurationSeconds,
						MissingDurationMultiplier: st.missingDurationMultiplier,
						Reason:                    interruptReason,
						Message:                   interruptMsg,
					},
				})
			} else {
				st.currentlyAdmitted = isAdmitted
				st.currentAdmittedTime = time.Time{}
			}
		}
	}

	// 3. Check if Workload Finished cleanly -> record Full Execution.
	if isFinished && finishedCleanly && !st.recordedFull {
		finishedAt := finishedCond.LastTransitionTime.Time.UTC()
		if finishedAt.IsZero() {
			finishedAt = now
		}

		admittedAt := st.currentAdmittedTime
		if admittedAt.IsZero() {
			admittedAt = st.lastAdmittedTime
		}
		if admittedAt.IsZero() {
			if admittedCond != nil && !admittedCond.LastTransitionTime.IsZero() {
				admittedAt = admittedCond.LastTransitionTime.Time.UTC()
			} else if !wl.CreationTimestamp.IsZero() {
				admittedAt = wl.CreationTimestamp.Time.UTC()
			} else {
				admittedAt = finishedAt
			}
		}

		// If the workload was admitted and finished before its admission was separately observed,
		// emit the admission event first.
		if st.lastRecordedAdmittedTime.IsZero() {
			st.lastAdmittedTime = admittedAt
			st.lastRecordedAdmittedTime = admittedAt
			events = append(events, ExecutionTimeEvent{
				Timestamp:     now,
				EventType:     ExecutionEventAdmitted,
				WorkloadState: WorkloadStateAdmitted,
				Payload: ExecutionTimePayload{
					WorkloadName:              workloadName,
					Namespace:                 wl.Namespace,
					OwnerKind:                 ownerKind,
					OwnerName:                 ownerName,
					OwnerAPIVersion:           ownerAPIVersion,
					LocalQueueName:            localQueueName,
					ClusterQueueName:          clusterQueueName,
					PriorityClassName:         priorityClassName,
					Priority:                  priorityCopy,
					PodSetAssignments:         effectivePodSets,
					UnhealthyNodes:            effectiveUnhealthy,
					CreationTime:              wl.CreationTimestamp,
					AdmittedTime:              metav1.NewTime(admittedAt),
					Preempted:                 false,
					PreemptionCount:           st.preemptionCount,
					InterruptionCount:         st.interruptionCount,
					TrueDurationSeconds:       st.trueDurationSeconds,
					MissingDurationMultiplier: st.missingDurationMultiplier,
					Reason:                    "Admitted",
				},
			})
		}

		durSec := finishedAt.Sub(admittedAt).Seconds()
		if durSec < 0 {
			durSec = 0
		}
		durStr := time.Duration(durSec * float64(time.Second)).Round(time.Millisecond).String()
		endMetaTime := metav1.NewTime(finishedAt)

		st.recordedFull = true
		st.currentlyAdmitted = isAdmitted

		events = append(events, ExecutionTimeEvent{
			Timestamp:     now,
			EventType:     ExecutionEventFull,
			WorkloadState: WorkloadStateAdmitted,
			Payload: ExecutionTimePayload{
				ExecutionType:             "Full",
				WorkloadName:              workloadName,
				Namespace:                 wl.Namespace,
				OwnerKind:                 ownerKind,
				OwnerName:                 ownerName,
				OwnerAPIVersion:           ownerAPIVersion,
				LocalQueueName:            localQueueName,
				ClusterQueueName:          clusterQueueName,
				PriorityClassName:         priorityClassName,
				Priority:                  priorityCopy,
				PodSetAssignments:         effectivePodSets,
				UnhealthyNodes:            effectiveUnhealthy,
				CreationTime:              wl.CreationTimestamp,
				AdmittedTime:              metav1.NewTime(admittedAt),
				EndTime:                   &endMetaTime,
				ExecutionDurationSeconds:  &durSec,
				ExecutionDuration:         durStr,
				Preempted:                 false,
				PreemptionCount:           st.preemptionCount,
				InterruptionCount:         st.interruptionCount,
				TrueDurationSeconds:       st.trueDurationSeconds,
				MissingDurationMultiplier: st.missingDurationMultiplier,
				Reason:                    finishedCond.Reason,
				Message:                   finishedCond.Message,
			},
		})
	}

	if !isAdmitted && !st.recordedFull {
		st.currentlyAdmitted = false
		st.currentAdmittedTime = time.Time{}
	}
	s.mu.Unlock()

	for _, ev := range events {
		if err := s.execTimeLog.Append(ev); err != nil {
			log.Printf("ERROR writing Workload execution event to %s: %v", s.cfg.ExecutionTimeFile, err)
		}
	}
}

func parsePositiveFloatAnnotation(ann map[string]string, key string) *float64 {
	if len(ann) == 0 {
		return nil
	}
	raw := strings.TrimSpace(ann[key])
	if raw == "" {
		return nil
	}
	raw = strings.TrimSuffix(raw, "s")
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil || v <= 0 {
		return nil
	}
	return &v
}

func extractPositiveFloatAnnotationFromPayload(payload any, key string) *float64 {
	m, ok := payload.(map[string]any)
	if !ok {
		return nil
	}
	meta, ok := m["metadata"].(map[string]any)
	if !ok {
		return nil
	}
	ann, ok := meta["annotations"].(map[string]any)
	if !ok {
		return nil
	}
	val, ok := ann[key]
	if !ok || val == nil {
		return nil
	}
	strVal, ok := val.(string)
	if !ok {
		return nil
	}
	strVal = strings.TrimSuffix(strings.TrimSpace(strVal), "s")
	f, err := strconv.ParseFloat(strVal, 64)
	if err != nil || f <= 0 {
		return nil
	}
	return &f
}

// flushInFlightWorkloadsAsPartial emits PartialExecution (interruptionReason="ScrapeEnded")
// for any Workload that was Admitted and still running when scraping terminated, so downstream
// replay knows the observed elapsed duration up to scrape cutoff and can scale/override it.
func (s *Scraper) flushInFlightWorkloadsAsPartial(shutdownTime time.Time) {
	if s.execTimeLog == nil {
		return
	}
	now := shutdownTime.UTC()
	var events []ExecutionTimeEvent

	s.mu.Lock()
	keys := make([]string, 0, len(s.workloadStates))
	for k := range s.workloadStates {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		st := s.workloadStates[k]
		if st == nil || st.recordedFull || st.recordedTerminalFailure || !st.currentlyAdmitted || st.currentAdmittedTime.IsZero() {
			continue
		}
		partialKey := "ScrapeEnded/" + st.currentAdmittedTime.Format(time.RFC3339Nano)
		if st.recordedPartialKeys[partialKey] {
			continue
		}
		st.recordedPartialKeys[partialKey] = true
		st.interruptionCount++

		if ref, ok := s.workloadToOwner[k]; ok && ref.kind != "" && ref.name != "" {
			ownerKey := ref.kind + "/" + st.namespace + "/" + ref.name
			if wfSt, ok := s.workflowStates[ownerKey]; ok {
				if st.trueDurationSeconds == nil {
					st.trueDurationSeconds = extractPositiveFloatAnnotationFromPayload(wfSt.lastPayload, "kueue-optimizer.gke.io/true-execution-time-seconds")
				}
				if st.missingDurationMultiplier == nil {
					st.missingDurationMultiplier = extractPositiveFloatAnnotationFromPayload(wfSt.lastPayload, "kueue-optimizer.gke.io/missing-duration-multiplier")
				}
			}
		}

		durSec := now.Sub(st.currentAdmittedTime).Seconds()
		if durSec < 0 {
			durSec = 0
		}
		durStr := time.Duration(durSec * float64(time.Second)).Round(time.Millisecond).String()
		endMeta := metav1.NewTime(now)

		events = append(events, ExecutionTimeEvent{
			Timestamp:     now,
			EventType:     ExecutionEventPartial,
			WorkloadState: WorkloadStateAdmitted,
			Payload: ExecutionTimePayload{
				ExecutionType:             "Partial",
				WorkloadName:              st.workloadName,
				Namespace:                 st.namespace,
				OwnerKind:                 st.ownerKind,
				OwnerName:                 st.ownerName,
				OwnerAPIVersion:           st.ownerAPIVersion,
				LocalQueueName:            st.localQueueName,
				ClusterQueueName:          st.clusterQueueName,
				PriorityClassName:         st.priorityClassName,
				Priority:                  st.priority,
				PodSetAssignments:         st.podSetAssignments,
				UnhealthyNodes:            st.unhealthyNodes,
				CreationTime:              st.creationTime,
				AdmittedTime:              metav1.NewTime(st.currentAdmittedTime),
				EndTime:                   &endMeta,
				ExecutionDurationSeconds:  &durSec,
				ExecutionDuration:         durStr,
				Preempted:                 false,
				PreemptionCount:           st.preemptionCount,
				InterruptionReason:        InterruptionReasonScrapeEnded,
				InterruptionCount:         st.interruptionCount,
				TrueDurationSeconds:       st.trueDurationSeconds,
				MissingDurationMultiplier: st.missingDurationMultiplier,
				Reason:                    InterruptionReasonScrapeEnded,
				Message:                   "Workload was still admitted and executing when cluster-scraper stopped",
			},
		})
	}
	s.mu.Unlock()

	for _, ev := range events {
		if err := s.execTimeLog.Append(ev); err != nil {
			log.Printf("ERROR writing ScrapeEnded partial execution event to %s: %v", s.cfg.ExecutionTimeFile, err)
		}
	}
}

func compareMetaOrder(a, b metav1.Object) bool {
	ta := a.GetCreationTimestamp().Time
	tb := b.GetCreationTimestamp().Time
	if !ta.Equal(tb) {
		return ta.Before(tb)
	}
	if a.GetNamespace() != b.GetNamespace() {
		return a.GetNamespace() < b.GetNamespace()
	}
	return a.GetName() < b.GetName()
}

func (s *Scraper) syncInitialWorkloads(ctx context.Context, now time.Time) string {
	if s.kueueClient == nil || ctx.Err() != nil {
		return ""
	}
	list, err := s.kueueClient.KueueV1beta2().Workloads("").List(ctx, metav1.ListOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			log.Printf("WARN failed to list Kueue Workloads at startup: %v", err)
		}
		return ""
	}
	sort.Slice(list.Items, func(i, j int) bool {
		return compareMetaOrder(&list.Items[i], &list.Items[j])
	})
	for i := range list.Items {
		s.handleWorkloadWatchEventAt(watch.Added, &list.Items[i], now)
	}
	if list.ResourceVersion != "" {
		return list.ResourceVersion
	}
	return "0"
}

func (s *Scraper) syncInitialJobSets(ctx context.Context, now time.Time) string {
	if s.jobsetClient == nil || ctx.Err() != nil {
		return ""
	}
	list, err := s.jobsetClient.JobsetV1alpha2().JobSets("").List(ctx, metav1.ListOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			log.Printf("WARN failed to list JobSets at startup: %v", err)
		}
		return ""
	}
	sort.Slice(list.Items, func(i, j int) bool {
		return compareMetaOrder(&list.Items[i], &list.Items[j])
	})
	for i := range list.Items {
		s.handleJobSetWatchEventAt(watch.Added, &list.Items[i], now, true)
	}
	if list.ResourceVersion != "" {
		return list.ResourceVersion
	}
	return "0"
}

func (s *Scraper) syncInitialJobs(ctx context.Context, now time.Time) string {
	if s.kubeClient == nil || ctx.Err() != nil {
		return ""
	}
	list, err := s.kubeClient.BatchV1().Jobs("").List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Printf("WARN failed to list Jobs at startup: %v", err)
		return ""
	}
	sort.Slice(list.Items, func(i, j int) bool {
		return compareMetaOrder(&list.Items[i], &list.Items[j])
	})
	for i := range list.Items {
		s.handleJobWatchEventAt(watch.Added, &list.Items[i], now, true)
	}
	if list.ResourceVersion != "" {
		return list.ResourceVersion
	}
	return "0"
}

func (s *Scraper) syncInitialDeployments(ctx context.Context, now time.Time) string {
	if s.kubeClient == nil || ctx.Err() != nil {
		return ""
	}
	list, err := s.kubeClient.AppsV1().Deployments("").List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Printf("WARN failed to list Deployments at startup: %v", err)
		return ""
	}
	sort.Slice(list.Items, func(i, j int) bool {
		return compareMetaOrder(&list.Items[i], &list.Items[j])
	})
	for i := range list.Items {
		s.handleDeploymentWatchEventAt(watch.Added, &list.Items[i], now)
	}
	if list.ResourceVersion != "" {
		return list.ResourceVersion
	}
	return "0"
}

func (s *Scraper) syncInitialReplicaSets(ctx context.Context) string {
	if s.kubeClient == nil || ctx.Err() != nil {
		return ""
	}
	list, err := s.kubeClient.AppsV1().ReplicaSets("").List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Printf("WARN failed to list ReplicaSets at startup: %v", err)
		return ""
	}
	for i := range list.Items {
		s.handleReplicaSetWatchEvent(watch.Added, &list.Items[i])
	}
	if list.ResourceVersion != "" {
		return list.ResourceVersion
	}
	return "0"
}

func (s *Scraper) syncInitialPods(ctx context.Context, now time.Time) (string, []corev1.Pod) {
	if s.kubeClient == nil || ctx.Err() != nil {
		return "", nil
	}
	list, err := s.kubeClient.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Printf("WARN failed to list Pods at startup: %v", err)
		return "", nil
	}
	sort.Slice(list.Items, func(i, j int) bool {
		return compareMetaOrder(&list.Items[i], &list.Items[j])
	})
	for i := range list.Items {
		s.handlePodWatchEventAt(watch.Added, &list.Items[i], now, true)
	}
	if list.ResourceVersion != "" {
		return list.ResourceVersion, list.Items
	}
	return "0", list.Items
}

func (s *Scraper) syncInitialDynamicResource(
	ctx context.Context,
	gvr schema.GroupVersionResource,
	kindName string,
	handler func(watch.EventType, *unstructured.Unstructured),
) string {
	if s.dynamicClient == nil || ctx.Err() != nil {
		return ""
	}
	list, err := s.dynamicClient.Resource(gvr).Namespace(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			log.Printf("WARN failed to list %s at startup: %v", kindName, err)
		}
		return ""
	}
	sort.Slice(list.Items, func(i, j int) bool {
		return compareMetaOrder(&list.Items[i], &list.Items[j])
	})
	for i := range list.Items {
		handler(watch.Added, &list.Items[i])
	}
	if rv := list.GetResourceVersion(); rv != "" {
		return rv
	}
	return "0"
}

func watchOptionsFromSentinel(rv string) metav1.ListOptions {
	if rv == "" || rv == "0" {
		return metav1.ListOptions{AllowWatchBookmarks: true}
	}
	return metav1.ListOptions{
		ResourceVersion:     rv,
		AllowWatchBookmarks: true,
	}
}

func isWatchExpiredError(err error) bool {
	return apierrors.IsResourceExpired(err) || apierrors.IsGone(err)
}

func extractWatchRVAndExpired(ev watch.Event) (string, bool) {
	if ev.Type == watch.Error {
		if ev.Object != nil {
			if statusErr := apierrors.FromObject(ev.Object); statusErr != nil && isWatchExpiredError(statusErr) {
				return "", true
			}
		}
		return "", false
	}
	if metaObj, ok := ev.Object.(metav1.Object); ok && metaObj != nil {
		return metaObj.GetResourceVersion(), false
	}
	return "", false
}

func (s *Scraper) reconcileWorkflowDeletionsOnRelist(kind string, presentRawKeys map[string]bool, now time.Time) {
	var deletedEvents []WorkflowEvent
	s.mu.Lock()
	var keys []string
	for k, st := range s.workflowStates {
		if st.kind == kind && !presentRawKeys[k] {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		st := s.workflowStates[k]
		delete(s.workflowStates, k)
		wlState := st.workloadState
		if wlState == "" {
			wlState = WorkloadStateNotAdmitted
		}
		deletedEvents = append(deletedEvents, WorkflowEvent{
			Timestamp:     now,
			EventType:     EventDeleted,
			WorkloadState: wlState,
			Payload: WorkflowPayload{
				ObjectKind:     st.kind,
				Name:           st.name,
				Namespace:      st.namespace,
				CreationTime:   st.creationTime,
				LocalQueueName: st.localQueueName,
				Object:         st.lastPayload,
			},
		})
	}
	s.mu.Unlock()

	for _, ev := range deletedEvents {
		if s.workflowsLog != nil {
			if err := s.workflowsLog.Append(ev); err != nil {
				log.Printf("ERROR writing reconciled %s deletion event to %s: %v", kind, s.cfg.WorkflowsFile, err)
			}
		}
	}
}

func (s *Scraper) reconcileWorkloadDeletionsOnRelist(presentRawKeys map[string]bool, now time.Time) {
	var partialEvents []ExecutionTimeEvent
	s.mu.Lock()
	var missing []string
	for k := range s.workloadStates {
		if !presentRawKeys[k] {
			missing = append(missing, k)
		}
	}
	sort.Strings(missing)
	for _, k := range missing {
		if delEv := s.handleDeletedWorkloadKeyLocked(k, nil, now); delEv != nil {
			partialEvents = append(partialEvents, *delEv)
		}
	}
	s.mu.Unlock()

	for _, ev := range partialEvents {
		if s.execTimeLog != nil {
			if err := s.execTimeLog.Append(ev); err != nil {
				log.Printf("ERROR writing reconciled deleted workload PartialExecution event to %s: %v", s.cfg.ExecutionTimeFile, err)
			}
		}
	}
}

func (s *Scraper) watchNodesLoop(ctx context.Context, initialRV string) {
	if s.kubeClient == nil {
		return
	}
	rv := initialRV
	for {
		if ctx.Err() != nil {
			return
		}
		if rv == "" {
			rv = s.scrapeNodes(ctx, time.Now().UTC())
			if rv == "" {
				if ctx.Err() != nil {
					return
				}
				sleepContext(ctx, 2*time.Second)
				continue
			}
		}

		watchOpts := watchOptionsFromSentinel(rv)
		w, err := s.kubeClient.CoreV1().Nodes().Watch(ctx, watchOpts)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if isWatchExpiredError(err) {
				rv = ""
				continue
			}
			log.Printf("WARN failed to start Node watch: %v", err)
			sleepContext(ctx, 2*time.Second)
			continue
		}
		nextRV, expired := s.consumeNodeWatch(ctx, w, rv)
		if expired {
			rv = ""
		} else {
			rv = nextRV
		}
	}
}

func (s *Scraper) consumeNodeWatch(ctx context.Context, w watch.Interface, currentRV string) (string, bool) {
	defer w.Stop()
	rv := currentRV
	for {
		select {
		case <-ctx.Done():
			return rv, false
		case ev, ok := <-w.ResultChan():
			if !ok {
				return rv, false
			}
			evRV, expired := extractWatchRVAndExpired(ev)
			if expired {
				return "", true
			}
			if evRV != "" {
				rv = evRV
			}
			if ev.Type == watch.Bookmark || ev.Type == watch.Error {
				continue
			}
			if node, ok := ev.Object.(*corev1.Node); ok {
				s.handleNodeWatchEvent(ev.Type, node)
			}
		}
	}
}

func (s *Scraper) watchPodsLoop(ctx context.Context, initialRV string) {
	if s.kubeClient == nil {
		return
	}
	rv := initialRV
	for {
		if ctx.Err() != nil {
			return
		}
		if rv == "" {
			relistNow := time.Now().UTC()
			list, err := s.kubeClient.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("WARN failed to list Pods for watch initialization: %v", err)
				sleepContext(ctx, 2*time.Second)
				continue
			}
			sort.Slice(list.Items, func(i, j int) bool {
				return compareMetaOrder(&list.Items[i], &list.Items[j])
			})
			for i := range list.Items {
				s.handlePodWatchEventAt(watch.Added, &list.Items[i], relistNow, true)
			}
			s.scrapeScheduledAndRunningPodsWithList(ctx, relistNow, list.Items)
			rv = listRVOrZero(list.ResourceVersion)
		}

		watchOpts := watchOptionsFromSentinel(rv)
		w, err := s.kubeClient.CoreV1().Pods("").Watch(ctx, watchOpts)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if isWatchExpiredError(err) {
				rv = ""
				continue
			}
			log.Printf("WARN failed to start Pod watch: %v", err)
			sleepContext(ctx, 2*time.Second)
			continue
		}
		nextRV, expired := s.consumePodWatch(ctx, w, rv)
		if expired {
			rv = ""
		} else {
			rv = nextRV
		}
	}
}

func (s *Scraper) consumePodWatch(ctx context.Context, w watch.Interface, currentRV string) (string, bool) {
	defer w.Stop()
	rv := currentRV
	for {
		select {
		case <-ctx.Done():
			return rv, false
		case ev, ok := <-w.ResultChan():
			if !ok {
				return rv, false
			}
			evRV, expired := extractWatchRVAndExpired(ev)
			if expired {
				return "", true
			}
			if evRV != "" {
				rv = evRV
			}
			if ev.Type == watch.Bookmark || ev.Type == watch.Error {
				continue
			}
			if pod, ok := ev.Object.(*corev1.Pod); ok {
				s.handlePodWatchEvent(ev.Type, pod)
			}
		}
	}
}

func (s *Scraper) watchJobsLoop(ctx context.Context, initialRV string) {
	if s.kubeClient == nil {
		return
	}
	rv := initialRV
	for {
		if ctx.Err() != nil {
			return
		}
		if rv == "" {
			relistNow := time.Now().UTC()
			list, err := s.kubeClient.BatchV1().Jobs("").List(ctx, metav1.ListOptions{})
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("WARN failed to list Jobs: %v", err)
				sleepContext(ctx, 2*time.Second)
				continue
			}
			sort.Slice(list.Items, func(i, j int) bool {
				return compareMetaOrder(&list.Items[i], &list.Items[j])
			})
			present := make(map[string]bool, len(list.Items))
			for i := range list.Items {
				present["Job/"+list.Items[i].Namespace+"/"+list.Items[i].Name] = true
				s.handleJobWatchEventAt(watch.Added, &list.Items[i], relistNow, true)
			}
			s.reconcileWorkflowDeletionsOnRelist("Job", present, relistNow)
			rv = listRVOrZero(list.ResourceVersion)
		}

		watchOpts := watchOptionsFromSentinel(rv)
		w, err := s.kubeClient.BatchV1().Jobs("").Watch(ctx, watchOpts)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if isWatchExpiredError(err) {
				rv = ""
				continue
			}
			log.Printf("WARN failed to watch Jobs: %v", err)
			sleepContext(ctx, 2*time.Second)
			continue
		}
		expired := false
		func() {
			defer w.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case ev, ok := <-w.ResultChan():
					if !ok {
						return
					}
					evRV, isExp := extractWatchRVAndExpired(ev)
					if isExp {
						expired = true
						return
					}
					if evRV != "" {
						rv = evRV
					}
					if ev.Type == watch.Bookmark || ev.Type == watch.Error {
						continue
					}
					if job, ok := ev.Object.(*batchv1.Job); ok {
						s.handleJobWatchEvent(ev.Type, job)
					}
				}
			}
		}()
		if expired {
			rv = ""
		}
	}
}

func (s *Scraper) watchDeploymentsLoop(ctx context.Context, initialRV string) {
	if s.kubeClient == nil {
		return
	}
	rv := initialRV
	for {
		if ctx.Err() != nil {
			return
		}
		if rv == "" {
			relistNow := time.Now().UTC()
			list, err := s.kubeClient.AppsV1().Deployments("").List(ctx, metav1.ListOptions{})
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("WARN failed to list Deployments: %v", err)
				sleepContext(ctx, 2*time.Second)
				continue
			}
			sort.Slice(list.Items, func(i, j int) bool {
				return compareMetaOrder(&list.Items[i], &list.Items[j])
			})
			present := make(map[string]bool, len(list.Items))
			for i := range list.Items {
				present["Deployment/"+list.Items[i].Namespace+"/"+list.Items[i].Name] = true
				s.handleDeploymentWatchEventAt(watch.Added, &list.Items[i], relistNow)
			}
			s.reconcileWorkflowDeletionsOnRelist("Deployment", present, relistNow)
			rv = listRVOrZero(list.ResourceVersion)
		}

		watchOpts := watchOptionsFromSentinel(rv)
		w, err := s.kubeClient.AppsV1().Deployments("").Watch(ctx, watchOpts)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if isWatchExpiredError(err) {
				rv = ""
				continue
			}
			log.Printf("WARN failed to watch Deployments: %v", err)
			sleepContext(ctx, 2*time.Second)
			continue
		}
		expired := false
		func() {
			defer w.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case ev, ok := <-w.ResultChan():
					if !ok {
						return
					}
					evRV, isExp := extractWatchRVAndExpired(ev)
					if isExp {
						expired = true
						return
					}
					if evRV != "" {
						rv = evRV
					}
					if ev.Type == watch.Bookmark || ev.Type == watch.Error {
						continue
					}
					if deploy, ok := ev.Object.(*appsv1.Deployment); ok {
						s.handleDeploymentWatchEvent(ev.Type, deploy)
					}
				}
			}
		}()
		if expired {
			rv = ""
		}
	}
}

func (s *Scraper) watchReplicaSetsLoop(ctx context.Context, initialRV string) {
	if s.kubeClient == nil {
		return
	}
	rv := initialRV
	for {
		if ctx.Err() != nil {
			return
		}
		if rv == "" {
			list, err := s.kubeClient.AppsV1().ReplicaSets("").List(ctx, metav1.ListOptions{})
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("WARN failed to list ReplicaSets: %v", err)
				sleepContext(ctx, 2*time.Second)
				continue
			}
			for i := range list.Items {
				s.handleReplicaSetWatchEvent(watch.Added, &list.Items[i])
			}
			rv = listRVOrZero(list.ResourceVersion)
		}

		watchOpts := watchOptionsFromSentinel(rv)
		w, err := s.kubeClient.AppsV1().ReplicaSets("").Watch(ctx, watchOpts)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if isWatchExpiredError(err) {
				rv = ""
				continue
			}
			log.Printf("WARN failed to watch ReplicaSets: %v", err)
			sleepContext(ctx, 2*time.Second)
			continue
		}
		expired := false
		func() {
			defer w.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case ev, ok := <-w.ResultChan():
					if !ok {
						return
					}
					evRV, isExp := extractWatchRVAndExpired(ev)
					if isExp {
						expired = true
						return
					}
					if evRV != "" {
						rv = evRV
					}
					if ev.Type == watch.Bookmark || ev.Type == watch.Error {
						continue
					}
					if rs, ok := ev.Object.(*appsv1.ReplicaSet); ok {
						s.handleReplicaSetWatchEvent(ev.Type, rs)
					}
				}
			}
		}()
		if expired {
			rv = ""
		}
	}
}

func (s *Scraper) watchJobSetsLoop(ctx context.Context, initialRV string) {
	if s.jobsetClient == nil {
		return
	}
	rv := initialRV
	for {
		if ctx.Err() != nil {
			return
		}
		if rv == "" {
			relistNow := time.Now().UTC()
			list, err := s.jobsetClient.JobsetV1alpha2().JobSets("").List(ctx, metav1.ListOptions{})
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				if !apierrors.IsNotFound(err) {
					log.Printf("WARN failed to list JobSets: %v", err)
				}
				sleepContext(ctx, 5*time.Second)
				continue
			}
			sort.Slice(list.Items, func(i, j int) bool {
				return compareMetaOrder(&list.Items[i], &list.Items[j])
			})
			present := make(map[string]bool, len(list.Items))
			for i := range list.Items {
				present["JobSet/"+list.Items[i].Namespace+"/"+list.Items[i].Name] = true
				s.handleJobSetWatchEventAt(watch.Added, &list.Items[i], relistNow, true)
			}
			s.reconcileWorkflowDeletionsOnRelist("JobSet", present, relistNow)
			rv = listRVOrZero(list.ResourceVersion)
		}

		watchOpts := watchOptionsFromSentinel(rv)
		w, err := s.jobsetClient.JobsetV1alpha2().JobSets("").Watch(ctx, watchOpts)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if isWatchExpiredError(err) {
				rv = ""
				continue
			}
			log.Printf("WARN failed to watch JobSets: %v", err)
			sleepContext(ctx, 5*time.Second)
			continue
		}
		expired := false
		func() {
			defer w.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case ev, ok := <-w.ResultChan():
					if !ok {
						return
					}
					evRV, isExp := extractWatchRVAndExpired(ev)
					if isExp {
						expired = true
						return
					}
					if evRV != "" {
						rv = evRV
					}
					if ev.Type == watch.Bookmark || ev.Type == watch.Error {
						continue
					}
					if js, ok := ev.Object.(*jobsetv1alpha2.JobSet); ok {
						s.handleJobSetWatchEvent(ev.Type, js)
					}
				}
			}
		}()
		if expired {
			rv = ""
		}
	}
}

func (s *Scraper) watchWorkloadsLoop(ctx context.Context, initialRV string) {
	if s.kueueClient == nil {
		return
	}
	rv := initialRV
	for {
		if ctx.Err() != nil {
			return
		}
		if rv == "" {
			relistNow := time.Now().UTC()
			list, err := s.kueueClient.KueueV1beta2().Workloads("").List(ctx, metav1.ListOptions{})
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				if !apierrors.IsNotFound(err) {
					log.Printf("WARN failed to list Kueue Workloads: %v", err)
				}
				sleepContext(ctx, 5*time.Second)
				continue
			}
			sort.Slice(list.Items, func(i, j int) bool {
				return compareMetaOrder(&list.Items[i], &list.Items[j])
			})
			present := make(map[string]bool, len(list.Items))
			for i := range list.Items {
				present[list.Items[i].Namespace+"/"+list.Items[i].Name] = true
				s.handleWorkloadWatchEventAt(watch.Added, &list.Items[i], relistNow)
			}
			s.reconcileWorkloadDeletionsOnRelist(present, relistNow)
			rv = listRVOrZero(list.ResourceVersion)
		}

		watchOpts := watchOptionsFromSentinel(rv)
		w, err := s.kueueClient.KueueV1beta2().Workloads("").Watch(ctx, watchOpts)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if isWatchExpiredError(err) {
				rv = ""
				continue
			}
			log.Printf("WARN failed to watch Kueue Workloads: %v", err)
			sleepContext(ctx, 5*time.Second)
			continue
		}
		expired := false
		func() {
			defer w.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case ev, ok := <-w.ResultChan():
					if !ok {
						return
					}
					evRV, isExp := extractWatchRVAndExpired(ev)
					if isExp {
						expired = true
						return
					}
					if evRV != "" {
						rv = evRV
					}
					if ev.Type == watch.Bookmark || ev.Type == watch.Error {
						continue
					}
					if wl, ok := ev.Object.(*kueuev1beta2.Workload); ok {
						s.handleWorkloadWatchEvent(ev.Type, wl)
					}
				}
			}
		}()
		if expired {
			rv = ""
		}
	}
}

func (s *Scraper) watchClusterQueuesLoop(ctx context.Context, initialRV string) {
	if s.kueueClient == nil {
		return
	}
	rv := initialRV
	for {
		if ctx.Err() != nil {
			return
		}
		if rv == "" {
			rv = s.scrapeClusterQueues(ctx, time.Now().UTC())
			if rv == "" {
				if ctx.Err() != nil {
					return
				}
				sleepContext(ctx, 5*time.Second)
				continue
			}
		}
		w, err := s.kueueClient.KueueV1beta2().ClusterQueues().Watch(ctx, watchOptionsFromSentinel(rv))
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if isWatchExpiredError(err) {
				rv = ""
				continue
			}
			sleepContext(ctx, 5*time.Second)
			continue
		}
		expired := false
		func() {
			defer w.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case ev, ok := <-w.ResultChan():
					if !ok {
						return
					}
					evRV, isExp := extractWatchRVAndExpired(ev)
					if isExp {
						expired = true
						return
					}
					if evRV != "" {
						rv = evRV
					}
					if ev.Type == watch.Bookmark || ev.Type == watch.Error {
						continue
					}
					if cq, ok := ev.Object.(*kueuev1beta2.ClusterQueue); ok {
						s.handleInfraObjectWatchEventAt(ev.Type, "ClusterQueue", cq.Name, cq.Name, "", SanitizeClusterQueue(cq, s.hasher), time.Now().UTC())
					}
				}
			}
		}()
		if expired {
			rv = ""
		}
	}
}

func (s *Scraper) watchCohortsLoop(ctx context.Context, initialRV string) {
	if s.kueueClient == nil {
		return
	}
	rv := initialRV
	for {
		if ctx.Err() != nil {
			return
		}
		if rv == "" {
			rv = s.scrapeCohorts(ctx, time.Now().UTC())
			if rv == "" {
				if ctx.Err() != nil {
					return
				}
				sleepContext(ctx, 5*time.Second)
				continue
			}
		}
		w, err := s.kueueClient.KueueV1beta2().Cohorts().Watch(ctx, watchOptionsFromSentinel(rv))
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if isWatchExpiredError(err) {
				rv = ""
				continue
			}
			sleepContext(ctx, 5*time.Second)
			continue
		}
		expired := false
		func() {
			defer w.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case ev, ok := <-w.ResultChan():
					if !ok {
						return
					}
					evRV, isExp := extractWatchRVAndExpired(ev)
					if isExp {
						expired = true
						return
					}
					if evRV != "" {
						rv = evRV
					}
					if ev.Type == watch.Bookmark || ev.Type == watch.Error {
						continue
					}
					if cohort, ok := ev.Object.(*kueuev1beta2.Cohort); ok {
						s.handleInfraObjectWatchEventAt(ev.Type, "Cohort", cohort.Name, cohort.Name, "", SanitizeCohort(cohort, s.hasher), time.Now().UTC())
					}
				}
			}
		}()
		if expired {
			rv = ""
		}
	}
}

func (s *Scraper) watchLocalQueuesLoop(ctx context.Context, initialRV string) {
	if s.kueueClient == nil {
		return
	}
	rv := initialRV
	for {
		if ctx.Err() != nil {
			return
		}
		if rv == "" {
			rv = s.scrapeLocalQueues(ctx, time.Now().UTC())
			if rv == "" {
				if ctx.Err() != nil {
					return
				}
				sleepContext(ctx, 5*time.Second)
				continue
			}
		}
		w, err := s.kueueClient.KueueV1beta2().LocalQueues("").Watch(ctx, watchOptionsFromSentinel(rv))
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if isWatchExpiredError(err) {
				rv = ""
				continue
			}
			sleepContext(ctx, 5*time.Second)
			continue
		}
		expired := false
		func() {
			defer w.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case ev, ok := <-w.ResultChan():
					if !ok {
						return
					}
					evRV, isExp := extractWatchRVAndExpired(ev)
					if isExp {
						expired = true
						return
					}
					if evRV != "" {
						rv = evRV
					}
					if ev.Type == watch.Bookmark || ev.Type == watch.Error {
						continue
					}
					if lq, ok := ev.Object.(*kueuev1beta2.LocalQueue); ok {
						s.handleInfraObjectWatchEventAt(ev.Type, "LocalQueue", lq.Namespace+"/"+lq.Name, lq.Name, lq.Namespace, SanitizeLocalQueue(lq, s.hasher), time.Now().UTC())
					}
				}
			}
		}()
		if expired {
			rv = ""
		}
	}
}

func (s *Scraper) watchResourceFlavorsLoop(ctx context.Context, initialRV string) {
	if s.kueueClient == nil {
		return
	}
	rv := initialRV
	for {
		if ctx.Err() != nil {
			return
		}
		if rv == "" {
			rv = s.scrapeResourceFlavors(ctx, time.Now().UTC())
			if rv == "" {
				if ctx.Err() != nil {
					return
				}
				sleepContext(ctx, 5*time.Second)
				continue
			}
		}
		w, err := s.kueueClient.KueueV1beta2().ResourceFlavors().Watch(ctx, watchOptionsFromSentinel(rv))
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if isWatchExpiredError(err) {
				rv = ""
				continue
			}
			sleepContext(ctx, 5*time.Second)
			continue
		}
		expired := false
		func() {
			defer w.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case ev, ok := <-w.ResultChan():
					if !ok {
						return
					}
					evRV, isExp := extractWatchRVAndExpired(ev)
					if isExp {
						expired = true
						return
					}
					if evRV != "" {
						rv = evRV
					}
					if ev.Type == watch.Bookmark || ev.Type == watch.Error {
						continue
					}
					if rf, ok := ev.Object.(*kueuev1beta2.ResourceFlavor); ok {
						s.handleInfraObjectWatchEventAt(ev.Type, "ResourceFlavor", rf.Name, rf.Name, "", SanitizeResourceFlavor(rf, s.hasher), time.Now().UTC())
					}
				}
			}
		}()
		if expired {
			rv = ""
		}
	}
}

func (s *Scraper) watchTopologiesLoop(ctx context.Context, initialRV string) {
	if s.kueueClient == nil {
		return
	}
	rv := initialRV
	for {
		if ctx.Err() != nil {
			return
		}
		if rv == "" {
			rv = s.scrapeTopologies(ctx, time.Now().UTC())
			if rv == "" {
				if ctx.Err() != nil {
					return
				}
				sleepContext(ctx, 5*time.Second)
				continue
			}
		}
		w, err := s.kueueClient.KueueV1beta2().Topologies().Watch(ctx, watchOptionsFromSentinel(rv))
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if isWatchExpiredError(err) {
				rv = ""
				continue
			}
			sleepContext(ctx, 5*time.Second)
			continue
		}
		expired := false
		func() {
			defer w.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case ev, ok := <-w.ResultChan():
					if !ok {
						return
					}
					evRV, isExp := extractWatchRVAndExpired(ev)
					if isExp {
						expired = true
						return
					}
					if evRV != "" {
						rv = evRV
					}
					if ev.Type == watch.Bookmark || ev.Type == watch.Error {
						continue
					}
					if topo, ok := ev.Object.(*kueuev1beta2.Topology); ok {
						s.handleInfraObjectWatchEventAt(ev.Type, "Topology", topo.Name, topo.Name, "", SanitizeTopology(topo, s.hasher), time.Now().UTC())
					}
				}
			}
		}()
		if expired {
			rv = ""
		}
	}
}

func (s *Scraper) watchWorkloadPriorityClassesLoop(ctx context.Context, initialRV string) {
	if s.kueueClient == nil {
		return
	}
	rv := initialRV
	for {
		if ctx.Err() != nil {
			return
		}
		if rv == "" {
			rv = s.scrapeWorkloadPriorityClasses(ctx, time.Now().UTC())
			if rv == "" {
				if ctx.Err() != nil {
					return
				}
				sleepContext(ctx, 5*time.Second)
				continue
			}
		}
		w, err := s.kueueClient.KueueV1beta2().WorkloadPriorityClasses().Watch(ctx, watchOptionsFromSentinel(rv))
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if isWatchExpiredError(err) {
				rv = ""
				continue
			}
			sleepContext(ctx, 5*time.Second)
			continue
		}
		expired := false
		func() {
			defer w.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case ev, ok := <-w.ResultChan():
					if !ok {
						return
					}
					evRV, isExp := extractWatchRVAndExpired(ev)
					if isExp {
						expired = true
						return
					}
					if evRV != "" {
						rv = evRV
					}
					if ev.Type == watch.Bookmark || ev.Type == watch.Error {
						continue
					}
					if wpc, ok := ev.Object.(*kueuev1beta2.WorkloadPriorityClass); ok {
						s.handleInfraObjectWatchEventAt(ev.Type, "WorkloadPriorityClass", wpc.Name, wpc.Name, "", SanitizeWorkloadPriorityClass(wpc, s.hasher), time.Now().UTC())
					}
				}
			}
		}()
		if expired {
			rv = ""
		}
	}
}

func (s *Scraper) watchDynamicResourceLoop(
	ctx context.Context,
	gvr schema.GroupVersionResource,
	kindName string,
	initialRV string,
	handler func(watch.EventType, *unstructured.Unstructured, bool),
) {
	if s.dynamicClient == nil {
		return
	}
	loggedMissingCRD := false
	rv := initialRV
	for {
		if ctx.Err() != nil {
			return
		}
		if rv == "" {
			relistNow := time.Now().UTC()
			list, err := s.dynamicClient.Resource(gvr).Namespace(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				if apierrors.IsNotFound(err) {
					if !loggedMissingCRD {
						log.Printf("INFO CRD for %s (%s) is not installed on cluster; will retry periodically", kindName, gvr.String())
						loggedMissingCRD = true
					}
					sleepContext(ctx, 30*time.Second)
					continue
				}
				log.Printf("WARN failed to list %s: %v", kindName, err)
				sleepContext(ctx, 5*time.Second)
				continue
			}
			loggedMissingCRD = false
			sort.Slice(list.Items, func(i, j int) bool {
				return compareMetaOrder(&list.Items[i], &list.Items[j])
			})
			present := make(map[string]bool, len(list.Items))
			for i := range list.Items {
				present[kindName+"/"+list.Items[i].GetNamespace()+"/"+list.Items[i].GetName()] = true
				handler(watch.Added, &list.Items[i], true)
			}
			s.reconcileWorkflowDeletionsOnRelist(kindName, present, relistNow)
			rv = listRVOrZero(list.GetResourceVersion())
		}

		watchOpts := watchOptionsFromSentinel(rv)
		w, err := s.dynamicClient.Resource(gvr).Namespace(metav1.NamespaceAll).Watch(ctx, watchOpts)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if isWatchExpiredError(err) {
				rv = ""
				continue
			}
			log.Printf("WARN failed to watch %s: %v", kindName, err)
			sleepContext(ctx, 5*time.Second)
			continue
		}
		expired := false
		func() {
			defer w.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case ev, ok := <-w.ResultChan():
					if !ok {
						return
					}
					evRV, isExp := extractWatchRVAndExpired(ev)
					if isExp {
						expired = true
						return
					}
					if evRV != "" {
						rv = evRV
					}
					if ev.Type == watch.Bookmark || ev.Type == watch.Error {
						continue
					}
					if u, ok := ev.Object.(*unstructured.Unstructured); ok {
						handler(ev.Type, u, false)
					}
				}
			}
		}()
		if expired {
			rv = ""
		}
	}
}

func sleepContext(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
