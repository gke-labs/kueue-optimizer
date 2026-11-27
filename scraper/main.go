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
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	jobsetclientset "sigs.k8s.io/jobset/client-go/clientset/versioned"
	kueueclientset "sigs.k8s.io/kueue/client-go/clientset/versioned"
)

func parseIntervalArg(raw string) (time.Duration, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 60 * time.Second, nil
	}
	if secs, err := strconv.ParseFloat(raw, 64); err == nil {
		if secs <= 0 {
			return 0, fmt.Errorf("interval must be > 0, got %s", raw)
		}
		return time.Duration(secs * float64(time.Second)), nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid interval %q: %w", raw, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("interval must be > 0, got %s", d)
	}
	return d, nil
}

func buildRestConfig(kubeconfig string, qps float32, burst int) (*rest.Config, error) {
	if kubeconfig == "" {
		kubeconfig = os.Getenv("KUBECONFIG")
	}
	var (
		cfg *rest.Config
		err error
	)
	if kubeconfig != "" {
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
		cfg, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
			loadingRules,
			&clientcmd.ConfigOverrides{},
		).ClientConfig()
		if err != nil {
			cfg, err = rest.InClusterConfig()
		}
	}
	if err != nil {
		return nil, err
	}
	cfg.QPS = qps
	cfg.Burst = burst
	return cfg, nil
}

type csvLabelsFlag []string

func (f *csvLabelsFlag) String() string {
	return strings.Join(*f, ",")
}

func (f *csvLabelsFlag) Set(raw string) error {
	for part := range strings.SplitSeq(raw, ",") {
		k := strings.ToLower(strings.TrimSpace(part))
		if k != "" && !slices.Contains(*f, k) {
			*f = append(*f, k)
		}
	}
	return nil
}

func parseFlags(progName string, args []string, errOut io.Writer) (Config, float64, int, error) {
	var (
		kubeconfig        string
		intervalStr       string
		duration          time.Duration
		hashNames         bool
		sha256Names       bool
		pauseImage        string
		extraHashLabels   csvLabelsFlag
		outputDir         string
		infraFile         string
		workflowsFile     string
		executionTimeFile string
		kueueNamespace    string
		kueueConfigMap    string
		qps               float64
		burst             int
	)

	// Use a dedicated FlagSet instead of flag.CommandLine because transitive Kubernetes
	// dependencies (sigs.k8s.io/controller-runtime/pkg/client/config) register -kubeconfig
	// on flag.CommandLine in init().
	fs := flag.NewFlagSet(progName, flag.ContinueOnError)
	if errOut != nil {
		fs.SetOutput(errOut)
	}
	fs.StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig file (defaults to $KUBECONFIG, ~/.kube/config, or in-cluster config)")
	fs.StringVar(&intervalStr, "interval", "60s", "Periodic scrape interval (e.g. 60s or 60; default 60s)")
	fs.StringVar(&intervalStr, "n", "60s", "Shorthand for --interval (periodic scrape interval, default 60s)")
	fs.DurationVar(&duration, "duration", 0, "Optional total run duration (e.g. 5m; 0 runs until interrupted)")
	fs.BoolVar(&hashNames, "hash-names", false, "Replace names of workloads, pods, jobs, jobsets, etc. and sensitive/workload label values with per-run HMAC-SHA256 hashes")
	fs.BoolVar(&sha256Names, "sha256-names", false, "Alias for --hash-names")
	fs.StringVar(&pauseImage, "pause-image", DefaultPauseImage, "Pause container image used to replace container command lines")
	fs.Var(&extraHashLabels, "extra-hash-labels", "Comma-separated additional list of label keys or <domain>/* patterns to hash when --hash-names is enabled (may be repeated)")
	fs.StringVar(&outputDir, "output-dir", ".", "Directory where infra.json, workflows.json, and execution-time.json are created")
	fs.StringVar(&infraFile, "infra-file", "infra.json", "Path (or filename in --output-dir) for infrastructure & pod scrape events")
	fs.StringVar(&workflowsFile, "workflows-file", "workflows.json", "Path (or filename in --output-dir) for workflow creation/update events")
	fs.StringVar(&executionTimeFile, "execution-time-file", "execution-time.json", "Path (or filename in --output-dir) for workload execution time events")
	fs.StringVar(&kueueNamespace, "kueue-namespace", DefaultKueueNamespace, "Namespace containing Kueue's controller ConfigMap (empty string disables ConfigMap scraping)")
	fs.StringVar(&kueueConfigMap, "kueue-configmap", DefaultKueueConfigMapName, "ConfigMap name holding Kueue's controller_manager_config.yaml (empty string disables ConfigMap scraping)")
	fs.Float64Var(&qps, "qps", 200.0, "Kubernetes API client QPS limit")
	fs.IntVar(&burst, "burst", 400, "Kubernetes API client burst limit")

	if err := fs.Parse(args); err != nil {
		return Config{}, 0, 0, err
	}

	interval, err := parseIntervalArg(intervalStr)
	if err != nil {
		if errOut != nil {
			_, _ = fmt.Fprintf(errOut, "Invalid scrape interval: %v\n", err)
		}
		return Config{}, 0, 0, fmt.Errorf("invalid scrape interval: %w", err)
	}

	hashEnabled := hashNames || sha256Names
	if len(extraHashLabels) > 0 && !hashEnabled {
		if errOut != nil {
			_, _ = fmt.Fprintln(errOut, "--extra-hash-labels requires --hash-names (or --sha256-names) to be enabled")
		}
		return Config{}, 0, 0, fmt.Errorf("--extra-hash-labels requires --hash-names to be enabled")
	}

	resolveOutputPath := func(dir, file string) string {
		if filepath.IsAbs(file) || dir == "" || dir == "." {
			return file
		}
		return filepath.Join(dir, file)
	}

	trimmedKueueNS := strings.TrimSpace(kueueNamespace)
	trimmedKueueCM := strings.TrimSpace(kueueConfigMap)
	cfg := Config{
		Kubeconfig:               kubeconfig,
		Interval:                 interval,
		Duration:                 duration,
		HashNames:                hashEnabled,
		PauseImage:               pauseImage,
		ExtraHashLabels:          []string(extraHashLabels),
		InfraFile:                resolveOutputPath(outputDir, infraFile),
		WorkflowsFile:            resolveOutputPath(outputDir, workflowsFile),
		ExecutionTimeFile:        resolveOutputPath(outputDir, executionTimeFile),
		KueueNamespace:           trimmedKueueNS,
		KueueConfigMapName:       trimmedKueueCM,
		DisableKueueConfigScrape: trimmedKueueNS == "" || trimmedKueueCM == "",
	}
	return cfg, qps, burst, nil
}

func main() {
	cfg, qps, burst, err := parseFlags(os.Args[0], os.Args[1:], os.Stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		os.Exit(2)
	}

	// Print startup configuration, scraped objects, omitted fields, and SHA-256 hashing status.
	PrintStartupBanner(os.Stdout, cfg)

	restCfg, err := buildRestConfig(cfg.Kubeconfig, float32(qps), burst)
	if err != nil {
		log.Fatalf("Failed to load Kubernetes client configuration: %v", err)
	}

	kubeClient, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		log.Fatalf("Failed to create Kubernetes client: %v", err)
	}
	kueueClient, err := kueueclientset.NewForConfig(restCfg)
	if err != nil {
		log.Fatalf("Failed to create Kueue client: %v", err)
	}
	jobsetClient, err := jobsetclientset.NewForConfig(restCfg)
	if err != nil {
		log.Fatalf("Failed to create JobSet client: %v", err)
	}
	dynamicClient, err := dynamic.NewForConfig(restCfg)
	if err != nil {
		log.Fatalf("Failed to create dynamic Kubernetes client: %v", err)
	}

	infraLog, err := NewJSONEventLog(cfg.InfraFile)
	if err != nil {
		log.Fatalf("Failed to initialize %s: %v", cfg.InfraFile, err)
	}
	defer func() { _ = infraLog.Close() }()

	workflowsLog, err := NewJSONEventLog(cfg.WorkflowsFile)
	if err != nil {
		log.Fatalf("Failed to initialize %s: %v", cfg.WorkflowsFile, err)
	}
	defer func() { _ = workflowsLog.Close() }()

	execTimeLog, err := NewJSONEventLog(cfg.ExecutionTimeFile)
	if err != nil {
		log.Fatalf("Failed to initialize %s: %v", cfg.ExecutionTimeFile, err)
	}
	defer func() { _ = execTimeLog.Close() }()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if cfg.Duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.Duration)
		defer cancel()
	}

	scraper := NewScraper(
		cfg,
		kubeClient,
		kueueClient,
		jobsetClient,
		dynamicClient,
		infraLog,
		workflowsLog,
		execTimeLog,
	)

	if err := scraper.Run(ctx); err != nil {
		log.Fatalf("Scraper terminated with error: %v", err)
	}

	if err := errors.Join(infraLog.Close(), workflowsLog.Close(), execTimeLog.Close()); err != nil {
		log.Fatalf("Failed to flush and close output event logs: %v", err)
	}

	fmt.Printf(
		"\nScraper finished cleanly. Events recorded: %s=%d, %s=%d, %s=%d\n",
		cfg.InfraFile, infraLog.Count(),
		cfg.WorkflowsFile, workflowsLog.Count(),
		cfg.ExecutionTimeFile, execTimeLog.Count(),
	)
}
