# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Makefile for deploying and evaluating isolated kueue-optimizer chambers inside a host Kubernetes cluster

NAMESPACE ?= kueue-optimizer-chamber-01
LOCAL_APISERVER_PORT ?= 16443
LOCAL_PROMETHEUS_PORT ?= 9090
PROMETHEUS_SCRAPE_INTERVAL ?= 10s

.PHONY: help
help: ## Show available targets
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m [NAMESPACE=$(NAMESPACE)]\n\nTargets:\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-24s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

.PHONY: set-namespace
set-namespace: ## Update the target namespace and Prometheus scrape interval in Kustomize manifests
	@sed -i "s/^  name: .*/  name: $(NAMESPACE)/" ./deploy/base/namespace.yaml
	@sed -i "s/^namespace: .*/namespace: $(NAMESPACE)/" \
		./deploy/base/kustomization.yaml \
		./deploy/overlays/with-sample-queues/kustomization.yaml \
		./deploy/kustomization.yaml
	@sed -i -E "s/^([[:space:]]*(scrape|evaluation)_interval:).*/\1 $(PROMETHEUS_SCRAPE_INTERVAL)/" ./deploy/base/config/prometheus.yaml

.PHONY: generate
generate: ## Regenerate manifests and configs from ./kwok, ./kueue, and ./jobset for NAMESPACE (PKI is generated ad-hoc at deploy time)
	NAMESPACE=$(NAMESPACE) PROMETHEUS_SCRAPE_INTERVAL=$(PROMETHEUS_SCRAPE_INTERVAL) ./deploy/scripts/generate.sh

.PHONY: build
build: ## Render the full Kustomize manifest to stdout (picks PKI from cluster or generates ad-hoc)
	./deploy/scripts/chamber_common.py render --namespace $(NAMESPACE) --scrape-interval $(PROMETHEUS_SCRAPE_INTERVAL) --with-sample-queues

.PHONY: build-base
build-base: ## Render the base Kustomize manifest (without sample queues) to stdout (picks PKI from cluster or generates ad-hoc)
	./deploy/scripts/chamber_common.py render --namespace $(NAMESPACE) --scrape-interval $(PROMETHEUS_SCRAPE_INTERVAL)

.PHONY: deploy
deploy: ## Deploy the kueue-optimizer chamber (with sample queues) into NAMESPACE
	./deploy/scripts/chamber_common.py apply --namespace $(NAMESPACE) --scrape-interval $(PROMETHEUS_SCRAPE_INTERVAL) --with-sample-queues

.PHONY: deploy-base
deploy-base: ## Deploy only the base kueue-optimizer chamber into NAMESPACE
	./deploy/scripts/chamber_common.py apply --namespace $(NAMESPACE) --scrape-interval $(PROMETHEUS_SCRAPE_INTERVAL)

.PHONY: wait
wait: ## Wait for all chamber control-plane components to become Available in NAMESPACE
	kubectl -n $(NAMESPACE) wait --for=condition=Available --timeout=300s \
		deployment/kwok-registry \
		deployment/kwok-etcd \
		deployment/kwok-kube-apiserver \
		deployment/kwok-kube-controller-manager \
		deployment/kwok-kube-scheduler \
		deployment/kwok-controller \
		deployment/jobset-controller-manager \
		deployment/kueue-controller-manager \
		deployment/prometheus

.PHONY: undeploy
undeploy: ## Remove the kueue-optimizer chamber from NAMESPACE (verifies chamber ownership label)
	./deploy/scripts/chamber_common.py delete --namespace $(NAMESPACE)

.PHONY: local-kubeconfig
local-kubeconfig: ## Generate ./kwok-$(NAMESPACE).kubeconfig from cluster Secret/kwok-pki (or ad-hoc) for use with `make port-forward`
	./deploy/scripts/chamber_common.py local-kubeconfig --namespace $(NAMESPACE) --port $(LOCAL_APISERVER_PORT)

.PHONY: port-forward
port-forward: local-kubeconfig ## Port-forward the chamber kwok-kube-apiserver in NAMESPACE to localhost:$(LOCAL_APISERVER_PORT)
	kubectl -n $(NAMESPACE) port-forward svc/kwok-kube-apiserver $(LOCAL_APISERVER_PORT):6443

.PHONY: port-forward-prometheus
port-forward-prometheus: ## Port-forward the chamber Prometheus HTTP query interface to localhost:$(LOCAL_PROMETHEUS_PORT)
	kubectl -n $(NAMESPACE) port-forward svc/prometheus $(LOCAL_PROMETHEUS_PORT):9090

.PHONY: demo
demo: ## Run the synthetic demo (deploy chamber, 10x 4CPU/16Gi nodes, StrictFIFO ClusterQueue, and 20 JobSets)
	NAMESPACE=$(NAMESPACE) LOCAL_APISERVER_PORT=$(LOCAL_APISERVER_PORT) PROMETHEUS_SCRAPE_INTERVAL=$(PROMETHEUS_SCRAPE_INTERVAL) ./deploy/scripts/demo.py $(DEMO_ARGS)

.PHONY: demo-clean
demo-clean: ## Tear down the chamber deployed by the demo in NAMESPACE
	NAMESPACE=$(NAMESPACE) LOCAL_APISERVER_PORT=$(LOCAL_APISERVER_PORT) ./deploy/scripts/demo.py --cleanup $(DEMO_ARGS)

.PHONY: testrunner
testrunner: ## Replay a scraped test scenario (default: testscenarios/demo) in NAMESPACE per SCRAPER-SCHEMA.md
	./deploy/scripts/testrunner.py --namespace $(NAMESPACE) --port $(LOCAL_APISERVER_PORT) --scrape-interval $(PROMETHEUS_SCRAPE_INTERVAL) $(TESTRUNNER_ARGS)

.PHONY: testrunner-clean
testrunner-clean: ## Tear down the chamber deployed by testrunner in NAMESPACE
	./deploy/scripts/testrunner.py --cleanup --namespace $(NAMESPACE) --port $(LOCAL_APISERVER_PORT) $(TESTRUNNER_ARGS)

.PHONY: scraper-build
scraper-build: ## Build the Go Kubernetes & Kueue cluster scraper binary (./scraper/cluster-scraper)
	cd ./scraper && go build -o cluster-scraper .

.PHONY: scraper-test
scraper-test: ## Run unit tests for the Go Kubernetes & Kueue cluster scraper
	cd ./scraper && go test -v ./...

.PHONY: self-tests
self-tests: ## Deterministically generate all 95 self-test scenario directories (st01-st95 JSONs, READMEs, and AGENTS.md)
	python3 ./testscenarios/self-tests/generate_self_test_suite.py
	python3 ./deploy/scripts/generate_scenario_readme.py --all --root ./testscenarios/self-tests

.PHONY: self-tests-check
self-tests-check: ## Verify generated self-test scenarios and top-level docs are 100% in sync with zero drift
	python3 ./testscenarios/self-tests/generate_self_test_suite.py --check

.PHONY: python-test
python-test: ## Run Python unit tests
	python3 -m unittest discover -s deploy/scripts -p "test_*.py" -v

.PHONY: test
test: python-test scraper-test ## Run all Python and Go unit tests
