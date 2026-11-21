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

"""Modular scenario testrunner package for kueue-optimizer."""

from __future__ import annotations

from runner.loader import (
    DEFAULT_INFINITE_POD_DURATION_SEC,
    DEFAULT_SCENARIO_DIR,
    InitialPodSpec,
    InitialWorkloadPodGroup,
    ScenarioBundle,
    ScheduledWorkflowItem,
    load_scenario_bundle,
)
from runner.outline import print_scenario_outline
from runner.replay import (
    apply_scenario_queues_and_wait,
    cleanup_scenario_workloads,
    run_scenario_replay,
)
from runner.validation import (
    ALLOWED_INFRA_GVKS,
    ALLOWED_WORKFLOW_GVKS,
    ALLOWED_WORKFLOW_KINDS,
    MAX_K8S_NAME_LEN,
    SERVER_METADATA_FIELDS,
    SYSTEM_NAMESPACES,
    validate_workflow_events,
)

__all__ = [
    "ALLOWED_INFRA_GVKS",
    "ALLOWED_WORKFLOW_GVKS",
    "ALLOWED_WORKFLOW_KINDS",
    "DEFAULT_INFINITE_POD_DURATION_SEC",
    "DEFAULT_SCENARIO_DIR",
    "InitialPodSpec",
    "InitialWorkloadPodGroup",
    "MAX_K8S_NAME_LEN",
    "SERVER_METADATA_FIELDS",
    "SYSTEM_NAMESPACES",
    "ScenarioBundle",
    "ScheduledWorkflowItem",
    "apply_scenario_queues_and_wait",
    "cleanup_scenario_workloads",
    "load_scenario_bundle",
    "print_scenario_outline",
    "run_scenario_replay",
    "validate_workflow_events",
]
