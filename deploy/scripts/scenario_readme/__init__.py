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

"""Modular scenario analysis and README.md generation package."""

from scenario_readme.analysis import (
    COMMON_TOPOLOGY_LABEL_CANDIDATES,
    DEFAULT_SIX_HOUR_SEC,
    UNIQUE_NODE_LABEL_KEYS,
    UNIQUE_NODE_LABEL_SUBSTRINGS,
    ParsedWorkload,
    ScenarioAnalysis,
    analyze_scenario,
    describe_node_group,
    extract_rg_totals,
    format_speedup_val,
    node_group_fingerprint,
)
from scenario_readme.histograms import (
    DURATION_BINS,
    render_duration_histogram,
    render_timeline_arrival_histogram,
)
from scenario_readme.render import (
    fmt_quantity,
    generate_readme_for_scenario,
    render_scenario_readme,
)

__all__ = [
    "COMMON_TOPOLOGY_LABEL_CANDIDATES",
    "DEFAULT_SIX_HOUR_SEC",
    "DURATION_BINS",
    "ParsedWorkload",
    "ScenarioAnalysis",
    "UNIQUE_NODE_LABEL_KEYS",
    "UNIQUE_NODE_LABEL_SUBSTRINGS",
    "analyze_scenario",
    "describe_node_group",
    "extract_rg_totals",
    "fmt_quantity",
    "format_speedup_val",
    "generate_readme_for_scenario",
    "node_group_fingerprint",
    "render_duration_histogram",
    "render_scenario_readme",
    "render_timeline_arrival_histogram",
]
