#!/usr/bin/env python3
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

"""Generate comprehensive README.md summaries for kueue-optimizer test scenario directories.

Usage:
  # Single scenario directory:
  python3 deploy/scripts/generate_scenario_readme.py testscenarios/demo

  # All scenario directories containing infra.json under testscenarios/:
  python3 deploy/scripts/generate_scenario_readme.py --all

Core implementation lives in the `scenario_readme` package (`deploy/scripts/scenario_readme/`).
"""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

from chamber.constants import ROOT_DIR as REPO_ROOT, SCRIPT_DIR
from chamber.parsers import (  # noqa: F401
    format_duration_human as fmt_duration_human,
    format_duration_short as fmt_duration_short,
    format_epoch_iso as fmt_epoch_iso,
)
from scenario_readme import (  # noqa: F401
    COMMON_TOPOLOGY_LABEL_CANDIDATES,
    DURATION_BINS,
    UNIQUE_NODE_LABEL_KEYS,
    UNIQUE_NODE_LABEL_SUBSTRINGS,
    ParsedWorkload,
    ScenarioAnalysis,
    analyze_scenario,
    describe_node_group,
    fmt_quantity,
    generate_readme_for_scenario,
    node_group_fingerprint,
    render_duration_histogram,
    render_scenario_readme,
    render_timeline_arrival_histogram,
)

SCRIPT_PATH = SCRIPT_DIR / "generate_scenario_readme.py"


def discover_scenario_dirs(root: Path) -> list[Path]:
    """Find all directories under `root` containing `infra.json`."""
    dirs = [p.parent for p in root.rglob("infra.json") if p.is_file()]
    return sorted(set(dirs))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate comprehensive README.md summaries for kueue-optimizer test scenario directories."
    )
    parser.add_argument(
        "scenarios",
        nargs="*",
        type=Path,
        help="One or more scenario directories containing infra.json, workflows.json, and execution-time.json.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        default=False,
        help="Discover and generate README.md for all directories under --root containing infra.json.",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=REPO_ROOT / "testscenarios",
        help="Root directory searched when --all is specified (default: testscenarios/).",
    )
    parser.add_argument(
        "--filename",
        default="README.md",
        help="Output markdown filename written inside each scenario directory (default: README.md).",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=None,
        help="Explicit output path (only valid when processing a single scenario directory).",
    )
    args = parser.parse_args(argv)

    targets: list[Path] = []
    if args.all or not args.scenarios:
        targets.extend(discover_scenario_dirs(args.root.resolve()))
    for s in args.scenarios:
        targets.append(s.resolve())

    seen: set[Path] = set()
    unique_targets: list[Path] = []
    for t in targets:
        if t not in seen:
            seen.add(t)
            unique_targets.append(t)

    if not unique_targets:
        print("ERROR: No scenario directories found.", file=sys.stderr)
        return 1

    if args.output and len(unique_targets) > 1:
        print("ERROR: --output can only be specified when generating for a single scenario directory.", file=sys.stderr)
        return 1

    for idx, sdir in enumerate(unique_targets, start=1):
        out_path = args.output.resolve() if args.output else (sdir / args.filename)
        content = generate_readme_for_scenario(sdir)
        out_path.write_text(content, encoding="utf-8")
        try:
            rel_out = out_path.relative_to(REPO_ROOT)
        except ValueError:
            rel_out = out_path
        print(f"[{idx:>2}/{len(unique_targets):>2}] Wrote {rel_out} ({len(content):,} bytes)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
