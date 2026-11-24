# Repository Agent Instructions

## Keep Test Scenario `README.md` Summaries in Sync

Every test scenario directory under `testscenarios/` (any directory containing `infra.json`, `workflows.json`, and/or `execution-time.json`) has an auto-generated `README.md` summary produced by `deploy/scripts/generate_scenario_readme.py`.

**Mandatory rule:** You MUST regenerate the scenario `README.md` file(s) whenever ANY of the following occurs:

1. **Direct test scenario edits:** You create, edit, or regenerate `infra.json`, `workflows.json`, or `execution-time.json` in any scenario directory.
2. **Generator script runs/changes:** You modify or run any script that generates or mutates scenario JSON files (for example `deploy/scripts/demo.py` or a scenario's `generate_scenario.py`).
3. **Summary script changes:** You modify `deploy/scripts/generate_scenario_readme.py` or its implementation package `deploy/scripts/scenario_readme/` (in this case, always re-run with `--all` so all scenario `README.md` files stay consistent).
4. **New scenarios or drift detected:** You add a new scenario directory, notice a missing `README.md`, or have any reason to suspect a scenario's `README.md` is out of sync with its JSON inputs.

### How to Regenerate

- **Single scenario directory** (when only one scenario's JSONs changed):
  ```bash
  python3 deploy/scripts/generate_scenario_readme.py testscenarios/<path/to/scenario>
  ```
- **All scenario directories across the repository** (when multiple scenarios, `deploy/scripts/generate_scenario_readme.py`, or `deploy/scripts/scenario_readme/` changed):
  ```bash
  python3 deploy/scripts/generate_scenario_readme.py --all
  ```

Always include the updated `README.md` file(s) alongside your scenario or script changes (and in the same commit when committing). Never hand-edit the auto-generated sections of a scenario `README.md` without updating the generator (`deploy/scripts/generate_scenario_readme.py` and `deploy/scripts/scenario_readme/`) and re-running it.

---

## Keep the 90-Scenario Self-Test Suite (`testscenarios/self-tests/`) Synchronized & Cross-Examined

All 90 scenarios in `testscenarios/self-tests/` (`st01`–`st90`, covering baseline calibration, queueing & TAS, intra-CQ preemption, Cohort borrowing/reclaim, Fair Sharing, resource/admission edge cases, `t0` initial-workload bootstrap/preemption/Fair Sharing/Cohort reclaims, `>100`-node `>1,000`-workload scale + live `cluster-scraper` validation, mid-scenario node failures & TAS placement hot-swap, `waitForPodsReady` `--speedup`/`--duration` scaling, interrupted-workload `PartialExecution` multiplier/override replay, declarative Kueue controller `Configuration` statements covering `FairSharing` and `AdmissionFairSharing`, and `Deployment` scraping and replaying across scale-up, scale-down, rolling upgrades, pauses, `t0` readiness status, and deletion/re-creation) are deterministically generated via `make self-tests` from `testscenarios/self-tests/generate_self_test_suite.py` (with `st46`–`st60` in `testscenarios/self-tests/scale_scenarios.py`, `st61`–`st75` in `testscenarios/self-tests/failure_preemption_scenarios.py`, `st76`–`st80` in `testscenarios/self-tests/kueue_config_scenarios.py`, and `st81`–`st90` in `testscenarios/self-tests/deployment_scenarios.py`).

1. **Always pair and cross-examine runs in separate isolated chambers**: Follow `testscenarios/self-tests/AGENTS.md` and each scenario's local `AGENTS.md` — execute each test side-by-side with its designated paired counterpart(s) in **separate isolated `kueue-optimizer` chambers** (`--namespace kueue-optimizer-chamber-stXX --port 164XX` per scenario, up to 7 parallel chambers; never reuse a single chamber across scenarios with `--skip-chamber-deploy`) and cross-check the expected directional and numerical shifts across Sections `5.1`–`5.4`.
2. **Never allow drift**: Whenever `generate_self_test_suite.py`, any scenario definition, or any pairing rule is modified (or suspected of drift), run the full synchronization and check pipeline:
   ```bash
   make self-tests
   make self-tests-check
   ```

---

## Use "`kueue-optimizer` Chambers" (Never "KWOK Chambers") & Enforce Per-Run Isolation

**Mandatory rule:** Across all documentation, reports, commit messages, code comments, and agent responses, always refer to the isolated evaluation environments as **`kueue-optimizer` chambers** (or **`kueue-optimizer` evaluation chambers**, e.g. `--namespace kueue-optimizer-chamber-<id>`) — **never** refer to them as `"KWOK chambers"` or `"KWOK + Kueue + JobSet chambers"`.

- **Whole environment:** Always call the isolated evaluation namespace/environment a **`kueue-optimizer` chamber** (`kueue-optimizer-chamber-<id>`).
- **Individual components only:** Use `KWOK` strictly when referring to specific KWOK components or CRDs inside a `kueue-optimizer` chamber — such as `kwok-kube-apiserver`, `kwok-etcd`, `kwok-controller`, `kwok-kube-scheduler`, `kwok-kube-controller-manager`, `kwok-registry`, KWOK `Stage` rules, or simulated KWOK `Node` objects.
- **Strict per-run isolation:** Always execute scenarios and A/B arms in **separate isolated chambers** (`--namespace kueue-optimizer-chamber-<id> --port 164XX` per run; never reuse a single chamber across runs with `--skip-chamber-deploy` so Prometheus counters, histogram buckets, and TSDB series from one run never contaminate another run).

---

## Store All Ad-Hoc Kueue Images & Custom Builds in the Per-Chamber In-Memory Registry

Every isolated evaluation chamber (`--namespace kueue-optimizer-chamber-<id>`) runs its own dedicated, ephemeral OCI Distribution v2 registry (`deployment/kwok-registry`, `Service/kwok-registry` on `NodePort 30500 + (port - 16443)`, backed strictly by kernel `tmpfs` RAM at `/var/lib/registry` as defined in `deploy/base/registry.yaml`).

**Mandatory rule:** Whenever you build, test, compare, or deploy any ad-hoc, locally modified, experimental, or custom-version Kueue controller image (or any other ad-hoc chamber container image), you MUST store and serve it through the target chamber's in-memory registry via `deploy/scripts/testrunner.py` or `deploy/scripts/chamber_registry.py`:

1. **Always push ad-hoc Kueue builds/tags into the target chamber's `kwok-registry`:**
   - **Run a local Docker image in an isolated chamber** (automatically stages `kwok-registry`, pushes `localhost:<registry-port>/kueue:<tag>`, pins `@sha256:<digest>`, and rolls out `deployment/kueue-controller-manager`):
     ```bash
     python3 deploy/scripts/testrunner.py testscenarios/<path/to/scenario> \
       --namespace kueue-optimizer-chamber-exp1 --port 16451 \
       --kueue-image <local-kueue-image>:<tag>
     ```
   - **Build Kueue directly from a local source checkout and push into the chamber registry**:
     ```bash
     python3 deploy/scripts/testrunner.py testscenarios/<path/to/scenario> \
       --namespace kueue-optimizer-chamber-exp2 --port 16452 \
       --kueue-build-dir ./kueue
     ```
   - **Push, build, hot-swap, inspect, or reclaim RAM in a live chamber** via `deploy/scripts/chamber_registry.py`:
     ```bash
     python3 deploy/scripts/chamber_registry.py push <local-image> --namespace kueue-optimizer-chamber-exp1 --port 16451 --tag adhoc --set-kueue
     python3 deploy/scripts/chamber_registry.py build-kueue ./kueue --namespace kueue-optimizer-chamber-exp1 --port 16451 --tag adhoc --set-kueue
     python3 deploy/scripts/chamber_registry.py status --namespace kueue-optimizer-chamber-exp1 --port 16451
     python3 deploy/scripts/chamber_registry.py delete kueue:adhoc --namespace kueue-optimizer-chamber-exp1 --port 16451
     ```
2. **Never push ad-hoc or experimental Kueue images to external remote registries:** Do not push temporary/development Kueue images to shared external registries (`gcr.io`, `*.pkg.dev` / Artifact Registry, Docker Hub, etc.) for chamber evaluation.
3. **Never bypass the chamber registry with manual node mutations or manifest edits:** Never run manual `ctr -n k8s.io images import` on worker nodes, never deploy privileged node DaemonSets or modify node `containerd` configs, and never hardcode ad-hoc tags or `localhost:<port>/...` digests into `deploy/base/kueue.yaml` or other tracked manifests under `deploy/`.
4. **Maintain strict per-chamber image isolation across A/B arms:** When comparing two Kueue versions (`Arm A` vs. `Arm B`), push each arm's image into its own chamber's isolated registry (`kueue-optimizer-chamber-arm-a` vs. `kueue-optimizer-chamber-arm-b`). Deleting the chamber namespace automatically frees 100% of its `tmpfs` registry pages in RAM.

---

## Licensing & Path Hygiene

1. **Copyright headers:** Every authored source file, script, Makefile, and YAML configuration in this repository MUST include the Apache 2.0 `Copyright 2026 Google LLC` license header (`// Copyright 2026 Google LLC` for Go files; `# Copyright 2026 Google LLC` for Python, Shell, Makefile, and YAML files).
2. **Never hardcode machine-specific absolute paths:** Never write machine- or user-specific absolute filesystem paths (such as `/usr/local/...` or `/home/<user>/...`) into any Markdown documentation (`README.md`, `AGENTS.md`, `TESTRUNNER.md`, `SKILL.md`), generator script, or configuration file. Always use portable repository-relative paths (e.g., `deploy/scripts/testrunner.py`, `testscenarios/demo`, `skills/chamber-evaluation/SKILL.md`).

---

## Keep Repository Documentation & Project Structure in Sync (`README.md`, `TESTRUNNER.md`, `scraper/`)

**Mandatory rule:** Whenever you add, move, rename, or remove files, directories, packages, Make targets, CLI flags, chamber components, or test scenario suites, you MUST update the repository documentation — **especially `README.md` and its `Directory Structure` / project layout sections** — in the same change/commit so documentation never drifts from the codebase:

1. **`README.md` (`Directory Structure`, `Architecture`, `Prerequisites`, `Quick Start`):**
   - Always update the `Directory Structure` tree whenever top-level files, packages under `deploy/scripts/`, `scraper/` source/doc files, `testscenarios/` suites or generators, `skills/`, or `docs/` entries are added, moved, renamed, or deleted.
   - Keep the chamber `Architecture` table, image versions, `Prerequisites` (including `scraper/.go-version`), and `Quick Start` / test commands aligned with `deploy/base/`, `Makefile`, and `deploy/scripts/`.
   - Before committing, cross-check the `Directory Structure` tree in `README.md` against tracked files (`git ls-files`) to confirm nothing new is omitted and no removed paths remain listed.
2. **`TESTRUNNER.md`:** Update CLI flags, replay mechanics, or output section descriptions (`1`–`4`, `5.1`–`5.4`) whenever `deploy/scripts/testrunner.py`, `deploy/scripts/runner/`, `deploy/scripts/kueue_metrics_report.py`, or `deploy/scripts/metrics/` change user-facing behavior.
3. **`scraper/README.md` & `scraper/SCRAPER-SCHEMA.md`:** Update CLI flags, RBAC permissions, sanitization rules, and JSON Lines payload schemas whenever `scraper/` changes.

---

## Keep Repository Agent Skills (`skills/*/SKILL.md`) in Sync

**Mandatory rule:** Whenever you make significant changes to chamber orchestration, scenario schemas, scenario generation/README tooling, or metrics evaluation, you MUST review and update both `skills/chamber-evaluation/SKILL.md` and `skills/generate-testscenario/SKILL.md` in the same change/commit so all repository skills stay 100% accurate and current:

1. **Testrunner or Chamber Manager changes:** CLI flags, time-compression/duration logic, port-forwarding rules, prerequisite CR synthesis (`RuntimeClass`, `Topology`, `ResourceFlavor`, `Cohort`, `WorkloadPriorityClass`), or `infra-pods` handling in `deploy/scripts/testrunner.py`, `deploy/scripts/runner/`, `deploy/scripts/chamber_common.py`, or `deploy/scripts/chamber/`.
2. **Chamber deployment or manifest changes:** Control-plane topology, component versions, KWOK stage rules, or bootstrap manifests in `deploy/base/`, `deploy/overlays/`, or `deploy/scripts/generate.sh`.
3. **Metrics report changes:** Formulas, columns, sections (`5.1`–`5.4`), subtitles, or workload classification rules in `deploy/scripts/kueue_metrics_report.py` or `deploy/scripts/metrics/`.
4. **Scenario schema, generator, or methodology changes:** `scraper/SCRAPER-SCHEMA.md`, `deploy/scripts/generate_scenario_readme.py`, `deploy/scripts/scenario_readme/`, scenario directory conventions, or per-scenario `AGENTS.md` requirements.

---

## Always Commit with High-Verbosity, Structured Messages

**Mandatory rule:** Every Git commit MUST include a clear subject line followed by a detailed, multi-section body covering all three of the following:

1. **Goal & Motivation (`Why`):** Explain the underlying objective, problem statement, or benchmark/engineering requirement that motivated the change.
2. **Detailed Summary of Changes (`What` & `How`):** Provide a concrete technical breakdown of every meaningful code, configuration, schema, generator, or documentation modification (including key formulas, parameters, flags, data structures, and any regenerated files).
3. **Side Effects, Trade-offs & Implications:** Explicitly document potential side effects, behavioral shifts, impact on benchmark results or scheduler metrics, interactions with existing scenarios/scripts, backward-compatibility considerations, and operational caveats.
