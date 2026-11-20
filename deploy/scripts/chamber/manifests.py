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

"""Kubernetes/Kueue manifest builders and deployment manifest patch helpers."""

from __future__ import annotations

from pathlib import Path
import re
from typing import Any

from chamber.constants import (
    DEFAULT_FAIR_SHARING_PREEMPTION_STRATEGIES,
    DEFAULT_FLAVOR_NAME,
)
from chamber.parsers import parse_k8s_quantity


def build_kwok_node_manifest(
    name: str,
    node_idx: int,
    cpu: int,
    mem_gi: int,
    pods: int = 110,
    role: str = "agent",
    extra_labels: dict[str, str] | None = None,
    extra_annotations: dict[str, str] | None = None,
    accel_resource: str | None = None,
    accel_count: int | None = None,
    use_legacy_cidr: bool = False,
) -> dict[str, Any]:
    """Build a simulated KWOK Node manifest with annotations for Stage/node-initialize."""
    if use_legacy_cidr and node_idx < 256:
        cidr = f"10.244.{node_idx}.0/24"
    else:
        octet2 = 100 + (node_idx // 256)
        octet3 = node_idx % 256
        cidr = f"10.{octet2}.{octet3}.0/24"

    annotations: dict[str, str] = {
        "kwok.x-k8s.io/node": "fake",
        "node.alpha.kubernetes.io/ttl": "0",
        "kueue-optimizer.gke.io/node-cpu": str(cpu),
        "kueue-optimizer.gke.io/node-memory": f"{mem_gi}Gi",
        "kueue-optimizer.gke.io/node-pods": str(pods),
    }
    if accel_resource and accel_count:
        annotations["kueue-optimizer.gke.io/node-accel-resource"] = accel_resource
        annotations["kueue-optimizer.gke.io/node-accel-count"] = str(accel_count)
    if extra_annotations:
        annotations.update(extra_annotations)

    labels: dict[str, str] = {
        "beta.kubernetes.io/arch": "amd64",
        "beta.kubernetes.io/os": "linux",
        "kubernetes.io/arch": "amd64",
        "kubernetes.io/hostname": name,
        "kubernetes.io/os": "linux",
        "kubernetes.io/role": "agent",
        "node-role.kubernetes.io/agent": "",
        "type": "kwok",
        "kueue-optimizer.gke.io/node-role": role,
    }
    if extra_labels:
        labels.update(extra_labels)

    resources: dict[str, str] = {
        "cpu": str(cpu),
        "memory": f"{mem_gi}Gi",
        "pods": str(pods),
    }
    if accel_resource and accel_count:
        resources[accel_resource] = str(accel_count)

    return {
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {
            "name": name,
            "annotations": annotations,
            "labels": labels,
        },
        "spec": {
            "podCIDR": cidr,
            "podCIDRs": [cidr],
        },
        "status": {
            "allocatable": dict(resources),
            "capacity": dict(resources),
            "nodeInfo": {
                "architecture": "amd64",
                "kubeProxyVersion": "fake",
                "kubeletVersion": "fake",
                "operatingSystem": "linux",
            },
        },
    }


def build_namespace_manifest(
    name: str,
    part_of: str = "kueue-optimizer",
    exclude_from_kueue: bool = False,
    extra_labels: dict[str, str] | None = None,
) -> dict[str, Any]:
    labels: dict[str, str] = {
        "app.kubernetes.io/part-of": part_of,
    }
    if exclude_from_kueue:
        labels["kueue.x-k8s.io/exclude"] = "true"
        labels["kueue-optimizer.gke.io/kueue-exclude"] = "true"
    if extra_labels:
        labels.update(extra_labels)
    return {
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": name,
            "labels": labels,
        },
    }


def build_resource_flavor_manifest(
    name: str = DEFAULT_FLAVOR_NAME,
    node_labels: dict[str, str] | None = None,
) -> dict[str, Any]:
    return {
        "apiVersion": "kueue.x-k8s.io/v1beta2",
        "kind": "ResourceFlavor",
        "metadata": {"name": name},
        "spec": {
            "nodeLabels": node_labels if node_labels is not None else {"type": "kwok"},
        },
    }


def build_priority_class_manifest(name: str, value: int) -> dict[str, Any]:
    return {
        "apiVersion": "kueue.x-k8s.io/v1beta2",
        "kind": "WorkloadPriorityClass",
        "metadata": {"name": name},
        "value": int(value),
    }


def build_cohort_manifest(name: str) -> dict[str, Any]:
    return {
        "apiVersion": "kueue.x-k8s.io/v1beta2",
        "kind": "Cohort",
        "metadata": {"name": name},
    }


def build_cluster_queue_manifest(
    name: str,
    flavor_name: str,
    resources: list[dict[str, str]],
    queueing_strategy: str = "StrictFIFO",
    cohort_name: str | None = None,
    enable_preemption: bool = True,
    enable_fair_sharing: bool = True,
    fair_sharing_weight: int | str | None = None,
) -> dict[str, Any]:
    covered = [r["name"] for r in resources]
    spec: dict[str, Any] = {
        "namespaceSelector": {},
        "queueingStrategy": queueing_strategy,
        "resourceGroups": [
            {
                "coveredResources": covered,
                "flavors": [
                    {
                        "name": flavor_name,
                        "resources": resources,
                    }
                ],
            }
        ],
    }
    if cohort_name:
        spec["cohortName"] = cohort_name
    if enable_fair_sharing:
        spec["fairSharing"] = {"weight": 1 if fair_sharing_weight is None else fair_sharing_weight}
    elif fair_sharing_weight is not None:
        spec["fairSharing"] = {"weight": fair_sharing_weight}
    if enable_preemption:
        preemption_spec: dict[str, Any] = {
            "withinClusterQueue": "LowerPriority",
            "reclaimWithinCohort": "Any",
        }
        if not enable_fair_sharing:
            preemption_spec["borrowWithinCohort"] = {
                "policy": "LowerPriority",
            }
        spec["preemption"] = preemption_spec
    return {
        "apiVersion": "kueue.x-k8s.io/v1beta2",
        "kind": "ClusterQueue",
        "metadata": {"name": name},
        "spec": spec,
    }


def build_local_queue_manifest(name: str, namespace: str, cluster_queue: str) -> dict[str, Any]:
    return {
        "apiVersion": "kueue.x-k8s.io/v1beta2",
        "kind": "LocalQueue",
        "metadata": {
            "name": name,
            "namespace": namespace,
        },
        "spec": {
            "clusterQueue": cluster_queue,
        },
    }


def match_yaml_section_field(yaml_text: str, section_key: str, field_key: str) -> str | None:
    """Return the scalar value of `field_key` inside an indented YAML mapping `section_key:` (or None)."""
    pattern = (
        rf"^(?P<indent>[ \t]*)(?:-[ \t]+)?{re.escape(section_key)}:[ \t]*(?:#[^\n]*)?\n"
        rf"(?:(?P=indent)[ \t]+(?![- \t]*{re.escape(field_key)}:[ \t]*)[A-Za-z0-9_./-]+:[ \t]*[^\n]*\n)*"
        rf"(?P=indent)[ \t]+{re.escape(field_key)}:[ \t]*(?P<val>['\"]?[0-9A-Za-z._+-]+['\"]?)"
    )
    m = re.search(pattern, yaml_text, flags=re.MULTILINE)
    return m.group("val") if m else None


def replace_yaml_section_field(yaml_text: str, section_key: str, field_key: str, new_value: str) -> str:
    """Replace the scalar value of `field_key` inside any `section_key:` YAML mapping without depending on its prior value."""
    pattern = (
        rf"(^(?P<indent>[ \t]*)(?:-[ \t]+)?{re.escape(section_key)}:[ \t]*(?:#[^\n]*)?\n"
        rf"(?:(?P=indent)[ \t]+(?![- \t]*{re.escape(field_key)}:[ \t]*)[A-Za-z0-9_./-]+:[ \t]*[^\n]*\n)*"
        rf"(?P=indent)[ \t]+{re.escape(field_key)}:[ \t]*)['\"]?[0-9A-Za-z._+-]+['\"]?"
    )
    return re.sub(pattern, rf"\g<1>{new_value}", yaml_text, flags=re.MULTILINE)


def patch_registry_manifest(base_dst: Path, node_port: int, memory_limit: str) -> None:
    reg_yaml = base_dst / "registry.yaml"
    if not reg_yaml.exists():
        return
    reg_text = reg_yaml.read_text(encoding="utf-8")
    reg_text = re.sub(
        r"^([ \t]*nodePort:[ \t]*)\d+",
        rf"\g<1>{node_port}",
        reg_text,
        flags=re.MULTILINE,
    )
    mem_lim = (memory_limit or "").strip()
    if mem_lim:
        reg_text = replace_yaml_section_field(reg_text, "emptyDir", "sizeLimit", mem_lim)
        reg_text = replace_yaml_section_field(reg_text, "limits", "memory", mem_lim)
        # Ensure requests.memory never exceeds limits.memory when a smaller memory limit is requested
        cur_req = match_yaml_section_field(reg_text, "requests", "memory")
        if cur_req is not None:
            req_gib = parse_k8s_quantity(cur_req, "memory")
            lim_gib = parse_k8s_quantity(mem_lim, "memory")
            if lim_gib > 0 and req_gib > lim_gib:
                reg_text = replace_yaml_section_field(reg_text, "requests", "memory", mem_lim)
    reg_yaml.write_text(reg_text, encoding="utf-8")


def patch_kueue_fair_sharing_config(base_dst: Path, enable_fair_sharing: bool) -> None:
    """Enable or disable Preemption-Based Fair Sharing in controller_manager_config.yaml."""
    cfg_yaml = base_dst / "config" / "controller_manager_config.yaml"
    if not cfg_yaml.exists():
        return
    cfg_text = cfg_yaml.read_text(encoding="utf-8")
    # Strip any existing top-level fairSharing block cleanly
    cfg_text = re.sub(
        r"^fairSharing:[ \t]*(?:#[^\n]*)?\n(?:[ \t]+[^\n]*\n|-+[ \t]*[^\n]*\n)*",
        "",
        cfg_text,
        flags=re.MULTILINE,
    )
    if enable_fair_sharing:
        fs_lines = [
            "fairSharing:",
            "  preemptionStrategies:",
            *[f"  - {s}" for s in DEFAULT_FAIR_SHARING_PREEMPTION_STRATEGIES],
        ]
        fs_block = "\n".join(fs_lines) + "\n"
        if re.search(r"^internalCertManagement:", cfg_text, flags=re.MULTILINE):
            cfg_text = re.sub(
                r"^internalCertManagement:",
                f"{fs_block}internalCertManagement:",
                cfg_text,
                count=1,
                flags=re.MULTILINE,
            )
        else:
            cfg_text = cfg_text.rstrip("\n") + "\n" + fs_block
    cfg_yaml.write_text(cfg_text, encoding="utf-8")


def patch_kustomize_kueue_image(base_dst: Path, image_ref: str) -> None:
    ref = image_ref.strip()
    if not ref:
        return
    kueue_yaml = base_dst / "kueue.yaml"
    if kueue_yaml.exists():
        k_text = kueue_yaml.read_text(encoding="utf-8")
        k_text = re.sub(
            r"(-\s+name:\s+manager\s*\n\s+image:\s*)\S+(\s*\n\s+imagePullPolicy:\s*)\S+",
            rf"\g<1>{ref}\g<2>Always",
            k_text,
            count=1,
        )
        kueue_yaml.write_text(k_text, encoding="utf-8")

    kust_yaml = base_dst / "kustomization.yaml"
    if kust_yaml.exists():
        kust_text = kust_yaml.read_text(encoding="utf-8")
        # Remove the default kueue images override block so the explicit image in kueue.yaml wins verbatim
        kust_text = re.sub(
            r"-\s+name:\s+us-central1-docker\.pkg\.dev/k8s-staging-images/kueue/kueue\s*\n"
            r"\s+newName:\s+\S+\s*\n"
            r"\s+newTag:\s+\S+\s*\n?",
            "",
            kust_text,
        )
        kust_yaml.write_text(kust_text, encoding="utf-8")
