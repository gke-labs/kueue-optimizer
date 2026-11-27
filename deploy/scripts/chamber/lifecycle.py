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

"""ChamberManager lifecycle orchestration, Prometheus scrape waiting, and live cluster status queries."""

from __future__ import annotations

import atexit
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
import json
from pathlib import Path
import re
import shutil
import ssl
import subprocess
import sys
import tempfile
import threading
import time
from typing import Any
import urllib.request

from chamber.constants import (
    CHAMBER_CORE_HEALTH_COMPONENTS,
    CHAMBER_DEPLOYMENTS,
    CHAMBER_NAMESPACE_LABEL_KEY,
    CHAMBER_NAMESPACE_LABEL_VALUE,
    DEFAULT_APISERVER_PORT,
    DEFAULT_KUEUE_IMAGE,
    DEFAULT_REGISTRY_MEMORY_LIMIT,
    DEFAULT_SCRAPE_INTERVAL,
    DEPLOY_DIR,
    SCRIPT_DIR,
    validate_dns_label,
)
from chamber.k8s_client import ChamberK8sClientMixin
from chamber.manifests import (
    AdmissionFairSharingConfig,
    WaitForPodsReadyConfig,
    match_yaml_section_field,
    parse_fair_sharing_strategies,
    parse_feature_gates_arg,
    patch_kueue_admission_fair_sharing_config,
    patch_kueue_extra_configuration_fields,
    patch_kueue_fair_sharing_config,
    patch_kueue_feature_gates_config,
    patch_kueue_wait_for_pods_ready_config,
    patch_kustomize_kueue_image,
    patch_registry_manifest,
    reconcile_wait_for_pods_ready_feature_gates,
    replace_yaml_section_field,
    sanitize_kueue_feature_gates,
)
from chamber.parsers import (
    compute_prometheus_port,
    compute_registry_port,
    normalize_scrape_interval,
    resolve_kubeconfig_path,
)
from chamber.pki import ensure_pki_dir, inject_ca_bundle_into_manifests
from chamber.registry import ChamberRegistryMixin
from chamber.yaml_utils import docs_to_pretty_yaml


@dataclass(frozen=True)
class ComponentHealthStatus:
    """Health and container-restart summary for one chamber control-plane deployment."""

    deployment: str
    ready_pods: int
    total_pods: int
    restart_count: int
    pod_uids: tuple[str, ...] = ()


@dataclass(frozen=True)
class ChamberHealthSnapshot:
    """Point-in-time health snapshot of core chamber control-plane deployments."""

    components: dict[str, ComponentHealthStatus] = field(default_factory=dict)
    query_ok: bool = True
    error: str = ""


class ChamberManager(ChamberK8sClientMixin, ChamberRegistryMixin):
    """Manages connection to the kueue-optimizer chamber in the host cluster and its KWOK apiserver."""

    _match_yaml_section_field = staticmethod(match_yaml_section_field)
    _replace_yaml_section_field = staticmethod(replace_yaml_section_field)
    _patch_registry_manifest = staticmethod(patch_registry_manifest)
    _patch_kueue_fair_sharing_config = staticmethod(patch_kueue_fair_sharing_config)
    _patch_kueue_admission_fair_sharing_config = staticmethod(patch_kueue_admission_fair_sharing_config)
    _patch_kueue_wait_for_pods_ready_config = staticmethod(patch_kueue_wait_for_pods_ready_config)
    _patch_kueue_feature_gates_config = staticmethod(patch_kueue_feature_gates_config)
    _patch_kueue_extra_configuration_fields = staticmethod(patch_kueue_extra_configuration_fields)
    _patch_kustomize_kueue_image = staticmethod(patch_kustomize_kueue_image)

    def __init__(
        self,
        chamber_namespace: str,
        workload_namespace: str = "default",
        apiserver_port: int = DEFAULT_APISERVER_PORT,
        prometheus_port: int | None = None,
        scrape_interval: str = DEFAULT_SCRAPE_INTERVAL,
        kubeconfig_out: Path | str | None = None,
        registry_port: int | None = None,
        registry_memory_limit: str = DEFAULT_REGISTRY_MEMORY_LIMIT,
        kueue_image: str | None = None,
        kueue_build_dir: Path | str | None = None,
        enable_fair_sharing: bool = True,
        fair_sharing_strategies: tuple[str, ...] | list[str] | str | None = None,
        admission_fair_sharing: AdmissionFairSharingConfig | None = None,
        wait_for_pods_ready: WaitForPodsReadyConfig | None = None,
        kueue_feature_gates: dict[str, bool] | str | None = None,
        kueue_resources_config: dict[str, Any] | None = None,
        manage_jobs_without_queue_name: bool | None = None,
    ) -> None:
        self.chamber_namespace = validate_dns_label(chamber_namespace, "chamber namespace")
        self.workload_namespace = validate_dns_label(workload_namespace, "workload namespace")
        self.apiserver_port = apiserver_port
        self.prometheus_port = compute_prometheus_port(apiserver_port, prometheus_port)
        self._explicit_registry_port = registry_port
        self.registry_port = compute_registry_port(apiserver_port, registry_port)
        self.registry_memory_limit = (registry_memory_limit or DEFAULT_REGISTRY_MEMORY_LIMIT).strip()
        self.kueue_image = (kueue_image or "").strip() or None
        self.kueue_build_dir = Path(kueue_build_dir).resolve() if kueue_build_dir else None
        self._resolved_kueue_image: str | None = None
        self.enable_fair_sharing = bool(enable_fair_sharing)
        self.fair_sharing_strategies = parse_fair_sharing_strategies(fair_sharing_strategies)
        self.admission_fair_sharing = admission_fair_sharing
        self.wait_for_pods_ready = wait_for_pods_ready
        self.kueue_feature_gates = parse_feature_gates_arg(kueue_feature_gates)
        self.kueue_resources_config = dict(kueue_resources_config) if kueue_resources_config else None
        self.manage_jobs_without_queue_name = manage_jobs_without_queue_name
        self._rendered_controller_manager_config_yaml: str | None = None
        self.scrape_interval = normalize_scrape_interval(scrape_interval)
        self.kubeconfig_path = resolve_kubeconfig_path(self.chamber_namespace, kubeconfig_out)
        self._created_namespace_in_this_run: bool = False

        self._apiserver_pf_proc: subprocess.Popen[bytes] | None = None
        self._prom_pf_proc: subprocess.Popen[bytes] | None = None
        self._registry_pf_proc: subprocess.Popen[bytes] | None = None
        self._pf_lock = threading.Lock()
        self._executor: ThreadPoolExecutor | None = None
        self._tmp_deploy_dir: Path | None = None
        self._tmp_pki_dir: Path | None = None
        self._pki_source: str | None = None
        self._ssl_ctx: ssl.SSLContext | None = None
        self._tls_local = threading.local()
        atexit.register(self.close)

    @property
    def created_namespace_in_this_run(self) -> bool:
        """True only when this ChamberManager instance created a previously absent namespace during apply."""
        return self._created_namespace_in_this_run

    def _ensure_namespace_safe_for_apply(self, *, record_creation: bool = True) -> bool:
        """Verify target host namespace before server-side apply.

        Returns True if the namespace already existed (and already carries the chamber ownership label),
        False if the namespace does not yet exist (recording creation when record_creation=True).
        Raises RuntimeError if the namespace already exists without `kueue-optimizer.gke.io/chamber=true`.
        """
        get_proc = self.host_kubectl(
            ["get", "namespace", "-o", "json", "--", self.chamber_namespace],
            check=False,
            timeout=30.0,
        )
        if get_proc.returncode != 0:
            err_msg = (get_proc.stderr or "").strip()
            if "notfound" in err_msg.lower() or "not found" in err_msg.lower():
                if record_creation:
                    self._created_namespace_in_this_run = True
                return False
            raise RuntimeError(
                f"Failed to inspect namespace '{self.chamber_namespace}' before apply "
                f"(exit {get_proc.returncode}): {err_msg}"
            )

        try:
            ns_obj = json.loads(get_proc.stdout or "{}")
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"Failed to parse namespace '{self.chamber_namespace}' JSON before apply: {exc}"
            ) from exc

        labels = ((ns_obj.get("metadata") or {}).get("labels")) or {}
        if labels.get(CHAMBER_NAMESPACE_LABEL_KEY) != CHAMBER_NAMESPACE_LABEL_VALUE:
            raise RuntimeError(
                f"Refusing to deploy chamber into existing namespace '{self.chamber_namespace}': "
                f"missing chamber ownership label ('{CHAMBER_NAMESPACE_LABEL_KEY}={CHAMBER_NAMESPACE_LABEL_VALUE}')."
            )
        return True

    def read_live_kueue_config_yaml(self) -> str | None:
        """Read ConfigMap/kueue-manager-config data['controller_manager_config.yaml'] from the host chamber namespace."""
        proc = self.host_kubectl(
            ["-n", self.chamber_namespace, "get", "configmap", "kueue-manager-config", "-o", "json"],
            check=False,
            timeout=15.0,
        )
        if proc.returncode != 0 or not proc.stdout:
            return None
        try:
            cm_obj = json.loads(proc.stdout)
        except json.JSONDecodeError:
            return None
        cfg_yaml = str(((cm_obj.get("data") or {}).get("controller_manager_config.yaml")) or "")
        return cfg_yaml if cfg_yaml.strip() else None

    def detect_live_fair_sharing_enabled(self) -> bool | None:
        """Read ConfigMap/kueue-manager-config in the chamber namespace to detect whether fairSharing is active."""
        cfg_yaml = self.read_live_kueue_config_yaml()
        if not cfg_yaml:
            return None
        return bool(re.search(r"^fairSharing\s*:", cfg_yaml, flags=re.MULTILINE))

    def sync_kueue_configuration_to_kwok_apiserver(self) -> None:
        """Mirror active ConfigMap/kueue-manager-config into `kueue-system` inside the chamber KWOK apiserver.

        Allows `cluster-scraper` connected to the chamber kubeconfig to discover and record the active
        `config.kueue.x-k8s.io/v1beta2` `Configuration` into `infra.json` identically to a vanilla cluster.
        """
        cfg_yaml = self._rendered_controller_manager_config_yaml or self.read_live_kueue_config_yaml()
        if not cfg_yaml:
            return
        ns_doc = {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "kueue-system"}}
        st_ns, _ = self.api_request("GET", "/api/v1/namespaces/kueue-system")
        if st_ns == 404:
            self.api_request(
                "POST",
                "/api/v1/namespaces",
                body=json.dumps(ns_doc).encode("utf-8"),
                content_type="application/json",
            )
        cm_doc = {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "kueue-manager-config", "namespace": "kueue-system"},
            "data": {"controller_manager_config.yaml": cfg_yaml},
        }
        body = json.dumps(cm_doc).encode("utf-8")
        st_get, raw_get = self.api_request("GET", "/api/v1/namespaces/kueue-system/configmaps/kueue-manager-config")
        if st_get == 200 and raw_get:
            try:
                existing = json.loads(raw_get.decode("utf-8"))
                rv = (existing.get("metadata") or {}).get("resourceVersion")
                if rv:
                    cm_doc["metadata"]["resourceVersion"] = rv
                    body = json.dumps(cm_doc).encode("utf-8")
            except Exception:
                pass
            self.api_request(
                "PUT",
                "/api/v1/namespaces/kueue-system/configmaps/kueue-manager-config",
                body=body,
                content_type="application/json",
            )
        else:
            self.api_request(
                "POST",
                "/api/v1/namespaces/kueue-system/configmaps",
                body=body,
                content_type="application/json",
            )

    def ensure_pki(self, prefer_cluster: bool = True) -> Path:
        """Obtain PKI from Secret/kwok-pki in the cluster if present, or generate ad-hoc."""
        if self._tmp_pki_dir is None or not self._tmp_pki_dir.exists():
            self._tmp_pki_dir = Path(tempfile.mkdtemp(prefix="kueue-optimizer-pki-"))
            self._ssl_ctx = None
        _, source = ensure_pki_dir(
            chamber_namespace=self.chamber_namespace,
            target_pki_dir=self._tmp_pki_dir,
            prefer_cluster=prefer_cluster,
        )
        if self._pki_source is None:
            self._pki_source = source
        return self._tmp_pki_dir

    def close(self) -> None:
        if self._executor is not None:
            try:
                self._executor.shutdown(wait=False, cancel_futures=True)
            except Exception:
                pass
            self._executor = None
        for proc in (self._apiserver_pf_proc, self._prom_pf_proc, self._registry_pf_proc):
            if proc is not None and proc.poll() is None:
                try:
                    proc.terminate()
                    proc.wait(timeout=3)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass
        self._apiserver_pf_proc = None
        self._prom_pf_proc = None
        self._registry_pf_proc = None
        for tmp_d in (self._tmp_deploy_dir, self._tmp_pki_dir):
            if tmp_d and tmp_d.exists():
                shutil.rmtree(tmp_d, ignore_errors=True)
        self._tmp_deploy_dir = None
        self._tmp_pki_dir = None
        self._ssl_ctx = None

    def prepare_tmp_deploy_dir(
        self,
        initial_nodes: list[dict[str, Any]] | None = None,
        include_overlays: bool = False,
        prefer_cluster_pki: bool = True,
        check_cluster_ports: bool = False,
    ) -> Path:
        if check_cluster_ports:
            self._resolve_cluster_registry_port(check_cluster=True)

        if self._tmp_deploy_dir is None or not self._tmp_deploy_dir.exists():
            self._tmp_deploy_dir = Path(tempfile.mkdtemp(prefix="kueue-optimizer-chamber-deploy-"))
        base_dst = self._tmp_deploy_dir / "base"
        if base_dst.exists():
            shutil.rmtree(base_dst)
        shutil.copytree(DEPLOY_DIR / "base", base_dst)

        # Populate PKI (from cluster Secret/kwok-pki if running, or generated ad-hoc) and inject caBundle
        pki_src = self.ensure_pki(prefer_cluster=prefer_cluster_pki)
        pki_dst = base_dst / "pki"
        if pki_dst.exists():
            shutil.rmtree(pki_dst)
        shutil.copytree(pki_src, pki_dst)
        inject_ca_bundle_into_manifests(base_dst / "manifests", (pki_dst / "ca.crt").read_bytes())

        # Keep bootstrap kwok-nodes.yaml minimal when node count is large so ConfigMap/kwok-bootstrap
        # stays well under the 1MiB ConfigMap limit; nodes are synced via sync_kwok_nodes().
        nodes_file = base_dst / "manifests" / "kwok-nodes.yaml"
        if initial_nodes is not None:
            if len(initial_nodes) <= 50 and len(initial_nodes) > 0:
                nodes_file.write_text(docs_to_pretty_yaml(initial_nodes), encoding="utf-8")
            else:
                nodes_file.write_text("# Managed by ChamberManager\n", encoding="utf-8")

        prom_cfg = base_dst / "config" / "prometheus.yaml"
        prom_text = prom_cfg.read_text(encoding="utf-8")
        prom_text = re.sub(
            r"^(\s*(?:scrape|evaluation)_interval:).*",
            rf"\1 {self.scrape_interval}",
            prom_text,
            flags=re.MULTILINE,
        )
        prom_cfg.write_text(prom_text, encoding="utf-8")

        ns_yaml = base_dst / "namespace.yaml"
        ns_text = re.sub(
            r"^(\s*name:\s*).*",
            rf"\g<1>{self.chamber_namespace}",
            ns_yaml.read_text(encoding="utf-8"),
            flags=re.MULTILINE,
        )
        ns_yaml.write_text(ns_text, encoding="utf-8")

        kust_yaml = base_dst / "kustomization.yaml"
        kust_text = re.sub(
            r"^(namespace:\s*).*",
            rf"\g<1>{self.chamber_namespace}",
            kust_yaml.read_text(encoding="utf-8"),
            flags=re.MULTILINE,
        )
        kust_yaml.write_text(kust_text, encoding="utf-8")

        eff_wfpr, eff_fg = reconcile_wait_for_pods_ready_feature_gates(
            self.wait_for_pods_ready,
            self.kueue_feature_gates,
        )
        allow_unknown_fg = bool(self.kueue_image and not self.kueue_build_dir)
        self._patch_registry_manifest(base_dst, self.registry_port, self.registry_memory_limit)
        self._patch_kueue_fair_sharing_config(base_dst, self.enable_fair_sharing, self.fair_sharing_strategies)
        self._patch_kueue_admission_fair_sharing_config(base_dst, self.admission_fair_sharing)
        self._patch_kueue_wait_for_pods_ready_config(base_dst, eff_wfpr)
        self._patch_kueue_feature_gates_config(
            base_dst,
            eff_fg,
            kueue_build_dir=self.kueue_build_dir,
            allow_unknown=allow_unknown_fg,
            emit_warnings=True,
        )
        self._patch_kueue_extra_configuration_fields(
            base_dst,
            resources_block=self.kueue_resources_config,
            manage_jobs_without_queue_name=self.manage_jobs_without_queue_name,
        )
        cfg_file = base_dst / "config" / "controller_manager_config.yaml"
        if cfg_file.is_file():
            self._rendered_controller_manager_config_yaml = cfg_file.read_text(encoding="utf-8")

        active_kueue_img = self._resolved_kueue_image or self.kueue_image
        if active_kueue_img:
            self._patch_kustomize_kueue_image(base_dst, active_kueue_img)

        if include_overlays:
            overlays_dst = self._tmp_deploy_dir / "overlays"
            if overlays_dst.exists():
                shutil.rmtree(overlays_dst)
            shutil.copytree(DEPLOY_DIR / "overlays", overlays_dst)
            ov_kust = overlays_dst / "with-sample-queues" / "kustomization.yaml"
            if ov_kust.exists():
                ov_kust.write_text(
                    re.sub(
                        r"^(namespace:\s*).*",
                        rf"\g<1>{self.chamber_namespace}",
                        ov_kust.read_text(encoding="utf-8"),
                        flags=re.MULTILINE,
                    ),
                    encoding="utf-8",
                )
            root_kust = self._tmp_deploy_dir / "kustomization.yaml"
            shutil.copy2(DEPLOY_DIR / "kustomization.yaml", root_kust)
            root_kust.write_text(
                re.sub(
                    r"^(namespace:\s*).*",
                    rf"\g<1>{self.chamber_namespace}",
                    root_kust.read_text(encoding="utf-8"),
                    flags=re.MULTILINE,
                ),
                encoding="utf-8",
            )
        return base_dst

    def _wait_for_kueue_rollout_with_fg_recovery(
        self,
        base_dst: Path,
        active_fg: dict[str, bool],
    ) -> None:
        """Wait for deployment/kueue-controller-manager, auto-stripping any gate rejected by a newer Kueue image."""
        fg_state = dict(active_fg)
        deadline = time.monotonic() + 300.0
        while time.monotonic() < deadline:
            dep_proc = self.host_kubectl(
                ["-n", self.chamber_namespace, "get", "deployment", "kueue-controller-manager", "-o", "json"],
                check=False,
                timeout=20.0,
            )
            if dep_proc.returncode == 0 and dep_proc.stdout:
                try:
                    dep_obj = json.loads(dep_proc.stdout)
                except json.JSONDecodeError:
                    dep_obj = {}
                spec_reps = int(((dep_obj.get("spec") or {}).get("replicas")) or 1)
                dep_st = dep_obj.get("status") or {}
                obs_gen = int(dep_st.get("observedGeneration") or 0)
                meta_gen = int(((dep_obj.get("metadata") or {}).get("generation")) or 1)
                upd_reps = int(dep_st.get("updatedReplicas") or 0)
                avail_reps = int(dep_st.get("availableReplicas") or 0)
                ready_reps = int(dep_st.get("readyReplicas") or 0)
                if obs_gen >= meta_gen and upd_reps >= spec_reps and avail_reps >= spec_reps and ready_reps >= spec_reps:
                    return

            if fg_state:
                pods_proc = self.host_kubectl(
                    [
                        "-n",
                        self.chamber_namespace,
                        "get",
                        "pods",
                        "-l",
                        "control-plane=controller-manager",
                        "-o",
                        "json",
                    ],
                    check=False,
                    timeout=20.0,
                )
                rejected_gates: set[str] = set()
                if pods_proc.returncode == 0 and pods_proc.stdout:
                    try:
                        pod_items = json.loads(pods_proc.stdout).get("items") or []
                    except json.JSONDecodeError:
                        pod_items = []
                    for p_it in pod_items:
                        p_name = str(((p_it.get("metadata") or {}).get("name")) or "")
                        if not p_name or (p_it.get("metadata") or {}).get("deletionTimestamp"):
                            continue
                        c_statuses = ((p_it.get("status") or {}).get("containerStatuses")) or []
                        crashed = False
                        use_prev = False
                        for cs in c_statuses:
                            st_c = cs.get("state") or {}
                            last_c = cs.get("lastState") or {}
                            term_cur = st_c.get("terminated") or {}
                            term_prev = last_c.get("terminated") or {}
                            wait_cur = st_c.get("waiting") or {}
                            if term_cur and int(term_cur.get("exitCode") or 0) != 0:
                                crashed = True
                            elif wait_cur.get("reason") in ("CrashLoopBackOff", "Error") and term_prev:
                                crashed = True
                                use_prev = True
                        if not crashed:
                            continue
                        log_args = ["-n", self.chamber_namespace, "logs", p_name, "-c", "manager", "--tail=80"]
                        if use_prev:
                            log_args.append("--previous")
                        log_proc = self.host_kubectl(log_args, check=False, timeout=15.0)
                        log_txt = f"{log_proc.stdout or ''}\n{log_proc.stderr or ''}"
                        for m_unrec in re.finditer(r"unrecognized feature gate:\s*([A-Za-z0-9_]+)", log_txt):
                            if m_unrec.group(1) in fg_state:
                                rejected_gates.add(m_unrec.group(1))
                        for m_lock in re.finditer(r"cannot set feature gate\s+([A-Za-z0-9_]+)", log_txt):
                            if m_lock.group(1) in fg_state:
                                rejected_gates.add(m_lock.group(1))
                if rejected_gates:
                    dropped_list = ", ".join(sorted(rejected_gates))
                    print(
                        f"WARN: Chamber Kueue image rejected featureGate(s) [{dropped_list}]; "
                        "stripping from kueue-manager-config and restarting kueue-controller-manager...",
                        flush=True,
                    )
                    for rg in rejected_gates:
                        fg_state.pop(rg, None)
                    self._patch_kueue_feature_gates_config(
                        base_dst,
                        fg_state,
                        kueue_build_dir=self.kueue_build_dir,
                        allow_unknown=bool(self.kueue_image and not self.kueue_build_dir),
                        emit_warnings=False,
                    )
                    cfg_file = base_dst / "config" / "controller_manager_config.yaml"
                    if cfg_file.is_file():
                        self._rendered_controller_manager_config_yaml = cfg_file.read_text(encoding="utf-8")
                    self.host_kubectl(
                        ["apply", "--server-side", "--force-conflicts", "-k", str(base_dst)],
                        stream_output=True,
                    )
                    self.host_kubectl(
                        ["-n", self.chamber_namespace, "rollout", "restart", "deployment/kueue-controller-manager"],
                        stream_output=True,
                    )
            time.sleep(2.0)

        # Final standard rollout check to surface kubectl's exact status error on timeout.
        self.host_kubectl(
            [
                "-n",
                self.chamber_namespace,
                "rollout",
                "status",
                "--timeout=30s",
                "deployment/kueue-controller-manager",
            ],
            timeout=45.0,
            stream_output=True,
        )

    def deploy_chamber(self, initial_nodes: list[dict[str, Any]] | None = None) -> None:
        self._ensure_namespace_safe_for_apply(record_creation=False)
        self._resolve_cluster_registry_port(check_cluster=True)
        self._prepare_adhoc_kueue_image_before_deploy()
        base_dst = self.prepare_tmp_deploy_dir(initial_nodes=initial_nodes, check_cluster_ports=False)
        pki_note = "cluster Secret/kwok-pki" if self._pki_source == "cluster" else "ad-hoc generated PKI"
        kueue_img_note = self._resolved_kueue_image or DEFAULT_KUEUE_IMAGE
        fs_note = (
            f"enabled [{', '.join(self.fair_sharing_strategies)}]"
            if self.enable_fair_sharing
            else "disabled"
        )
        afs_note = (
            f", AdmissionFairSharing: [{self.admission_fair_sharing.summary_str()}]"
            if self.admission_fair_sharing and self.admission_fair_sharing.enabled
            else ""
        )
        eff_wfpr_log, raw_fg_log = reconcile_wait_for_pods_ready_feature_gates(
            self.wait_for_pods_ready,
            self.kueue_feature_gates,
        )
        eff_fg_log, _ = sanitize_kueue_feature_gates(
            raw_fg_log,
            kueue_build_dir=self.kueue_build_dir,
            allow_unknown=bool(self.kueue_image and not self.kueue_build_dir),
        )
        wfpr_note = (
            f", WaitForPodsReady: [{eff_wfpr_log.summary_str()}]"
            if eff_wfpr_log and (not eff_wfpr_log.enabled or eff_wfpr_log.configured)
            else ""
        )
        fg_note = (
            f", FeatureGates: {eff_fg_log}"
            if eff_fg_log
            else ""
        )
        print(
            f"==> Preparing & deploying chamber into host cluster namespace '{self.chamber_namespace}' "
            f"(PKI: {pki_note}, Registry: localhost:{self.registry_port} [RAM {self.registry_memory_limit}], "
            f"Kueue: {kueue_img_note}, FairSharing: {fs_note}{afs_note}{wfpr_note}{fg_note}, Prometheus scrape: {self.scrape_interval})...",
            flush=True,
        )
        self._ensure_namespace_safe_for_apply(record_creation=True)
        self.host_kubectl(
            ["apply", "--server-side", "--force-conflicts", "-k", str(base_dst)],
            stream_output=True,
        )

        print(
            f"==> Waiting for chamber control-plane Deployments in '{self.chamber_namespace}' to become Available...",
            flush=True,
        )
        for dep in CHAMBER_DEPLOYMENTS:
            if dep == "deployment/kueue-controller-manager" and eff_fg_log:
                self._wait_for_kueue_rollout_with_fg_recovery(base_dst, eff_fg_log)
                continue
            self.host_kubectl(
                [
                    "-n",
                    self.chamber_namespace,
                    "rollout",
                    "status",
                    "--timeout=300s",
                    dep,
                ],
                timeout=320.0,
                stream_output=True,
            )
        self.host_kubectl(
            [
                "-n",
                self.chamber_namespace,
                "wait",
                "--for=condition=Available",
                "--timeout=300s",
                *CHAMBER_DEPLOYMENTS,
            ],
            timeout=320.0,
            stream_output=True,
        )

    def write_local_kubeconfig(self, prefer_cluster: bool = True) -> Path:
        pki_dir = self.ensure_pki(prefer_cluster=prefer_cluster)
        raw_kc = (pki_dir / "kubeconfig").read_text(encoding="utf-8")
        kc_text = raw_kc.replace(
            "https://kwok-kube-apiserver:6443",
            f"https://127.0.0.1:{self.apiserver_port}",
        )
        self.kubeconfig_path.write_text(kc_text, encoding="utf-8")
        return self.kubeconfig_path

    def ensure_kwok_connection(self) -> None:
        self.write_local_kubeconfig(prefer_cluster=True)
        with self._pf_lock:
            self._start_or_restart_apiserver_pf_locked(timeout_sec=60.0)

    def ensure_prometheus_connection(self) -> None:
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        url = f"http://127.0.0.1:{self.prometheus_port}/-/ready"
        try:
            with opener.open(url, timeout=1.5):
                return
        except Exception:
            pass

        if self._prom_pf_proc is not None:
            try:
                self._prom_pf_proc.terminate()
                self._prom_pf_proc.wait(timeout=2)
            except Exception:
                try:
                    self._prom_pf_proc.kill()
                except Exception:
                    pass
            self._prom_pf_proc = None

        self._pkill_chamber_port_forward("prometheus", f"{self.prometheus_port}:9090")

        self._prom_pf_proc = subprocess.Popen(
            [
                "kubectl",
                "-n",
                self.chamber_namespace,
                "port-forward",
                "svc/prometheus",
                f"{self.prometheus_port}:9090",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        for _ in range(15):
            try:
                with opener.open(url, timeout=1.5):
                    return
            except Exception:
                time.sleep(1.0)

    def wait_for_prometheus_scrape(self, target_epoch: float) -> int:
        """Wait for at least one Prometheus scrape of kueue-metrics at or after target_epoch."""
        self.ensure_prometheus_connection()
        m = re.match(r"^(\d+)", self.scrape_interval)
        max_wait = (int(m.group(1)) if m else 10) + 5
        path = "/api/v1/query?query=timestamp(up%7Bjob%3D%22kueue-metrics%22%7D)"
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

        def _get_last_scrape_ts() -> float:
            try:
                with opener.open(f"http://127.0.0.1:{self.prometheus_port}{path}", timeout=3.0) as r:
                    data = json.loads(r.read().decode("utf-8"))
            except Exception:
                raw = f"/api/v1/namespaces/{self.chamber_namespace}/services/http:prometheus:9090/proxy{path}"
                proc = self.host_kubectl(["get", "--raw", raw], check=False, timeout=10.0)
                if proc.returncode != 0 or not proc.stdout:
                    return 0.0
                data = json.loads(proc.stdout)
            res = data.get("data", {}).get("result", [])
            if res:
                return float(res[0]["value"][1])
            return 0.0

        deadline = time.time() + max_wait
        last_ts = 0.0
        while time.time() < deadline:
            last_ts = _get_last_scrape_ts()
            if last_ts >= target_epoch:
                break
            time.sleep(1.0)
        return int(max(target_epoch, last_ts, time.time()))

    def run_kueue_metrics_report(
        self,
        queueing_strategy: str,
        start_epoch: float,
        end_epoch: float,
        jobset_count: int,
        total_pods: int,
        wait_for_scrape: bool = True,
    ) -> None:
        """Capture test-end cluster state, wait for final Prometheus scrape, and run kueue_metrics_report.py."""
        # Snapshot cluster state IMMEDIATELY at end_epoch before waiting for the final Prometheus scrape
        # so workloads actively running at test end are captured without post-test drift.
        snap_path: Path | None = None
        snap_data = self.capture_cluster_saturation_snapshot(eval_epoch=end_epoch)
        if snap_data is not None:
            try:
                with tempfile.NamedTemporaryFile(
                    mode="w",
                    encoding="utf-8",
                    prefix="kueue-optimizer-sat-snap-",
                    suffix=".json",
                    delete=False,
                ) as tf:
                    json.dump(snap_data, tf)
                    snap_path = Path(tf.name)
            except Exception:
                snap_path = None

        try:
            self.ensure_prometheus_connection()
            if wait_for_scrape:
                print("==> Waiting for final Prometheus scrape of Kueue metrics...", flush=True)
                query_end_epoch = self.wait_for_prometheus_scrape(end_epoch)
            else:
                query_end_epoch = int(max(start_epoch + 1, end_epoch))

            cmd = [
                sys.executable,
                str(SCRIPT_DIR / "kueue_metrics_report.py"),
                "--namespace",
                self.chamber_namespace,
                "--workload-namespace",
                self.workload_namespace,
                "--queueing-strategy",
                queueing_strategy,
                "--prometheus-url",
                f"http://127.0.0.1:{self.prometheus_port}",
                "--start-epoch",
                f"{start_epoch:.3f}",
                "--end-epoch",
                f"{max(start_epoch, end_epoch):.3f}",
                "--query-end-epoch",
                str(int(query_end_epoch)),
                "--jobset-count",
                str(jobset_count),
                "--total-pods",
                str(total_pods),
            ]
            if snap_path is not None:
                cmd.extend(["--cluster-snapshot-json", str(snap_path)])
            if self.kubeconfig_path.is_file():
                cmd.extend(["--kubeconfig", str(self.kubeconfig_path)])

            subprocess.run(cmd, check=False)
        finally:
            if snap_path is not None:
                try:
                    snap_path.unlink(missing_ok=True)
                except Exception:
                    pass

    def check_chamber_health_snapshot(
        self,
        *,
        components: dict[str, str] | None = None,
    ) -> ChamberHealthSnapshot:
        """Query host-cluster Pods in this chamber namespace and summarize readiness and restarts."""
        target_components = dict(CHAMBER_CORE_HEALTH_COMPONENTS if components is None else components)
        try:
            proc = self.host_kubectl(
                ["-n", self.chamber_namespace, "get", "pods", "-o", "json"],
                check=False,
                timeout=15.0,
            )
        except (subprocess.TimeoutExpired, OSError) as exc:
            return ChamberHealthSnapshot(query_ok=False, error=str(exc)[:300])
        if proc.returncode != 0:
            err_msg = (proc.stderr or proc.stdout or f"exit {proc.returncode}").strip()[:300]
            return ChamberHealthSnapshot(query_ok=False, error=err_msg)
        try:
            payload = json.loads(proc.stdout or "{}")
            pod_items = payload.get("items") or []
        except (json.JSONDecodeError, AttributeError) as exc:
            return ChamberHealthSnapshot(query_ok=False, error=f"invalid pods JSON: {exc}")

        counts: dict[str, dict[str, Any]] = {
            dep_name: {"ready": 0, "total": 0, "restarts": 0, "uids": []}
            for dep_name in target_components.values()
        }
        dep_names = tuple(target_components.values())
        for pod in pod_items:
            if not isinstance(pod, dict):
                continue
            meta = pod.get("metadata") or {}
            if meta.get("deletionTimestamp"):
                continue
            st_obj = pod.get("status") or {}
            phase = str(st_obj.get("phase") or "")
            # Ignore terminal Failed/Succeeded Pods (for example, old kubelet-Evicted Pods left behind
            # from earlier runs when reusing a chamber via --skip-chamber-deploy).
            if phase in ("Failed", "Succeeded"):
                continue
            labels = meta.get("labels") or {}
            comp_label = str(labels.get("app.kubernetes.io/component") or "")
            pod_name = str(meta.get("name") or "")
            dep_name = target_components.get(comp_label)
            if dep_name is None:
                for cand_dep in dep_names:
                    if pod_name.startswith(f"{cand_dep}-"):
                        dep_name = cand_dep
                        break
            if dep_name is None:
                continue

            entry = counts[dep_name]
            entry["total"] += 1
            pod_uid = str(meta.get("uid") or pod_name)
            if pod_uid:
                entry["uids"].append(pod_uid)

            conds = st_obj.get("conditions") or []
            is_ready = phase == "Running" and any(
                isinstance(c, dict) and c.get("type") == "Ready" and c.get("status") == "True"
                for c in conds
            )
            if is_ready:
                entry["ready"] += 1

            for cs_key in ("containerStatuses", "initContainerStatuses"):
                for cs in st_obj.get(cs_key) or []:
                    if isinstance(cs, dict):
                        try:
                            entry["restarts"] += max(0, int(cs.get("restartCount") or 0))
                        except (TypeError, ValueError):
                            pass

        comp_statuses = {
            dep_name: ComponentHealthStatus(
                deployment=dep_name,
                ready_pods=int(info["ready"]),
                total_pods=int(info["total"]),
                restart_count=int(info["restarts"]),
                pod_uids=tuple(sorted(info["uids"])),
            )
            for dep_name, info in counts.items()
        }
        return ChamberHealthSnapshot(components=comp_statuses, query_ok=True)

    def cleanup_chamber(self) -> None:
        print(f"==> Cleaning up chamber in host cluster namespace '{self.chamber_namespace}'...", flush=True)
        self.close()
        for svc in ("kwok-kube-apiserver", "prometheus", "kwok-registry"):
            self._pkill_chamber_port_forward(svc)

        get_proc = self.host_kubectl(
            ["get", "namespace", "-o", "json", "--", self.chamber_namespace],
            check=False,
            timeout=30.0,
        )
        if get_proc.returncode != 0:
            err_msg = (get_proc.stderr or "").strip()
            if "notfound" in err_msg.lower() or "not found" in err_msg.lower():
                if self.kubeconfig_path.exists():
                    self.kubeconfig_path.unlink()
                    print(f"    Removed {self.kubeconfig_path}", flush=True)
                print(
                    f"==> Chamber namespace '{self.chamber_namespace}' does not exist; cleanup complete.",
                    flush=True,
                )
                return
            raise RuntimeError(
                f"Failed to inspect namespace '{self.chamber_namespace}' before cleanup "
                f"(exit {get_proc.returncode}): {err_msg}"
            )

        try:
            ns_obj = json.loads(get_proc.stdout or "{}")
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"Failed to parse namespace '{self.chamber_namespace}' JSON during cleanup: {exc}"
            ) from exc

        labels = ((ns_obj.get("metadata") or {}).get("labels")) or {}
        if labels.get(CHAMBER_NAMESPACE_LABEL_KEY) != CHAMBER_NAMESPACE_LABEL_VALUE:
            raise RuntimeError(
                f"Refusing to delete namespace '{self.chamber_namespace}': missing chamber ownership label "
                f"('{CHAMBER_NAMESPACE_LABEL_KEY}={CHAMBER_NAMESPACE_LABEL_VALUE}')."
            )

        del_proc = self.host_kubectl(
            ["delete", "namespace", "--ignore-not-found", "--", self.chamber_namespace],
            check=False,
        )
        if del_proc.returncode != 0:
            err_msg = (del_proc.stderr or "").strip()
            raise RuntimeError(
                f"Failed to delete chamber namespace '{self.chamber_namespace}' "
                f"(exit {del_proc.returncode}): {err_msg}"
            )

        if self.kubeconfig_path.exists():
            self.kubeconfig_path.unlink()
            print(f"    Removed {self.kubeconfig_path}", flush=True)
        print(f"==> Chamber '{self.chamber_namespace}' cleanup complete.", flush=True)


def verify_chamber_control_plane_health(
    mgr: ChamberManager,
    baseline: ChamberHealthSnapshot | None = None,
    *,
    components: dict[str, str] | None = None,
    retries: int = 1,
    retry_delay_sec: float = 0.5,
    stop_check: Callable[[], bool] | None = None,
) -> tuple[bool, list[str], ChamberHealthSnapshot]:
    """Verify chamber control-plane deployments are Ready with no restarts or pod replacements since `baseline`."""
    attempts = 1 + max(0, int(retries))
    snap = ChamberHealthSnapshot(query_ok=False, error="not queried")
    for attempt in range(attempts):
        snap = (
            mgr.check_chamber_health_snapshot(components=components)
            if components is not None
            else mgr.check_chamber_health_snapshot()
        )
        if snap.query_ok:
            break
        if attempt + 1 < attempts:
            if stop_check is not None and stop_check():
                break
            if retry_delay_sec > 0:
                time.sleep(retry_delay_sec)
            if stop_check is not None and stop_check():
                break
    if not snap.query_ok:
        return False, [f"failed to query chamber control-plane pods: {snap.error or 'unknown error'}"], snap

    issues: list[str] = []
    for dep_name in sorted(snap.components.keys()):
        cur = snap.components[dep_name]
        if cur.total_pods <= 0 or cur.ready_pods <= 0 or cur.ready_pods < cur.total_pods:
            issues.append(f"{dep_name} unready ({cur.ready_pods}/{cur.total_pods} pods ready)")
        if baseline is not None and baseline.query_ok and dep_name in baseline.components:
            base_st = baseline.components[dep_name]
            if cur.restart_count > base_st.restart_count:
                issues.append(
                    f"{dep_name} restarted during replay "
                    f"(restartCount {base_st.restart_count} -> {cur.restart_count})"
                )
            if base_st.pod_uids and cur.pod_uids and set(cur.pod_uids) != set(base_st.pod_uids):
                issues.append(f"{dep_name} pod replaced during replay")
    return len(issues) == 0, issues, snap


@dataclass
class ClusterStatusSnapshot:
    """Snapshot of JobSets, Kueue Workloads, ClusterQueues, and KWOK Pods."""

    jobsets_total: int = 0
    jobsets_active: int = 0
    jobsets_completed: int = 0
    jobsets_failed: int = 0
    jobsets_suspended: int = 0
    completed_job_ids: set[str] = field(default_factory=set)

    workloads_admitted: int = 0
    workloads_pending: int = 0
    workloads_finished: int = 0
    workloads_evicted: int = 0
    workloads_query_ok: bool = False
    present_workload_keys: set[tuple[str, str]] = field(default_factory=set)
    finished_workload_keys: set[tuple[str, str]] = field(default_factory=set)
    evicted_workload_keys: set[tuple[str, str]] = field(default_factory=set)

    pods_running: int = 0
    pods_pending: int = 0
    pods_succeeded: int = 0
    pods_failed: int = 0

    cq_summary: list[str] = field(default_factory=list)
    cq_admitted_by_name: dict[str, int] = field(default_factory=dict)
    cq_pending_by_name: dict[str, int] = field(default_factory=dict)


def query_cluster_status(
    mgr: ChamberManager,
    accel_resource: str | None = None,
    *,
    workload_namespaces: list[str] | tuple[str, ...] | set[str] | None = None,
    include_kueue_workloads: bool = False,
) -> ClusterStatusSnapshot:
    """Query current JobSets, Kueue Workloads, ClusterQueues, and Pods from the KWOK cluster via HTTPS."""
    snap = ClusterStatusSnapshot()
    ns_filter = set(workload_namespaces) if workload_namespaces else None

    # 1. ClusterQueues status
    try:
        st_code, cq_raw = mgr.api_request("GET", "/apis/kueue.x-k8s.io/v1beta2/clusterqueues")
        if st_code == 200 and cq_raw:
            cq_data = json.loads(cq_raw.decode("utf-8"))
            for item in cq_data.get("items", []):
                name = item.get("metadata", {}).get("name", "?")
                st = item.get("status", {}) or {}
                adm = int(st.get("admittedWorkloads") or 0)
                pend = int(st.get("pendingWorkloads") or 0)
                snap.workloads_admitted += adm
                snap.workloads_pending += pend
                snap.cq_admitted_by_name[name] = adm
                snap.cq_pending_by_name[name] = pend

                accel_used = ""
                if accel_resource:
                    for flv in st.get("flavorsReservation", []) or []:
                        for res in flv.get("resources", []) or []:
                            if res.get("name") == accel_resource:
                                accel_used = f",{accel_resource.split('/')[-1]}={res.get('total', '0')}"
                snap.cq_summary.append(f"{name}[adm={adm},pend={pend}{accel_used}]")
    except Exception:
        pass

    # 2. JobSets status
    try:
        if ns_filter and (len(ns_filter) > 1 or mgr.workload_namespace not in ns_filter):
            js_path = "/apis/jobset.x-k8s.io/v1alpha2/jobsets"
        else:
            js_path = f"/apis/jobset.x-k8s.io/v1alpha2/namespaces/{mgr.workload_namespace}/jobsets"
        st_code, js_raw = mgr.api_request("GET", js_path)
        if st_code == 200 and js_raw:
            js_data = json.loads(js_raw.decode("utf-8"))
            items = [
                it
                for it in js_data.get("items", [])
                if not ns_filter or (it.get("metadata") or {}).get("namespace", "default") in ns_filter
            ]
            snap.jobsets_total = len(items)
            for item in items:
                meta = item.get("metadata", {}) or {}
                jid = (meta.get("annotations") or {}).get("kueue-optimizer.gke.io/job-id") or (
                    meta.get("labels") or {}
                ).get("kueue-optimizer.gke.io/job-id")
                spec = item.get("spec", {}) or {}
                st = item.get("status", {}) or {}
                conds = {c.get("type"): c.get("status") for c in (st.get("conditions") or [])}
                if conds.get("Completed") == "True":
                    snap.jobsets_completed += 1
                    snap.workloads_finished += 1
                    if jid:
                        snap.completed_job_ids.add(str(jid))
                elif conds.get("Failed") == "True":
                    snap.jobsets_failed += 1
                    snap.workloads_finished += 1
                    if jid:
                        snap.completed_job_ids.add(str(jid))
                elif spec.get("suspend") is True:
                    snap.jobsets_suspended += 1
                else:
                    snap.jobsets_active += 1
    except Exception:
        pass

    # 3. Optional Kueue Workloads status (counts non-JobSet workflows such as Job, Deployment, LWS, Ray, Pod)
    if include_kueue_workloads:
        try:
            st_wl, raw_wl = mgr.api_request("GET", "/apis/kueue.x-k8s.io/v1beta2/workloads")
            if st_wl == 200 and raw_wl:
                snap.workloads_query_ok = True
                items = json.loads(raw_wl.decode("utf-8")).get("items", [])
                adm_cnt = pend_cnt = finished_cnt = evicted_cnt = 0
                scoped_items = [
                    it
                    for it in items
                    if not ns_filter or (it.get("metadata") or {}).get("namespace", "default") in ns_filter
                ]
                js_wl_owners = {
                    (
                        (it.get("metadata") or {}).get("namespace", "default"),
                        str(oref.get("name")),
                    )
                    for it in scoped_items
                    for oref in ((it.get("metadata") or {}).get("ownerReferences") or [])
                    if isinstance(oref, dict) and str(oref.get("kind") or "") == "JobSet" and oref.get("name")
                }
                for it in scoped_items:
                    meta = it.get("metadata") or {}
                    ns = meta.get("namespace", "default")
                    wl_key = (str(ns), str(meta.get("name") or ""))
                    if wl_key[1]:
                        snap.present_workload_keys.add(wl_key)
                    orefs = [o for o in (meta.get("ownerReferences") or []) if isinstance(o, dict)]
                    if js_wl_owners and not any(str(o.get("kind") or "") == "JobSet" for o in orefs):
                        parent_js = (meta.get("labels") or {}).get("jobset.sigs.k8s.io/jobset-name")
                        if parent_js and (ns, str(parent_js)) in js_wl_owners:
                            continue
                    raw_conds = (it.get("status") or {}).get("conditions") or []
                    conds = {c.get("type"): c.get("status") for c in raw_conds}
                    fin_cond = next((c for c in raw_conds if c.get("type") == "Finished"), None)
                    fin_true = bool(fin_cond and fin_cond.get("status") == "True")
                    fin_succeeded = bool(fin_true and str(fin_cond.get("reason") or "") == "Succeeded")
                    is_deactivated = (it.get("spec") or {}).get("active") is False and conds.get("Admitted") != "True"
                    is_group_wl = (
                        str((meta.get("annotations") or {}).get("kueue.x-k8s.io/is-group-workload") or "") == "true"
                    )
                    is_evicted_or_preempted = (
                        conds.get("Evicted") == "True" or conds.get("Preempted") == "True"
                    )
                    if is_evicted_or_preempted and wl_key[1] and not fin_succeeded:
                        snap.evicted_workload_keys.add(wl_key)
                    if fin_true or is_deactivated:
                        finished_cnt += 1
                        if wl_key[1] and (fin_succeeded or is_deactivated or not (is_group_wl and is_evicted_or_preempted)):
                            snap.finished_workload_keys.add(wl_key)
                    elif is_evicted_or_preempted:
                        evicted_cnt += 1
                        pend_cnt += 1
                    elif conds.get("Admitted") == "True" or conds.get("QuotaReserved") == "True":
                        adm_cnt += 1
                    else:
                        pend_cnt += 1
                if scoped_items:
                    snap.workloads_admitted = adm_cnt
                    snap.workloads_pending = pend_cnt
                    snap.workloads_finished = max(snap.workloads_finished, finished_cnt)
                    snap.workloads_evicted = evicted_cnt
        except Exception:
            pass

    # 4. Pods phase summary
    try:
        if ns_filter and (len(ns_filter) > 1 or mgr.workload_namespace not in ns_filter):
            pod_path = "/api/v1/pods"
        else:
            pod_path = f"/api/v1/namespaces/{mgr.workload_namespace}/pods"
        st_code, pod_raw = mgr.api_request("GET", pod_path)
        if st_code == 200 and pod_raw:
            pod_data = json.loads(pod_raw.decode("utf-8"))
            for item in pod_data.get("items", []):
                if ns_filter and (item.get("metadata") or {}).get("namespace", "default") not in ns_filter:
                    continue
                p = (item.get("status", {}) or {}).get("phase", "")
                if p == "Running":
                    snap.pods_running += 1
                elif p == "Pending":
                    snap.pods_pending += 1
                elif p == "Succeeded":
                    snap.pods_succeeded += 1
                elif p == "Failed":
                    snap.pods_failed += 1
    except Exception:
        pass

    return snap
