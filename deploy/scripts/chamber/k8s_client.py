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

"""KWOK HTTPS/SSA client, port-forward helpers, and kubectl wrappers for ChamberManager."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import http.client
import json
import re
import ssl
import subprocess
import sys
import time
from typing import Any

from chamber.constants import (
    DEFAULT_INFRA_PODS_NAMESPACE,
    DEPLOY_DIR,
    REQUIRED_KWOK_CRDS,
)
from chamber.yaml_utils import describe_k8s_obj_header, obj_to_pretty_yaml


class ChamberK8sClientMixin:
    """Mixin providing host/KWOK kubectl execution, port-forward management, and direct HTTPS SSA calls."""

    def _get_ssl_ctx(self) -> ssl.SSLContext:
        if self._ssl_ctx is None:
            pki_dir = self.ensure_pki(prefer_cluster=True)
            ctx = ssl.create_default_context(cafile=str(pki_dir / "ca.crt"))
            if hasattr(ssl, "VERIFY_X509_STRICT"):
                ctx.verify_flags &= ~ssl.VERIFY_X509_STRICT
            ctx.load_cert_chain(
                certfile=str(pki_dir / "admin.crt"),
                keyfile=str(pki_dir / "admin.key"),
            )
            self._ssl_ctx = ctx
        return self._ssl_ctx

    def _get_https_conn(self) -> http.client.HTTPSConnection:
        conn: http.client.HTTPSConnection | None = getattr(self._tls_local, "conn", None)
        if conn is None:
            conn = http.client.HTTPSConnection(
                "127.0.0.1",
                self.apiserver_port,
                context=self._get_ssl_ctx(),
                timeout=30.0,
            )
            self._tls_local.conn = conn
        return conn

    def _probe_apiserver_ready(self, timeout: float = 2.0) -> bool:
        """Return True if the local port-forwarded KWOK kube-apiserver responds 200 on /readyz."""
        conn: http.client.HTTPSConnection | None = None
        try:
            conn = http.client.HTTPSConnection(
                "127.0.0.1",
                self.apiserver_port,
                context=self._get_ssl_ctx(),
                timeout=timeout,
            )
            conn.request("GET", "/readyz")
            resp = conn.getresponse()
            resp.read()
            return resp.status == 200
        except Exception:
            return False
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    def _pkill_chamber_port_forward(self, service: str, port_mapping: str | None = None) -> None:
        """Terminate stale kubectl port-forward processes strictly matching this chamber's exact namespace."""
        ns = re.escape(self.chamber_namespace)
        svc = re.escape(service)
        pattern = rf"(^|/)kubectl -n {ns} port-forward svc/{svc}"
        if port_mapping:
            pattern += rf" {re.escape(port_mapping)}$"
        else:
            pattern += r"( |$)"
        try:
            subprocess.run(
                ["pkill", "-f", pattern],
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except FileNotFoundError:
            pass

    def _start_or_restart_apiserver_pf_locked(self, timeout_sec: float = 60.0) -> None:
        """Start (or restart) kubectl port-forward to svc/kwok-kube-apiserver. Caller must hold _pf_lock."""
        if self._probe_apiserver_ready(timeout=1.5):
            return

        if self._apiserver_pf_proc is not None:
            try:
                self._apiserver_pf_proc.terminate()
                self._apiserver_pf_proc.wait(timeout=2)
            except Exception:
                try:
                    self._apiserver_pf_proc.kill()
                except Exception:
                    pass
            self._apiserver_pf_proc = None

        self._pkill_chamber_port_forward("kwok-kube-apiserver", f"{self.apiserver_port}:6443")

        deadline = time.monotonic() + timeout_sec
        while time.monotonic() < deadline:
            if self._apiserver_pf_proc is None or self._apiserver_pf_proc.poll() is not None:
                self._apiserver_pf_proc = subprocess.Popen(
                    [
                        "kubectl",
                        "-n",
                        self.chamber_namespace,
                        "port-forward",
                        "svc/kwok-kube-apiserver",
                        f"{self.apiserver_port}:6443",
                    ],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
            for _ in range(10):
                if self._probe_apiserver_ready(timeout=1.5):
                    return
                if self._apiserver_pf_proc.poll() is not None:
                    break
                time.sleep(0.3)
            time.sleep(0.5)

        raise RuntimeError(
            f"Timed out waiting to connect to KWOK kube-apiserver in namespace '{self.chamber_namespace}' "
            f"on localhost:{self.apiserver_port}"
        )

    def api_request(
        self,
        method: str,
        path: str,
        body: bytes | None = None,
        headers: dict[str, str] | None = None,
        content_type: str | None = None,
    ) -> tuple[int, bytes]:
        """Perform a direct keep-alive HTTPS request against the KWOK kube-apiserver with auto-reconnect."""
        hdrs = {"Accept": "application/json", "Connection": "keep-alive"}
        if content_type:
            hdrs["Content-Type"] = content_type
        if headers:
            hdrs.update(headers)
        max_attempts = 6
        for attempt in range(max_attempts):
            conn = self._get_https_conn()
            try:
                conn.request(method, path, body=body, headers=hdrs)
                resp = conn.getresponse()
                data = resp.read()
                if resp.status in (429, 500, 502, 503, 504) and attempt < max_attempts - 1:
                    time.sleep(min(2.0, 0.25 * (attempt + 1)))
                    continue
                return resp.status, data
            except Exception as exc:
                try:
                    conn.close()
                except Exception:
                    pass
                self._tls_local.conn = None
                if attempt == max_attempts - 1:
                    if body:
                        pretty_body = obj_to_pretty_yaml(body).rstrip()
                        raise RuntimeError(
                            f"API request {method} {path} failed: {exc}\n"
                            f"\n--- Object YAML that caused the issue ({method} {path}) ---\n"
                            f"{pretty_body}\n---"
                        ) from exc
                    raise
                with self._pf_lock:
                    try:
                        self._start_or_restart_apiserver_pf_locked(timeout_sec=30.0)
                    except Exception:
                        if attempt == max_attempts - 1:
                            if body:
                                pretty_body = obj_to_pretty_yaml(body).rstrip()
                                raise RuntimeError(
                                    f"API request {method} {path} failed: {exc}\n"
                                    f"\n--- Object YAML that caused the issue ({method} {path}) ---\n"
                                    f"{pretty_body}\n---"
                                ) from exc
                            raise
                time.sleep(min(1.0, 0.15 * (attempt + 1)))
        raise RuntimeError(f"Failed API request: {method} {path}")

    @staticmethod
    def _resource_ssa_path(doc: dict[str, Any], field_manager: str = "kueue-optimizer-runner") -> str:
        api_version = str(doc.get("apiVersion", "v1"))
        kind = str(doc.get("kind", ""))
        meta = doc.get("metadata", {}) or {}
        name = str(meta.get("name", ""))
        ns = meta.get("namespace")

        plural_map = {
            "Node": "nodes",
            "Namespace": "namespaces",
            "Pod": "pods",
            "Job": "jobs",
            "Deployment": "deployments",
            "LeaderWorkerSet": "leaderworkersets",
            "RayJob": "rayjobs",
            "RayCluster": "rayclusters",
            "Topology": "topologies",
            "ResourceFlavor": "resourceflavors",
            "WorkloadPriorityClass": "workloadpriorityclasses",
            "PriorityClass": "priorityclasses",
            "RuntimeClass": "runtimeclasses",
            "Cohort": "cohorts",
            "ClusterQueue": "clusterqueues",
            "LocalQueue": "localqueues",
            "Workload": "workloads",
            "JobSet": "jobsets",
            "Stage": "stages",
            "MutatingWebhookConfiguration": "mutatingwebhookconfigurations",
            "ValidatingWebhookConfiguration": "validatingwebhookconfigurations",
        }
        kind_lower = kind.lower()
        default_plural = f"{kind_lower}es" if kind_lower.endswith("s") else f"{kind_lower}s"
        plural = plural_map.get(kind, default_plural)
        prefix = "/api/v1" if api_version == "v1" else f"/apis/{api_version}"
        cluster_scoped = {
            "Node",
            "Namespace",
            "Topology",
            "ResourceFlavor",
            "WorkloadPriorityClass",
            "PriorityClass",
            "RuntimeClass",
            "Cohort",
            "ClusterQueue",
            "Stage",
            "MutatingWebhookConfiguration",
            "ValidatingWebhookConfiguration",
        }
        if ns and kind not in cluster_scoped:
            return f"{prefix}/namespaces/{ns}/{plural}/{name}?fieldManager={field_manager}&force=true"
        return f"{prefix}/{plural}/{name}?fieldManager={field_manager}&force=true"

    def _fetch_existing_object_for_error(self, path: str) -> dict[str, Any] | list[dict[str, Any]] | None:
        """Best-effort GET of the target resource (or collection) when a create/update/delete fails."""
        clean_path = path.split("?", 1)[0]
        if clean_path.endswith("/status"):
            clean_path = clean_path[: -len("/status")]
        try:
            st, raw = self.api_request("GET", clean_path)
            if 200 <= st < 300 and raw:
                parsed = json.loads(raw.decode("utf-8", errors="replace"))
                if isinstance(parsed, dict) and parsed.get("kind") != "Status":
                    if "items" in parsed and isinstance(parsed.get("items"), list):
                        return [it for it in parsed["items"] if isinstance(it, dict)]
                    return parsed
        except Exception:
            pass
        return None

    def _format_k8s_api_error(
        self,
        operation: str,
        path: str,
        status: int,
        data: bytes,
        obj: Any = None,
    ) -> str:
        """Format a detailed error message including the full API response and pretty-printed YAML of the object."""
        raw_err = data.decode("utf-8", errors="replace").strip()
        lines: list[str] = [f"{operation} {path} failed with HTTP {status}: {raw_err}"]

        try:
            err_json = json.loads(raw_err)
            if isinstance(err_json, dict) and err_json.get("kind") == "Status":
                msg = err_json.get("message")
                reason = err_json.get("reason")
                details = err_json.get("details") or {}
                causes = details.get("causes") or []
                if msg or reason:
                    lines.append(f"  Reason : {reason or 'Unknown'}")
                    lines.append(f"  Message: {msg or ''}")
                for c in causes:
                    if isinstance(c, dict):
                        c_field = c.get("field") or ""
                        c_reason = c.get("reason") or ""
                        c_msg = c.get("message") or ""
                        field_prefix = f"[{c_field}] " if c_field else ""
                        lines.append(f"  Cause  : {field_prefix}{c_reason}: {c_msg}")
        except Exception:
            pass

        server_obj = self._fetch_existing_object_for_error(path)

        if obj is not None:
            hdr = describe_k8s_obj_header(obj)
            yaml_str = obj_to_pretty_yaml(obj).rstrip()
            lines.append(f"\n--- Object YAML that caused the issue ({hdr}) ---")
            lines.append(yaml_str)
            lines.append("---")
            if isinstance(server_obj, dict):
                srv_hdr = describe_k8s_obj_header(server_obj)
                srv_yaml = obj_to_pretty_yaml(server_obj).rstrip()
                lines.append(f"--- Existing Object YAML on API Server ({srv_hdr}) ---")
                lines.append(srv_yaml)
                lines.append("---")
        elif server_obj is not None:
            hdr = describe_k8s_obj_header(server_obj)
            yaml_str = obj_to_pretty_yaml(server_obj).rstrip()
            lines.append(f"\n--- Object YAML that caused the issue ({hdr}) ---")
            lines.append(yaml_str)
            lines.append("---")

        return "\n".join(lines)

    def ssa_apply_doc(self, doc: dict[str, Any]) -> None:
        """Server-Side Apply a single Kubernetes manifest directly over keep-alive HTTPS."""
        path = self._resource_ssa_path(doc)
        payload = json.dumps(doc).encode("utf-8")
        status, data = self.api_request(
            "PATCH",
            path,
            body=payload,
            headers={"Content-Type": "application/apply-patch+yaml"},
        )
        if status < 200 or status >= 300:
            raise RuntimeError(
                self._format_k8s_api_error("SSA PATCH", path, status, data, obj=doc)
            )

    def ssa_apply_node_status(self, node_doc: dict[str, Any]) -> None:
        """Ensure Node .status.allocatable and .status.capacity match node_doc."""
        name = node_doc["metadata"]["name"]
        status_doc = {
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {"name": name},
            "status": {
                "allocatable": node_doc["status"]["allocatable"],
                "capacity": node_doc["status"]["capacity"],
            },
        }
        path = f"/api/v1/nodes/{name}/status?fieldManager=kueue-optimizer-runner&force=true"
        status, data = self.api_request(
            "PATCH",
            path,
            body=json.dumps(status_doc).encode("utf-8"),
            headers={"Content-Type": "application/apply-patch+yaml"},
        )
        if status < 200 or status >= 300:
            raise RuntimeError(
                self._format_k8s_api_error("SSA PATCH", path, status, data, obj=node_doc)
            )

    def api_delete(
        self,
        path: str,
        obj: Any = None,
        ignore_not_found: bool = True,
    ) -> tuple[int, bytes]:
        """Perform a DELETE against the KWOK kube-apiserver and pretty-print the object YAML on failure."""
        status, data = self.api_request("DELETE", path)
        if ignore_not_found and status == 404:
            return status, data
        if status < 200 or status >= 300:
            raise RuntimeError(
                self._format_k8s_api_error("DELETE", path, status, data, obj=obj)
            )
        return status, data

    def _get_executor(self) -> ThreadPoolExecutor:
        if self._executor is None:
            self._executor = ThreadPoolExecutor(max_workers=24)
        return self._executor

    def apply_manifests_parallel(
        self,
        docs: list[dict[str, Any]],
        max_workers: int = 24,
    ) -> None:
        """Apply manifests concurrently over persistent keep-alive TLS sockets (or sequentially when max_workers <= 1)."""
        if not docs:
            return
        if len(docs) == 1 or max_workers <= 1:
            for d in docs:
                self.ssa_apply_doc(d)
            return

        pool = self._get_executor()
        futures = [pool.submit(self.ssa_apply_doc, d) for d in docs]
        for fut in as_completed(futures):
            fut.result()

    def apply_pre_admitted_workload(
        self,
        workload_doc: dict[str, Any],
        status_body: dict[str, Any],
    ) -> None:
        """Create/apply a Kueue Workload CR and immediately patch its `/status` subresource."""
        self.ssa_apply_doc(workload_doc)
        meta = workload_doc.get("metadata") or {}
        ns = str(meta.get("namespace") or "default")
        name = str(meta.get("name") or "")
        api_version = str(workload_doc.get("apiVersion") or "kueue.x-k8s.io/v1beta2")
        status_path = f"/apis/{api_version}/namespaces/{ns}/workloads/{name}/status"
        merge_payload = {"status": status_body}
        st_code, raw = self.api_request(
            "PATCH",
            status_path,
            body=json.dumps(merge_payload).encode("utf-8"),
            headers={"Content-Type": "application/merge-patch+json"},
        )
        if st_code < 200 or st_code >= 300:
            raise RuntimeError(
                self._format_k8s_api_error(
                    "MERGE PATCH",
                    status_path,
                    st_code,
                    raw,
                    obj={**workload_doc, "status": status_body},
                )
            )

    def apply_pre_admitted_workloads_parallel(
        self,
        items: list[tuple[dict[str, Any], dict[str, Any]]],
    ) -> None:
        """Apply multiple pre-admitted Kueue Workloads + `/status` subresources concurrently."""
        if not items:
            return
        if len(items) == 1:
            self.apply_pre_admitted_workload(*items[0])
            return
        pool = self._get_executor()
        futures = [pool.submit(self.apply_pre_admitted_workload, wl_doc, st_doc) for wl_doc, st_doc in items]
        for fut in as_completed(futures):
            fut.result()

    def wait_for_pods_running(
        self,
        pod_keys: list[tuple[str, str]],
        timeout_sec: float = 90.0,
        *,
        require_ready: bool = False,
    ) -> None:
        """Wait until all specified `(namespace, pod_name)` pods reach Running (and Ready when requested) or Succeeded."""
        if not pod_keys:
            return
        target = set(pod_keys)
        deadline = time.monotonic() + timeout_sec
        while time.monotonic() < deadline:
            st, raw = self.api_request("GET", "/api/v1/pods")
            if st == 200 and raw:
                ready_keys: set[tuple[str, str]] = set()
                for it in json.loads(raw.decode("utf-8")).get("items", []):
                    meta = it.get("metadata") or {}
                    p_key = (str(meta.get("namespace") or "default"), str(meta.get("name") or ""))
                    if p_key not in target:
                        continue
                    pod_status = it.get("status") or {}
                    phase = str(pod_status.get("phase") or "")
                    if phase == "Succeeded":
                        ready_keys.add(p_key)
                    elif phase == "Running":
                        if not require_ready or any(
                            isinstance(c, dict) and c.get("type") == "Ready" and c.get("status") == "True"
                            for c in (pod_status.get("conditions") or [])
                        ):
                            ready_keys.add(p_key)
                if target.issubset(ready_keys):
                    return
            time.sleep(0.25)
        state_desc = "Running and Ready" if require_ready else "Running phase"
        raise RuntimeError(
            f"Timed out waiting for {len(target)} initial pod(s) to reach {state_desc} on KWOK nodes"
        )

    @staticmethod
    def _run_kubectl(
        cmd: list[str],
        input_text: str | None = None,
        check: bool = True,
        timeout: float = 300.0,
        stream_output: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        proc = subprocess.run(
            cmd,
            input=input_text,
            capture_output=not stream_output,
            text=True,
            check=False,
            timeout=timeout,
        )
        if proc.returncode != 0:
            is_mutating = any(
                verb in cmd for verb in ("apply", "create", "patch", "replace", "delete")
            )
            if check or is_mutating:
                if not stream_output:
                    if proc.stdout:
                        sys.stderr.write(proc.stdout)
                    if proc.stderr:
                        sys.stderr.write(proc.stderr)
                    sys.stderr.flush()
                    err_lower = (proc.stderr or "").lower()
                    if "invalid_rapt" in err_lower or "gke-gcloud-auth-plugin" in err_lower:
                        print(
                            "\n==> ERROR: Host cluster GKE credentials expired (invalid_rapt / gke-gcloud-auth-plugin).\n"
                            "    Please re-authenticate in your terminal:\n"
                            "      gcloud auth login && gcloud auth application-default login",
                            file=sys.stderr,
                            flush=True,
                        )
                if input_text and is_mutating:
                    pretty_in = obj_to_pretty_yaml(input_text).rstrip()
                    print(
                        f"\n--- Object YAML that caused the issue ({' '.join(cmd)}) ---\n"
                        f"{pretty_in}\n---",
                        file=sys.stderr,
                        flush=True,
                    )
            if check:
                raise subprocess.CalledProcessError(proc.returncode, cmd, output=proc.stdout, stderr=proc.stderr)
        return proc

    def kwok_kubectl(
        self,
        args: list[str],
        input_text: str | None = None,
        check: bool = True,
        timeout: float = 120.0,
        stream_output: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        cmd = ["kubectl", f"--kubeconfig={self.kubeconfig_path}", *args]
        return self._run_kubectl(
            cmd,
            input_text=input_text,
            check=check,
            timeout=timeout,
            stream_output=stream_output,
        )

    def host_kubectl(
        self,
        args: list[str],
        input_text: str | None = None,
        check: bool = True,
        timeout: float = 300.0,
        stream_output: bool = False,
    ) -> subprocess.CompletedProcess[str]:
        cmd = ["kubectl", *args]
        return self._run_kubectl(
            cmd,
            input_text=input_text,
            check=check,
            timeout=timeout,
            stream_output=stream_output,
        )

    def wait_for_crds(
        self,
        required_crds: tuple[str, ...] | set[str] = REQUIRED_KWOK_CRDS,
        timeout_sec: float = 120.0,
    ) -> None:
        req_set = set(required_crds)
        deadline = time.monotonic() + timeout_sec
        while time.monotonic() < deadline:
            st, data = self.api_request("GET", "/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
            if st == 200 and data:
                items = json.loads(data.decode("utf-8")).get("items", [])
                established = set()
                for item in items:
                    name = item.get("metadata", {}).get("name", "")
                    conds = {
                        c.get("type"): c.get("status")
                        for c in (item.get("status", {}) or {}).get("conditions", []) or []
                    }
                    if conds.get("Established") == "True":
                        established.add(name)
                if req_set.issubset(established):
                    return
            time.sleep(1.0)
        raise RuntimeError(f"Timed out waiting for CRDs to be Established: {sorted(req_set)}")

    def apply_kwok_stages(self) -> None:
        stages_yaml = (DEPLOY_DIR / "base" / "manifests" / "kwok-stages.yaml").read_text(encoding="utf-8")
        self.kwok_kubectl(
            ["apply", "--server-side", "--force-conflicts", "--validate=false", "-f", "-"],
            input_text=stages_yaml,
        )

    def ensure_kueue_webhook_namespace_exclusions(
        self,
        excluded_namespaces: tuple[str, ...] | list[str] = (DEFAULT_INFRA_PODS_NAMESPACE,),
    ) -> None:
        """Ensure Kueue mutating/validating webhooks explicitly exclude excluded_namespaces and kueue.x-k8s.io/exclude=true."""
        targets = (
            ("MutatingWebhookConfiguration", "/apis/admissionregistration.k8s.io/v1/mutatingwebhookconfigurations/kueue-mutating-webhook-configuration"),
            ("ValidatingWebhookConfiguration", "/apis/admissionregistration.k8s.io/v1/validatingwebhookconfigurations/kueue-validating-webhook-configuration"),
        )
        for kind, get_path in targets:
            st, raw = self.api_request("GET", get_path)
            if st != 200 or not raw:
                continue
            obj = json.loads(raw.decode("utf-8", errors="replace"))
            changed = False
            for wh in obj.get("webhooks") or []:
                ns_sel = wh.get("namespaceSelector")
                if not isinstance(ns_sel, dict):
                    continue
                exprs = ns_sel.setdefault("matchExpressions", [])
                name_expr = None
                excl_label_expr = None
                for ex in exprs:
                    if ex.get("key") == "kubernetes.io/metadata.name" and ex.get("operator") == "NotIn":
                        name_expr = ex
                    elif ex.get("key") == "kueue.x-k8s.io/exclude" and ex.get("operator") == "NotIn":
                        excl_label_expr = ex
                if name_expr is not None:
                    vals = list(name_expr.get("values") or [])
                    for ns_name in excluded_namespaces:
                        if ns_name and ns_name not in vals:
                            vals.append(ns_name)
                            changed = True
                    name_expr["values"] = vals
                if excl_label_expr is None:
                    exprs.append(
                        {
                            "key": "kueue.x-k8s.io/exclude",
                            "operator": "NotIn",
                            "values": ["true"],
                        }
                    )
                    changed = True
                if kind == "MutatingWebhookConfiguration" and wh.get("name") == "mpod.kb.io":
                    obj_sel = wh.setdefault("objectSelector", {})
                    obj_exprs = obj_sel.setdefault("matchExpressions", [])
                    if not any(
                        ex.get("key") == "kueue-optimizer.gke.io/initial-pod" and ex.get("operator") == "NotIn"
                        for ex in obj_exprs
                    ):
                        obj_exprs.append(
                            {
                                "key": "kueue-optimizer.gke.io/initial-pod",
                                "operator": "NotIn",
                                "values": ["true"],
                            }
                        )
                        changed = True
            if changed:
                patch_doc = {
                    "apiVersion": "admissionregistration.k8s.io/v1",
                    "kind": kind,
                    "metadata": {"name": obj["metadata"]["name"]},
                    "webhooks": obj.get("webhooks") or [],
                }
                self.ssa_apply_doc(patch_doc)

    def clear_workload_namespace(self, delete_sample_cq: bool = True) -> None:
        self.api_delete(f"/apis/jobset.x-k8s.io/v1alpha2/namespaces/{self.workload_namespace}/jobsets")
        self.api_delete(f"/apis/kueue.x-k8s.io/v1beta2/namespaces/{self.workload_namespace}/workloads")
        self.api_delete(f"/api/v1/namespaces/{self.workload_namespace}/pods")
        if delete_sample_cq:
            self.api_delete("/apis/kueue.x-k8s.io/v1beta2/clusterqueues/cluster-queue")

    def sync_kwok_nodes(
        self,
        nodes: list[dict[str, Any]],
        remove_stale: bool = True,
        timeout_sec: float = 180.0,
    ) -> None:
        """Apply simulated KWOK nodes concurrently, remove stale nodes, and wait until healthy nodes are Ready."""
        nodes_by_name: dict[str, dict[str, Any]] = {}
        unhealthy_t0_names: set[str] = set()
        prepared_nodes: list[dict[str, Any]] = []
        for raw_nd in nodes:
            nd = json.loads(json.dumps(raw_nd))
            nname = nd["metadata"]["name"]
            ann = (nd.get("metadata") or {}).get("annotations") or {}
            if str(ann.get("kueue-optimizer.gke.io/node-failed") or "").lower() == "true":
                unhealthy_t0_names.add(nname)
                spec_blk = nd.setdefault("spec", {})
                taints = list(spec_blk.get("taints") or [])
                if not any(isinstance(t, dict) and t.get("key") == "node.kubernetes.io/not-ready" for t in taints):
                    taints.append({"key": "node.kubernetes.io/not-ready", "effect": "NoSchedule"})
                spec_blk["taints"] = taints
            nodes_by_name[nname] = nd
            prepared_nodes.append(nd)

        healthy_t0_names = set(nodes_by_name.keys()) - unhealthy_t0_names

        t_start = time.monotonic()
        self.apply_manifests_parallel(prepared_nodes, max_workers=24)
        elapsed = time.monotonic() - t_start
        if len(prepared_nodes) > 20:
            print(
                f"    Applied {len(prepared_nodes)} KWOK nodes in {elapsed:.1f}s "
                f"({len(healthy_t0_names)} healthy, {len(unhealthy_t0_names)} NotReady at t0). Waiting for nodes...",
                flush=True,
            )

        # Immediately stamp Ready=False + full allocatable/capacity onto t0 NotReady nodes (skipped by node-initialize).
        if unhealthy_t0_names:
            now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            for u_name in sorted(unhealthy_t0_names):
                u_doc = nodes_by_name[u_name]
                u_st = u_doc.get("status") or {}
                status_patch = {
                    "status": {
                        "allocatable": u_st.get("allocatable") or {},
                        "capacity": u_st.get("capacity") or u_st.get("allocatable") or {},
                        "nodeInfo": u_st.get("nodeInfo") or {},
                        "phase": "Running",
                        "conditions": [
                            {
                                "type": "Ready",
                                "status": "False",
                                "reason": "KubeletNotReady",
                                "message": "Node was NotReady at scenario start (t0)",
                                "lastHeartbeatTime": now_iso,
                                "lastTransitionTime": now_iso,
                            }
                        ],
                    }
                }
                self.api_request(
                    "PATCH",
                    f"/api/v1/nodes/{u_name}/status",
                    body=json.dumps(status_patch).encode("utf-8"),
                    headers={"Content-Type": "application/merge-patch+json"},
                )

        stale_deleted = not remove_stale
        deadline = time.monotonic() + timeout_sec
        nodes_needing_status_patch: list[dict[str, Any]] = []
        while time.monotonic() < deadline:
            st, raw = self.api_request("GET", "/api/v1/nodes")
            if st == 200 and raw:
                items = json.loads(raw.decode("utf-8")).get("items", [])
                if not stale_deleted:
                    stale_items = [
                        it
                        for it in items
                        if (it.get("metadata", {}).get("labels") or {}).get("type") == "kwok"
                        and it["metadata"]["name"] not in nodes_by_name
                    ]
                    for s_it in stale_items:
                        sname = s_it["metadata"]["name"]
                        self.api_delete(f"/api/v1/nodes/{sname}", obj=s_it)
                    stale_deleted = True

                ready_healthy_count = 0
                present_unhealthy_count = 0
                nodes_needing_status_patch.clear()
                for it in items:
                    nname = it.get("metadata", {}).get("name", "")
                    if nname not in nodes_by_name:
                        continue
                    st_obj = it.get("status", {}) or {}
                    conds = {c.get("type"): c.get("status") for c in (st_obj.get("conditions") or [])}
                    if nname in unhealthy_t0_names:
                        present_unhealthy_count += 1
                        alloc = st_obj.get("allocatable") or {}
                        expected_alloc = nodes_by_name[nname]["status"]["allocatable"]
                        if any(str(alloc.get(k)) != str(v) for k, v in expected_alloc.items()):
                            nodes_needing_status_patch.append(nodes_by_name[nname])
                    elif conds.get("Ready") == "True":
                        ready_healthy_count += 1
                        alloc = st_obj.get("allocatable") or {}
                        expected_alloc = nodes_by_name[nname]["status"]["allocatable"]
                        if any(str(alloc.get(k)) != str(v) for k, v in expected_alloc.items()):
                            nodes_needing_status_patch.append(nodes_by_name[nname])

                if ready_healthy_count >= len(healthy_t0_names) and present_unhealthy_count >= len(unhealthy_t0_names):
                    break
            time.sleep(0.5)
        else:
            raise RuntimeError(
                f"Timed out waiting for {len(healthy_t0_names)} healthy KWOK node(s) to become Ready "
                f"(and {len(unhealthy_t0_names)} t0-NotReady node(s) to sync)"
            )

        if nodes_needing_status_patch:
            pool = self._get_executor()
            futs = [pool.submit(self.ssa_apply_node_status, nd) for nd in nodes_needing_status_patch]
            for fut in as_completed(futs):
                fut.result()

    def apply_and_wait_queues(
        self,
        queue_docs: list[dict[str, Any]],
        timeout_sec: float = 60.0,
    ) -> None:
        """Apply prerequisite resources and queues, then wait until all ClusterQueues & LocalQueues are Active."""
        cq_names = {d["metadata"]["name"] for d in queue_docs if d.get("kind") == "ClusterQueue"}
        lq_names = {d["metadata"]["name"] for d in queue_docs if d.get("kind") == "LocalQueue"}

        prereq_docs = [d for d in queue_docs if d.get("kind") not in ("ClusterQueue", "LocalQueue")]
        cq_lq_docs = [d for d in queue_docs if d.get("kind") in ("ClusterQueue", "LocalQueue")]
        self.apply_manifests_parallel(prereq_docs, max_workers=16)
        self.apply_manifests_parallel(cq_lq_docs, max_workers=16)

        deadline = time.monotonic() + timeout_sec
        while time.monotonic() < deadline:
            st_cq, raw_cq = self.api_request("GET", "/apis/kueue.x-k8s.io/v1beta2/clusterqueues")
            st_lq, raw_lq = self.api_request(
                "GET",
                f"/apis/kueue.x-k8s.io/v1beta2/namespaces/{self.workload_namespace}/localqueues",
            )
            if st_cq == 200 and st_lq == 200 and raw_cq and raw_lq:
                active_cqs = {
                    it["metadata"]["name"]
                    for it in json.loads(raw_cq.decode("utf-8")).get("items", [])
                    if any(
                        c.get("type") == "Active" and c.get("status") == "True"
                        for c in (it.get("status", {}) or {}).get("conditions") or []
                    )
                }
                active_lqs = {
                    it["metadata"]["name"]
                    for it in json.loads(raw_lq.decode("utf-8")).get("items", [])
                    if any(
                        c.get("type") == "Active" and c.get("status") == "True"
                        for c in (it.get("status", {}) or {}).get("conditions") or []
                    )
                }
                if cq_names.issubset(active_cqs) and lq_names.issubset(active_lqs):
                    return
            time.sleep(0.5)
        raise RuntimeError("Timed out waiting for ClusterQueues and LocalQueues to become Active")

    def capture_cluster_saturation_snapshot(self, eval_epoch: float | None = None) -> dict[str, Any] | None:
        """Capture exact Node capacities, Kueue Workloads, and Pod phases at test end (before scrape wait)."""
        try:
            self.ensure_kwok_connection()
            snapshot: dict[str, Any] = {
                "capturedAtEpoch": float(eval_epoch if eval_epoch is not None else time.time()),
                "nodes": [],
                "workloads": [],
                "pods": [],
            }
            st_n, raw_n = self.api_request("GET", "/api/v1/nodes")
            if st_n == 200 and raw_n:
                snapshot["nodes"] = (json.loads(raw_n.decode("utf-8")) or {}).get("items") or []

            st_w, raw_w = self.api_request("GET", "/apis/kueue.x-k8s.io/v1beta2/workloads")
            if st_w != 200 or not raw_w:
                st_w, raw_w = self.api_request("GET", "/apis/kueue.x-k8s.io/v1beta1/workloads")
            if st_w == 200 and raw_w:
                snapshot["workloads"] = (json.loads(raw_w.decode("utf-8")) or {}).get("items") or []

            retired_wls: dict[tuple[str, str], dict[str, Any]] = getattr(self, "_retired_serving_workloads", None) or {}
            if retired_wls:
                merged_wls: list[dict[str, Any]] = []
                replaced_keys: set[tuple[str, str]] = set()
                for w in snapshot["workloads"]:
                    if not isinstance(w, dict):
                        continue
                    wk = (
                        str(((w.get("metadata") or {}).get("namespace")) or "default"),
                        str(((w.get("metadata") or {}).get("name")) or ""),
                    )
                    if wk in retired_wls:
                        w_conds = ((w.get("status") or {}).get("conditions")) or []
                        is_pre_gc_owner_gone = any(
                            isinstance(c, dict)
                            and c.get("type") == "Finished"
                            and c.get("status") == "True"
                            and c.get("reason") == "OwnerNotFound"
                            for c in w_conds
                        )
                        if is_pre_gc_owner_gone:
                            merged_wls.append(retired_wls[wk])
                            replaced_keys.add(wk)
                            continue
                        replaced_keys.add(wk)
                    merged_wls.append(w)
                for rw_key in sorted(retired_wls.keys()):
                    if rw_key not in replaced_keys:
                        merged_wls.append(retired_wls[rw_key])
                snapshot["workloads"] = merged_wls

            st_p, raw_p = self.api_request("GET", "/api/v1/pods")
            if st_p == 200 and raw_p:
                snapshot["pods"] = (json.loads(raw_p.decode("utf-8")) or {}).get("items") or []

            node_timeline = getattr(self, "_node_capacity_timeline", None) or []
            if len(node_timeline) > 1:
                snapshot["nodeCapacityTimeline"] = [dict(entry) for entry in node_timeline]

            return snapshot
        except Exception:
            return None
