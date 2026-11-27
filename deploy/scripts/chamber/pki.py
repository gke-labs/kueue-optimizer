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

"""PKI generation, cluster Secret/kwok-pki retrieval, and CA bundle injection."""

from __future__ import annotations

import base64
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile

from chamber.constants import (
    CA_BUNDLE_MANIFEST_FILES,
    CA_BUNDLE_PLACEHOLDER,
    METRICS_TOKEN_CSV,
    PKI_REQUIRED_KEYS,
    PKI_SECRET_NAME,
)


def fetch_pki_from_cluster(chamber_namespace: str, target_pki_dir: Path) -> bool:
    """Attempt to read existing Secret/kwok-pki from chamber_namespace in the host cluster.

    Returns True if Secret/kwok-pki was fetched and written, False ONLY if the secret or
    namespace does not exist in the host cluster (NotFound). Raises RuntimeError on transient
    network/timeout, authentication, RBAC, or malformed-secret errors so callers never silently
    generate a conflicting ad-hoc CA for a running chamber.
    """
    try:
        proc = subprocess.run(
            ["kubectl", "-n", chamber_namespace, "get", "secret", PKI_SECRET_NAME, "-o", "json"],
            capture_output=True,
            text=True,
            check=False,
            timeout=15.0,
        )
    except Exception as exc:
        raise RuntimeError(
            f"Failed to read Secret/{PKI_SECRET_NAME} in namespace '{chamber_namespace}': {exc}"
        ) from exc

    if proc.returncode != 0:
        err_msg = (proc.stderr or "").strip()
        err_lower = err_msg.lower()
        if "invalid_rapt" in err_lower or "gke-gcloud-auth-plugin" in err_lower:
            if proc.stderr:
                sys.stderr.write(proc.stderr)
            raise RuntimeError(
                "Host cluster GKE credentials expired (invalid_rapt / gke-gcloud-auth-plugin). "
                "Please re-authenticate in your terminal:\n"
                "  gcloud auth login && gcloud auth application-default login"
            )
        if "notfound" in err_lower:
            return False
        raise RuntimeError(
            f"Failed to read Secret/{PKI_SECRET_NAME} in namespace '{chamber_namespace}' "
            f"(exit {proc.returncode}): {err_msg or 'unknown kubectl error'}"
        )

    if not proc.stdout:
        raise RuntimeError(
            f"Empty response when reading Secret/{PKI_SECRET_NAME} in namespace '{chamber_namespace}'."
        )

    try:
        secret_obj = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Failed to parse Secret/{PKI_SECRET_NAME} JSON in namespace '{chamber_namespace}': {exc}"
        ) from exc

    if not isinstance(secret_obj, dict):
        raise RuntimeError(
            f"Secret/{PKI_SECRET_NAME} JSON in namespace '{chamber_namespace}' must be an object, "
            f"got {type(secret_obj).__name__}."
        )

    data = secret_obj.get("data")
    if not isinstance(data, dict):
        raise RuntimeError(
            f"Secret/{PKI_SECRET_NAME} in namespace '{chamber_namespace}' has missing or non-object 'data' field."
        )

    missing_keys = [k for k in PKI_REQUIRED_KEYS if k not in data]
    if missing_keys:
        raise RuntimeError(
            f"Secret/{PKI_SECRET_NAME} in namespace '{chamber_namespace}' is missing required keys: "
            f"{', '.join(missing_keys)}"
        )

    decoded_files: dict[str, bytes] = {}
    for key in PKI_REQUIRED_KEYS:
        val = data[key]
        if not isinstance(val, str) or not val.strip():
            raise RuntimeError(
                f"Secret/{PKI_SECRET_NAME} in namespace '{chamber_namespace}' has empty or non-string value "
                f"for required key '{key}'."
            )
        try:
            raw_bytes = base64.b64decode(val, validate=True)
        except Exception as exc:
            raise RuntimeError(
                f"Failed to base64-decode key '{key}' in Secret/{PKI_SECRET_NAME} "
                f"from namespace '{chamber_namespace}': {exc}"
            ) from exc
        if not raw_bytes:
            raise RuntimeError(
                f"Secret/{PKI_SECRET_NAME} in namespace '{chamber_namespace}' decoded to empty bytes "
                f"for required key '{key}'."
            )
        decoded_files[key] = raw_bytes

    if "token.csv" in data and data["token.csv"] is not None:
        token_val = data["token.csv"]
        if not isinstance(token_val, str):
            raise RuntimeError(
                f"Secret/{PKI_SECRET_NAME} in namespace '{chamber_namespace}' has non-string value for key 'token.csv'."
            )
        if token_val:
            try:
                decoded_files["token.csv"] = base64.b64decode(token_val, validate=True)
            except Exception as exc:
                raise RuntimeError(
                    f"Failed to base64-decode key 'token.csv' in Secret/{PKI_SECRET_NAME} "
                    f"from namespace '{chamber_namespace}': {exc}"
                ) from exc
        else:
            decoded_files["token.csv"] = METRICS_TOKEN_CSV.encode("utf-8")
    else:
        decoded_files["token.csv"] = METRICS_TOKEN_CSV.encode("utf-8")

    staged_paths: list[tuple[Path, Path]] = []
    try:
        target_pki_dir.mkdir(parents=True, exist_ok=True)
        for key, raw_bytes in decoded_files.items():
            tmp_path = target_pki_dir / f".{key}.tmp.{os.getpid()}"
            tmp_path.write_bytes(raw_bytes)
            tmp_path.chmod(0o644)
            staged_paths.append((tmp_path, target_pki_dir / key))
        for tmp_path, dst_path in staged_paths:
            os.replace(tmp_path, dst_path)
        return True
    except Exception as exc:
        for tmp_path, _ in staged_paths:
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass
        raise RuntimeError(
            f"Failed to decode or write Secret/{PKI_SECRET_NAME} from namespace '{chamber_namespace}': {exc}"
        ) from exc


def generate_adhoc_pki(chamber_namespace: str, target_pki_dir: Path) -> None:
    """Generate ad-hoc CA & admin keypair, certificates, kubeconfigs, and token.csv via openssl."""
    target_pki_dir.mkdir(parents=True, exist_ok=True)
    apiserver_svc = "kwok-kube-apiserver"
    controller_svc = "kwok-controller"
    kueue_svc = "kwok-kueue"
    jobset_svc = "kwok-jobset"

    with tempfile.TemporaryDirectory(prefix="kueue-optimizer-openssl-") as tmp_str:
        tmp_dir = Path(tmp_str)
        ca_cnf = tmp_dir / "ca.cnf"
        ca_cnf.write_text(
            "[req]\n"
            "distinguished_name = req_distinguished_name\n"
            "x509_extensions = v3_ca\n"
            "[req_distinguished_name]\n"
            "[v3_ca]\n"
            "basicConstraints = critical, CA:TRUE, pathlen:0\n"
            "keyUsage = critical, digitalSignature, keyCertSign, cRLSign\n"
            "subjectKeyIdentifier = hash\n",
            encoding="utf-8",
        )

        ca_key = target_pki_dir / "ca.key"
        ca_crt = target_pki_dir / "ca.crt"
        subprocess.run(
            ["openssl", "genrsa", "-out", str(ca_key), "2048"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        subprocess.run(
            [
                "openssl",
                "req",
                "-x509",
                "-new",
                "-nodes",
                "-key",
                str(ca_key),
                "-subj",
                "/CN=kwok-ca",
                "-days",
                "3650",
                "-config",
                str(ca_cnf),
                "-extensions",
                "v3_ca",
                "-out",
                str(ca_crt),
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        admin_cnf = tmp_dir / "admin.cnf"
        admin_cnf.write_text(
            f"""[req]
req_extensions = v3_req
distinguished_name = req_distinguished_name
[req_distinguished_name]
[v3_req]
basicConstraints = CA:FALSE
keyUsage = critical, nonRepudiation, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = @alt_names
[alt_names]
DNS.1 = kubernetes
DNS.2 = kubernetes.default
DNS.3 = kubernetes.default.svc
DNS.4 = kubernetes.default.svc.cluster.local
DNS.5 = {apiserver_svc}
DNS.6 = {apiserver_svc}.{chamber_namespace}
DNS.7 = {apiserver_svc}.{chamber_namespace}.svc
DNS.8 = {apiserver_svc}.{chamber_namespace}.svc.cluster.local
DNS.9 = {controller_svc}
DNS.10 = {controller_svc}.{chamber_namespace}
DNS.11 = {controller_svc}.{chamber_namespace}.svc
DNS.12 = {controller_svc}.{chamber_namespace}.svc.cluster.local
DNS.13 = {kueue_svc}
DNS.14 = {kueue_svc}.{chamber_namespace}
DNS.15 = {kueue_svc}.{chamber_namespace}.svc
DNS.16 = {kueue_svc}.{chamber_namespace}.svc.cluster.local
DNS.17 = kueue-webhook-service
DNS.18 = kueue-webhook-service.kueue-system
DNS.19 = kueue-webhook-service.kueue-system.svc
DNS.20 = kueue-webhook-service.kueue-system.svc.cluster.local
DNS.21 = kueue-visibility-server
DNS.22 = kueue-visibility-server.kueue-system
DNS.23 = kueue-visibility-server.kueue-system.svc
DNS.24 = kueue-visibility-server.kueue-system.svc.cluster.local
DNS.25 = kueue-controller-manager-metrics-service
DNS.26 = kueue-controller-manager-metrics-service.kueue-system
DNS.27 = kueue-controller-manager-metrics-service.kueue-system.svc
DNS.28 = kueue-controller-manager-metrics-service.kueue-system.svc.cluster.local
DNS.29 = {jobset_svc}
DNS.30 = {jobset_svc}.{chamber_namespace}
DNS.31 = {jobset_svc}.{chamber_namespace}.svc
DNS.32 = {jobset_svc}.{chamber_namespace}.svc.cluster.local
DNS.33 = jobset-webhook-service
DNS.34 = jobset-webhook-service.jobset-system
DNS.35 = jobset-webhook-service.jobset-system.svc
DNS.36 = jobset-webhook-service.jobset-system.svc.cluster.local
DNS.37 = jobset-controller-manager-metrics-service
DNS.38 = jobset-controller-manager-metrics-service.jobset-system
DNS.39 = jobset-controller-manager-metrics-service.jobset-system.svc
DNS.40 = jobset-controller-manager-metrics-service.jobset-system.svc.cluster.local
DNS.41 = localhost
IP.1 = 127.0.0.1
IP.2 = 10.0.0.1
""",
            encoding="utf-8",
        )

        admin_key = target_pki_dir / "admin.key"
        admin_csr = tmp_dir / "admin.csr"
        admin_crt = target_pki_dir / "admin.crt"
        ca_srl = tmp_dir / "ca.srl"

        subprocess.run(
            ["openssl", "genrsa", "-out", str(admin_key), "2048"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        subprocess.run(
            [
                "openssl",
                "req",
                "-new",
                "-key",
                str(admin_key),
                "-subj",
                "/CN=kwok-admin/O=system:masters",
                "-out",
                str(admin_csr),
                "-config",
                str(admin_cnf),
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        subprocess.run(
            [
                "openssl",
                "x509",
                "-req",
                "-in",
                str(admin_csr),
                "-CA",
                str(ca_crt),
                "-CAkey",
                str(ca_key),
                "-CAcreateserial",
                "-CAserial",
                str(ca_srl),
                "-out",
                str(admin_crt),
                "-days",
                "3650",
                "-extensions",
                "v3_req",
                "-extfile",
                str(admin_cnf),
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    ca_b64 = base64.b64encode(ca_crt.read_bytes()).decode("ascii")
    cert_b64 = base64.b64encode(admin_crt.read_bytes()).decode("ascii")
    key_b64 = base64.b64encode(admin_key.read_bytes()).decode("ascii")

    def _build_kc(server_url: str) -> str:
        ctx_name = f"kwok-{chamber_namespace}"
        user_name = f"kwok-{chamber_namespace}-admin"
        return (
            "apiVersion: v1\n"
            "kind: Config\n"
            "clusters:\n"
            "- cluster:\n"
            f"    certificate-authority-data: {ca_b64}\n"
            f"    server: {server_url}\n"
            f"  name: {ctx_name}\n"
            "contexts:\n"
            "- context:\n"
            f"    cluster: {ctx_name}\n"
            f"    user: {user_name}\n"
            f"  name: {ctx_name}\n"
            f"current-context: {ctx_name}\n"
            "preferences: {}\n"
            "users:\n"
            f"- name: {user_name}\n"
            "  user:\n"
            f"    client-certificate-data: {cert_b64}\n"
            f"    client-key-data: {key_b64}\n"
        )

    (target_pki_dir / "kubeconfig").write_text(
        _build_kc(f"https://{apiserver_svc}:6443"),
        encoding="utf-8",
    )
    (target_pki_dir / "external-kubeconfig").write_text(
        _build_kc(f"https://{apiserver_svc}.{chamber_namespace}.svc:6443"),
        encoding="utf-8",
    )
    (target_pki_dir / "token.csv").write_text(METRICS_TOKEN_CSV, encoding="utf-8")

    for fname in (*PKI_REQUIRED_KEYS, "token.csv"):
        (target_pki_dir / fname).chmod(0o644)


def ensure_pki_dir(
    chamber_namespace: str,
    target_pki_dir: Path,
    prefer_cluster: bool = True,
) -> tuple[Path, str]:
    """Ensure PKI files exist in target_pki_dir by picking from cluster or generating ad-hoc."""
    if all((target_pki_dir / k).exists() for k in PKI_REQUIRED_KEYS):
        return target_pki_dir, "cached"
    if prefer_cluster and fetch_pki_from_cluster(chamber_namespace, target_pki_dir):
        return target_pki_dir, "cluster"
    generate_adhoc_pki(chamber_namespace, target_pki_dir)
    return target_pki_dir, "adhoc"


def inject_ca_bundle_into_manifests(manifests_dir: Path, ca_crt_bytes: bytes) -> None:
    """Inject base64-encoded CA certificate into webhook/CRD caBundle fields in manifests_dir."""
    ca_b64 = base64.b64encode(ca_crt_bytes).decode("ascii")
    for fname in CA_BUNDLE_MANIFEST_FILES:
        fpath = manifests_dir / fname
        if not fpath.exists():
            continue
        text = fpath.read_text(encoding="utf-8")
        text = text.replace(CA_BUNDLE_PLACEHOLDER, ca_b64)
        text = re.sub(r"(caBundle:\s*)[A-Za-z0-9+/=_]+", rf"\g<1>{ca_b64}", text)
        text = re.sub(r'("caBundle":\s*")[A-Za-z0-9+/=_]+(")', rf"\g<1>{ca_b64}\g<2>", text)
        fpath.write_text(text, encoding="utf-8")
