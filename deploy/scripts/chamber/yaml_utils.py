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

"""Zero-dependency Kubernetes YAML formatting and JSONL scenario file reader."""

from __future__ import annotations

import json
from pathlib import Path
import re
from typing import Any


YAML_SPECIAL_STRINGS: frozenset[str] = frozenset(
    {"true", "false", "yes", "no", "on", "off", "null", "~", ""}
)
YAML_SAFE_UNQUOTED_RE = re.compile(r"^[A-Za-z0-9_./-]+$")
K8S_TOP_KEY_ORDER: tuple[str, ...] = ("apiVersion", "kind", "metadata", "value", "spec", "status")
K8S_META_KEY_ORDER: tuple[str, ...] = ("name", "namespace", "uid", "creationTimestamp", "labels", "annotations")


def _yaml_scalar(val: Any) -> str:
    if val is None:
        return "null"
    if isinstance(val, bool):
        return "true" if val else "false"
    if isinstance(val, (int, float)):
        return str(val)
    if isinstance(val, dict) and not val:
        return "{}"
    if isinstance(val, list) and not val:
        return "[]"
    s = str(val)
    if s == "":
        return '""'
    if s.lower() in YAML_SPECIAL_STRINGS:
        return json.dumps(s)
    try:
        float(s)
        return json.dumps(s)
    except ValueError:
        pass
    if YAML_SAFE_UNQUOTED_RE.match(s) and not s.startswith("-"):
        return s
    return json.dumps(s)


def _ordered_dict_items(d: dict[str, Any], level: int = 0, parent_key: str = "") -> list[tuple[str, Any]]:
    if level == 0:
        priority = {k: i for i, k in enumerate(K8S_TOP_KEY_ORDER)}
        return sorted(d.items(), key=lambda kv: (priority.get(kv[0], 99), kv[0]))
    if parent_key == "metadata":
        priority = {k: i for i, k in enumerate(K8S_META_KEY_ORDER)}
        return sorted(d.items(), key=lambda kv: (priority.get(kv[0], 99), kv[0]))
    return list(d.items())


def to_clean_yaml(obj: Any, indent: int = 0, parent_key: str = "") -> str:
    """Serialize a Python dict/list/scalar into clean, zero-dependency Kubernetes YAML."""
    pad = "  " * indent
    if isinstance(obj, dict):
        if not obj:
            return f"{pad}{{}}" if indent > 0 else "{}"
        lines: list[str] = []
        for k, v in _ordered_dict_items(obj, level=indent, parent_key=parent_key):
            key_str = k if YAML_SAFE_UNQUOTED_RE.match(str(k)) else json.dumps(str(k))
            if isinstance(v, dict):
                if not v:
                    lines.append(f"{pad}{key_str}: {{}}")
                else:
                    lines.append(f"{pad}{key_str}:")
                    lines.append(to_clean_yaml(v, indent + 1, parent_key=str(k)))
            elif isinstance(v, list):
                if not v:
                    lines.append(f"{pad}{key_str}: []")
                else:
                    lines.append(f"{pad}{key_str}:")
                    lines.append(to_clean_yaml(v, indent, parent_key=str(k)))
            else:
                lines.append(f"{pad}{key_str}: {_yaml_scalar(v)}")
        return "\n".join(lines)

    if isinstance(obj, list):
        if not obj:
            return f"{pad}[]" if indent > 0 else "[]"
        lines = []
        for item in obj:
            if isinstance(item, dict):
                if not item:
                    lines.append(f"{pad}- {{}}")
                    continue
                items_kv = _ordered_dict_items(item, level=indent + 1)
                first_k, first_v = items_kv[0]
                fk_str = first_k if YAML_SAFE_UNQUOTED_RE.match(str(first_k)) else json.dumps(str(first_k))
                if isinstance(first_v, dict):
                    if not first_v:
                        lines.append(f"{pad}- {fk_str}: {{}}")
                    else:
                        lines.append(f"{pad}- {fk_str}:")
                        lines.append(to_clean_yaml(first_v, indent + 2, parent_key=str(first_k)))
                elif isinstance(first_v, list):
                    if not first_v:
                        lines.append(f"{pad}- {fk_str}: []")
                    else:
                        lines.append(f"{pad}- {fk_str}:")
                        lines.append(to_clean_yaml(first_v, indent + 1, parent_key=str(first_k)))
                else:
                    lines.append(f"{pad}- {fk_str}: {_yaml_scalar(first_v)}")
                for rk, rv in items_kv[1:]:
                    rk_str = rk if YAML_SAFE_UNQUOTED_RE.match(str(rk)) else json.dumps(str(rk))
                    if isinstance(rv, dict):
                        if not rv:
                            lines.append(f"{pad}  {rk_str}: {{}}")
                        else:
                            lines.append(f"{pad}  {rk_str}:")
                            lines.append(to_clean_yaml(rv, indent + 2, parent_key=str(rk)))
                    elif isinstance(rv, list):
                        if not rv:
                            lines.append(f"{pad}  {rk_str}: []")
                        else:
                            lines.append(f"{pad}  {rk_str}:")
                            lines.append(to_clean_yaml(rv, indent + 1, parent_key=str(rk)))
                    else:
                        lines.append(f"{pad}  {rk_str}: {_yaml_scalar(rv)}")
            elif isinstance(item, list):
                if not item:
                    lines.append(f"{pad}- []")
                else:
                    lines.append(f"{pad}-")
                    lines.append(to_clean_yaml(item, indent + 1))
            else:
                lines.append(f"{pad}- {_yaml_scalar(item)}")
        return "\n".join(lines)

    return f"{pad}{_yaml_scalar(obj)}"


def docs_to_pretty_yaml(docs: list[dict[str, Any]]) -> str:
    """Format documents as readable YAML for --dry-run, README rendering, and error reporting."""
    if not docs:
        return ""
    return "\n---\n".join(to_clean_yaml(doc) for doc in docs) + "\n"


def obj_to_pretty_yaml(obj: Any) -> str:
    """Pretty-print a Kubernetes object (dict, list of dicts, or JSON/YAML string/bytes) as YAML."""
    if obj is None:
        return ""
    if isinstance(obj, bytes):
        obj = obj.decode("utf-8", errors="replace")
    if isinstance(obj, str):
        stripped = obj.strip()
        if not stripped:
            return ""
        try:
            parsed_json = json.loads(stripped)
            if isinstance(parsed_json, dict):
                return docs_to_pretty_yaml([parsed_json])
            if isinstance(parsed_json, list):
                return docs_to_pretty_yaml([d for d in parsed_json if isinstance(d, dict)])
        except Exception:
            pass
        try:
            import yaml  # type: ignore

            loaded = [d for d in yaml.safe_load_all(stripped) if d is not None]
            if loaded:
                return yaml.safe_dump_all(loaded, sort_keys=False)
        except Exception:
            pass
        return stripped + ("\n" if not stripped.endswith("\n") else "")
    if isinstance(obj, dict):
        return docs_to_pretty_yaml([obj])
    if isinstance(obj, list):
        dict_items = [d for d in obj if isinstance(d, dict)]
        if dict_items:
            return docs_to_pretty_yaml(dict_items)
    return str(obj) + "\n"


def describe_k8s_obj_header(obj: Any) -> str:
    """Return a concise 'Kind namespace/name' label for a Kubernetes manifest dict."""
    if not isinstance(obj, dict):
        return "Object"
    kind = str(obj.get("kind") or "Object")
    meta = obj.get("metadata") or {}
    name = str(meta.get("name") or "")
    ns = str(meta.get("namespace") or "")
    if ns and name:
        return f"{kind} {ns}/{name}"
    if name:
        return f"{kind} {name}"
    return kind


def load_jsonl_events(path: Path, *, missing_ok: bool = False) -> list[dict[str, Any]]:
    """Read a JSON Lines (NDJSON) file one line at a time, also supporting legacy JSON arrays."""
    if not path.exists():
        if missing_ok:
            return []
        raise FileNotFoundError(f"Scenario file not found: {path}")
    events: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        checked_first = False
        for line in f:
            stripped = line.strip()
            if not stripped:
                continue
            if not checked_first:
                checked_first = True
                if stripped.startswith("["):
                    f.seek(0)
                    data = json.load(f)
                    if isinstance(data, list):
                        return data
                    if missing_ok:
                        return []
                    raise ValueError(f"Unexpected JSON root in {path}")
            events.append(json.loads(stripped))
    return events
