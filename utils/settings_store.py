"""
Persistent settings store for integrations, policy packs, and RBAC.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

try:
    from google.cloud import storage  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    storage = None


INTEGRATION_KEYS = {"github", "jira", "linear"}
POLICY_PACKS = {"standard", "strict", "regulated"}
RBAC_ROLES = {"executive", "delivery", "engineering", "security"}
RBAC_PERMISSIONS = {
    "view_executive_dashboard",
    "run_pipeline",
    "approve_gates",
    "manage_integrations",
    "manage_policies",
    "manage_rbac",
    "view_engineering_logs",
}
USER_STATUSES = {"active", "inactive"}
INTEGRATION_SECRET_KEYS = {
    "github": "token",
    "jira": "api_token",
    "linear": "api_token",
}
LLM_PROVIDER_KEYS = {"anthropic", "openai"}
LLM_DEFAULT_MODELS = {
    "anthropic": "claude-sonnet-4-20250514",
    "openai": "gpt-4o",
}
KNOWLEDGE_SOURCE_TYPES = {"file", "wiki", "repo", "standards", "issues", "other"}
KNOWLEDGE_SCOPES = {"global", "workspace", "client", "project"}
DATA_CLASSIFICATIONS = {"public", "internal", "confidential", "regulated"}
KNOWLEDGE_SET_STATES = {"draft", "published", "deprecated"}
SPECIALIST_TOOL_MODES = {"read_only", "read_write"}
SPECIALIST_DEPTH_TIERS = {"shallow", "standard", "deep"}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_json_load(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        return default


def _safe_json_write(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=True))
    tmp.replace(path)


def _stable_json(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str)
    except Exception:
        return str(value)


def _digest(value: Any) -> str:
    return hashlib.sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _mask_secret(secret: str) -> str:
    raw = str(secret or "").strip()
    if not raw:
        return ""
    if len(raw) <= 4:
        return "*" * len(raw)
    return ("*" * max(4, len(raw) - 4)) + raw[-4:]


def _normalize_llm_secret(provider: str, secret: str) -> str:
    raw = str(secret or "").strip()
    if not raw:
        return ""
    value = raw.strip().strip('"').strip("'").strip()
    if value.lower().startswith("bearer "):
        value = value[7:].strip()

    key = str(provider or "").strip().lower()
    env_tokens = [
        f"{key.upper()}_API_KEY",
        "OPENAI_API_KEY",
        "ANTHROPIC_API_KEY",
    ]
    for token in env_tokens:
        # Handles forms like:
        #   OPENAI_API_KEY=sk-...
        #   export OPENAI_API_KEY="sk-..."
        match = re.match(rf"^(?:export\s+)?{re.escape(token)}\s*[:=]\s*(.+)$", value, flags=re.IGNORECASE)
        if match:
            value = str(match.group(1) or "").strip().strip('"').strip("'").strip()
            break

    # If user pasted a whole command/snippet, extract the key-like token.
    if key == "openai":
        m = re.search(r"(sk-proj-[A-Za-z0-9_\-]{16,}|sk-[A-Za-z0-9_\-]{16,})", value)
        if m:
            return m.group(1).strip()
    if key == "anthropic":
        m = re.search(r"(sk-ant-[A-Za-z0-9_\-]{16,})", value)
        if m:
            return m.group(1).strip()

    return value.strip()


class _SettingsStateBackend:
    """
    Persist settings state either locally or in GCS.

    Environment variables:
      - SYNTHETIX_SETTINGS_BACKEND: auto|local|gcs (default: auto)
      - SYNTHETIX_SETTINGS_GCS_BUCKET: target bucket (optional)
      - SYNTHETIX_SETTINGS_GCS_PREFIX: object prefix (default: settings)

    auto mode:
      - If a GCS bucket is configured for run store (`RUN_STORE_GCS_BUCKET`) and
        google-cloud-storage is available, settings state is persisted in GCS.
      - Otherwise falls back to local file.
    """

    def __init__(self, local_path: Path):
        self.local_path = local_path
        mode_raw = str(os.getenv("SYNTHETIX_SETTINGS_BACKEND", "auto") or "auto").strip().lower()
        if mode_raw not in {"auto", "local", "gcs"}:
            mode_raw = "auto"
        self.mode = mode_raw
        self.gcs_bucket = str(
            os.getenv("SYNTHETIX_SETTINGS_GCS_BUCKET", "")
            or os.getenv("RUN_STORE_GCS_BUCKET", "")
            or os.getenv("SYNTHETIX_RUN_STORE_BUCKET", "")
        ).strip()
        self.gcs_prefix = str(os.getenv("SYNTHETIX_SETTINGS_GCS_PREFIX", "settings") or "settings").strip("/") or "settings"
        self.gcs_blob = f"{self.gcs_prefix}/settings_state.json"
        self._gcs = None

        if self._use_gcs():
            try:
                self._gcs = storage.Client()
            except Exception:
                self._gcs = None

    def _use_gcs(self) -> bool:
        if self.mode == "local":
            return False
        if self.mode == "gcs":
            return bool(storage is not None and self.gcs_bucket)
        # auto mode
        return bool(storage is not None and self.gcs_bucket)

    @property
    def backend_label(self) -> str:
        if self._use_gcs() and self._gcs is not None:
            return "gcs"
        return "local_file"

    def _read_gcs_json(self) -> dict[str, Any] | None:
        if not self._use_gcs() or self._gcs is None:
            return None
        try:
            bucket = self._gcs.bucket(self.gcs_bucket)
            blob = bucket.blob(self.gcs_blob)
            if not blob.exists():
                return None
            txt = blob.download_as_text()
            data = json.loads(txt)
            if isinstance(data, dict):
                return data
        except Exception:
            return None
        return None

    def _write_gcs_json(self, payload: dict[str, Any]) -> bool:
        if not self._use_gcs() or self._gcs is None:
            return False
        try:
            bucket = self._gcs.bucket(self.gcs_bucket)
            blob = bucket.blob(self.gcs_blob)
            blob.cache_control = "no-store"
            blob.upload_from_string(
                json.dumps(payload, indent=2, ensure_ascii=True),
                content_type="application/json; charset=utf-8",
            )
            return True
        except Exception:
            return False

    def load(self, default_payload: dict[str, Any]) -> dict[str, Any]:
        if self._use_gcs():
            from_gcs = self._read_gcs_json()
            if isinstance(from_gcs, dict):
                return from_gcs
            # One-time migration path: seed GCS from existing local settings if present.
            local = _safe_json_load(self.local_path, default_payload)
            if isinstance(local, dict):
                if self._write_gcs_json(local):
                    return local
                return local
            return copy.deepcopy(default_payload)
        data = _safe_json_load(self.local_path, default_payload)
        if isinstance(data, dict):
            return data
        return copy.deepcopy(default_payload)

    def save(self, payload: dict[str, Any]) -> None:
        if self._use_gcs() and self._write_gcs_json(payload):
            return
        _safe_json_write(self.local_path, payload)


class _SecretBackend:
    """
    Optional backend for persisting secrets outside local filesystem state.

    Enabled via environment variables:
      - SYNTHETIX_SECRET_BACKEND=gcp_secret_manager
      - GOOGLE_CLOUD_PROJECT=<project-id> (or GCP_PROJECT)
      - SYNTHETIX_SECRET_PREFIX=synthetix (optional)
    """

    def __init__(self) -> None:
        mode = str(os.getenv("SYNTHETIX_SECRET_BACKEND", "local") or "local").strip().lower()
        self.mode = mode
        self.project = (
            str(os.getenv("GOOGLE_CLOUD_PROJECT", "")).strip()
            or str(os.getenv("GCP_PROJECT", "")).strip()
        )
        self.prefix = str(os.getenv("SYNTHETIX_SECRET_PREFIX", "synthetix")).strip() or "synthetix"
        self._cleared_sentinel = "__SYNTHETIX_EMPTY__"
        self._client: Any = None
        self._enabled = False

        if mode in {"gcp_secret_manager", "gcp-secret-manager", "secret_manager", "secret-manager"}:
            try:
                from google.cloud import secretmanager  # type: ignore

                if self.project:
                    self._client = secretmanager.SecretManagerServiceClient()
                    self._enabled = True
            except Exception:
                self._client = None
                self._enabled = False

    @property
    def enabled(self) -> bool:
        return bool(self._enabled and self._client and self.project)

    @property
    def backend_label(self) -> str:
        if self.enabled:
            return "gcp_secret_manager"
        return "local_file"

    def _secret_id(self, slot: str) -> str:
        raw = f"{self.prefix}-{slot}".lower()
        safe = "".join(ch if (ch.isalnum() or ch in {"-", "_"}) else "-" for ch in raw).strip("-_")
        return safe[:255] or "synthetix-secret"

    def _secret_name(self, slot: str) -> str:
        return f"projects/{self.project}/secrets/{self._secret_id(slot)}"

    def _ensure_secret(self, slot: str) -> str:
        if not self.enabled:
            return ""
        name = self._secret_name(slot)
        parent = f"projects/{self.project}"
        secret_id = self._secret_id(slot)
        try:
            self._client.create_secret(
                request={
                    "parent": parent,
                    "secret_id": secret_id,
                    "secret": {"replication": {"automatic": {}}},
                }
            )
        except Exception:
            # Already exists or transient creation race; read path still works.
            pass
        return name

    def write(self, slot: str, secret: str) -> str:
        value = str(secret or "").strip()
        if not value:
            return ""
        if not self.enabled:
            return value
        name = self._ensure_secret(slot)
        if not name:
            return value
        try:
            self._client.add_secret_version(
                request={
                    "parent": name,
                    "payload": {"data": value.encode("utf-8")},
                }
            )
            return ""
        except Exception:
            # Fallback to local persistence if secret manager write fails.
            return value

    def read(self, slot: str, fallback: str = "") -> str:
        local = str(fallback or "").strip()
        if local:
            return local
        if not self.enabled:
            return local
        try:
            response = self._client.access_secret_version(
                request={"name": f"{self._secret_name(slot)}/versions/latest"}
            )
            data = response.payload.data.decode("utf-8") if response and response.payload else ""
            value = str(data or "").strip()
            if value == self._cleared_sentinel:
                return ""
            return value
        except Exception:
            return local

    def clear(self, slot: str) -> None:
        if not self.enabled:
            return
        name = self._ensure_secret(slot)
        if not name:
            return
        try:
            # Secret Manager may reject empty payloads; use a sentinel and map it back to empty on reads.
            self._client.add_secret_version(
                request={
                    "parent": name,
                    "payload": {"data": self._cleared_sentinel.encode("utf-8")},
                }
            )
        except Exception:
            pass


def _parse_github_repo_reference(value: str) -> tuple[str, str]:
    raw = str(value or "").strip()
    if not raw:
        return "", ""
    if raw.startswith("git@") and ":" in raw:
        path = raw.split(":", 1)[1]
    else:
        parsed = urlparse(raw)
        path = parsed.path if parsed.scheme and parsed.netloc else raw
    segments = [seg for seg in path.split("/") if seg]
    if len(segments) < 2:
        return "", ""
    owner = segments[0].strip()
    repo = segments[1].strip()
    if repo.endswith(".git"):
        repo = repo[:-4]
    return owner, repo


def _default_role_permissions() -> dict[str, list[str]]:
    return {
        "executive": ["view_executive_dashboard"],
        "delivery": ["view_executive_dashboard", "run_pipeline", "approve_gates"],
        "engineering": ["run_pipeline", "view_engineering_logs"],
        "security": ["view_executive_dashboard", "approve_gates", "view_engineering_logs"],
    }


def _default_settings() -> dict[str, Any]:
    now = _utc_now()
    return {
        "version": 1,
        "updated_at": now,
        "integrations": {
            "github": {
                "base_url": "https://api.github.com",
                "owner": "",
                "repository": "",
                "auth_type": "pat",
                "token": "",
                "read_only": True,
                "run_export_enabled": False,
                "export_base_url": "",
                "export_owner": "",
                "export_repository": "",
                "export_branch": "",
                "export_prefix": "synthetix",
                "connected": False,
                "status": "disconnected",
                "last_tested_at": "",
                "last_error": "",
            },
            "jira": {
                "base_url": "https://your-domain.atlassian.net",
                "project_key": "",
                "email": "",
                "api_token": "",
                "connected": False,
                "status": "disconnected",
                "last_tested_at": "",
                "last_error": "",
            },
            "linear": {
                "base_url": "https://api.linear.app",
                "team_key": "",
                "api_token": "",
                "connected": False,
                "status": "disconnected",
                "last_tested_at": "",
                "last_error": "",
            },
        },
        "llm": {
            "default_provider": "anthropic",
            "providers": {
                "anthropic": {
                    "model": LLM_DEFAULT_MODELS["anthropic"],
                    "base_url": "https://api.anthropic.com",
                    "api_key": "",
                    "connected": False,
                    "status": "disconnected",
                    "last_tested_at": "",
                    "last_error": "",
                },
                "openai": {
                    "model": LLM_DEFAULT_MODELS["openai"],
                    "base_url": "https://api.openai.com",
                    "api_key": "",
                    "connected": False,
                    "status": "disconnected",
                    "last_tested_at": "",
                    "last_error": "",
                },
            },
        },
        "policies": {
            "policy_pack": "standard",
            "quality_gate_min_pass_rate": 0.85,
            "require_human_approval": False,
            "block_on_critical_failures": True,
            "require_security_gate": True,
            "branch_protection_required": False,
            "exception_sla_hours": 72,
        },
        "exceptions": [],
        "rbac": {
            "roles": _default_role_permissions(),
            "assignments": [],
            "sso": {"enabled": False, "provider": ""},
        },
        "users": [
            {
                "id": "usr-local",
                "email": "local-user@synthetix.local",
                "display_name": "Local User",
                "status": "active",
                "role": "engineering",
                "created_at": now,
                "updated_at": now,
                "last_seen_at": "",
            }
        ],
        "knowledge_hub": {
            "sources": [],
            "sets": [],
            "agent_brains": [],
            "project_bindings": [],
            "specialists": [],
        },
        "audit_log": [],
    }


@dataclass
class SettingsStore:
    root_dir: str

    def __post_init__(self) -> None:
        self.root = Path(self.root_dir)
        self.root.mkdir(parents=True, exist_ok=True)
        self.path = self.root / "settings_state.json"
        self._state_backend = _SettingsStateBackend(self.path)
        self._secrets = _SecretBackend()
        # Ensure settings are initialized in active persistence backend.
        initial = self._state_backend.load(_default_settings())
        if not isinstance(initial, dict):
            initial = _default_settings()
        if not initial:
            initial = _default_settings()
        self._state_backend.save(initial)

    @staticmethod
    def _integration_secret_slot(provider: str) -> str:
        key = str(provider or "").strip().lower()
        secret_key = INTEGRATION_SECRET_KEYS.get(key, "token")
        return f"integration-{key}-{secret_key}"

    @staticmethod
    def _llm_secret_slot(provider: str) -> str:
        key = str(provider or "").strip().lower()
        return f"llm-{key}-api-key"

    def _read_integration_secret(self, provider: str, integration: dict[str, Any]) -> str:
        key = str(provider or "").strip().lower()
        secret_key = INTEGRATION_SECRET_KEYS.get(key, "token")
        local = str(integration.get(secret_key, "")).strip() if isinstance(integration, dict) else ""
        return self._secrets.read(self._integration_secret_slot(key), local)

    def _write_integration_secret(self, provider: str, integration: dict[str, Any], secret: str) -> None:
        key = str(provider or "").strip().lower()
        secret_key = INTEGRATION_SECRET_KEYS.get(key, "token")
        integration[secret_key] = self._secrets.write(self._integration_secret_slot(key), str(secret or "").strip())

    def _clear_integration_secret(self, provider: str, integration: dict[str, Any]) -> None:
        key = str(provider or "").strip().lower()
        secret_key = INTEGRATION_SECRET_KEYS.get(key, "token")
        integration[secret_key] = ""
        self._secrets.clear(self._integration_secret_slot(key))

    def _read_llm_secret(self, provider: str, cfg: dict[str, Any]) -> str:
        key = str(provider or "").strip().lower()
        local = str(cfg.get("api_key", "")).strip() if isinstance(cfg, dict) else ""
        raw = self._secrets.read(self._llm_secret_slot(key), local)
        return _normalize_llm_secret(key, raw)

    def _write_llm_secret(self, provider: str, cfg: dict[str, Any], secret: str) -> None:
        key = str(provider or "").strip().lower()
        normalized = _normalize_llm_secret(key, str(secret or "").strip())
        cfg["api_key"] = self._secrets.write(self._llm_secret_slot(key), normalized)

    def _clear_llm_secret(self, provider: str, cfg: dict[str, Any]) -> None:
        key = str(provider or "").strip().lower()
        cfg["api_key"] = ""
        self._secrets.clear(self._llm_secret_slot(key))

    def _load(self) -> dict[str, Any]:
        data = self._state_backend.load(_default_settings())
        if not isinstance(data, dict):
            data = _default_settings()

        data.setdefault("integrations", {})
        for provider, defaults in _default_settings()["integrations"].items():
            cur = data["integrations"].get(provider, {})
            if not isinstance(cur, dict):
                cur = {}
            merged = dict(defaults)
            merged.update(cur)
            data["integrations"][provider] = merged

        data.setdefault("policies", {})
        policy_defaults = _default_settings()["policies"]
        merged_policies = dict(policy_defaults)
        merged_policies.update(data["policies"] if isinstance(data["policies"], dict) else {})
        data["policies"] = merged_policies

        llm_defaults = _default_settings()["llm"]
        llm = data.get("llm", {})
        if not isinstance(llm, dict):
            llm = {}
        providers = llm.get("providers", {})
        if not isinstance(providers, dict):
            providers = {}
        merged_providers: dict[str, Any] = {}
        for provider, defaults in llm_defaults["providers"].items():
            cur = providers.get(provider, {})
            if not isinstance(cur, dict):
                cur = {}
            merged = dict(defaults)
            merged.update(cur)
            model = str(merged.get("model", "")).strip()
            merged["model"] = model or LLM_DEFAULT_MODELS[provider]
            merged_providers[provider] = merged
        default_provider = str(llm.get("default_provider", llm_defaults["default_provider"])).strip().lower()
        if default_provider not in LLM_PROVIDER_KEYS:
            default_provider = llm_defaults["default_provider"]
        data["llm"] = {
            "default_provider": default_provider,
            "providers": merged_providers,
        }

        data.setdefault("exceptions", [])
        if not isinstance(data["exceptions"], list):
            data["exceptions"] = []

        data.setdefault("rbac", {})
        if not isinstance(data["rbac"], dict):
            data["rbac"] = {}
        data["rbac"].setdefault("roles", {})
        data["rbac"].setdefault("assignments", [])
        data["rbac"].setdefault("sso", {"enabled": False, "provider": ""})
        for role, perms in _default_role_permissions().items():
            current = data["rbac"]["roles"].get(role)
            if not isinstance(current, list):
                data["rbac"]["roles"][role] = list(perms)

        users = data.get("users", [])
        if not isinstance(users, list):
            users = []
        normalized_users: list[dict[str, Any]] = []
        for user in users:
            if not isinstance(user, dict):
                continue
            email = str(user.get("email", "")).strip().lower()
            if "@" not in email:
                continue
            status = str(user.get("status", "active")).strip().lower()
            role = str(user.get("role", "engineering")).strip().lower()
            normalized_users.append(
                {
                    "id": str(user.get("id", "")).strip() or f"usr-{uuid.uuid4().hex[:10]}",
                    "email": email,
                    "display_name": str(user.get("display_name", "")).strip() or email.split("@", 1)[0],
                    "status": status if status in USER_STATUSES else "active",
                    "role": role if role in RBAC_ROLES else "engineering",
                    "created_at": str(user.get("created_at", "")).strip() or _utc_now(),
                    "updated_at": str(user.get("updated_at", "")).strip() or _utc_now(),
                    "last_seen_at": str(user.get("last_seen_at", "")).strip(),
                }
            )
        if not normalized_users:
            normalized_users = copy.deepcopy(_default_settings().get("users", []))
        data["users"] = sorted(normalized_users, key=lambda row: str(row.get("email", "")))

        knowledge_hub = data.get("knowledge_hub", {})
        if not isinstance(knowledge_hub, dict):
            knowledge_hub = {}
        for key in ("sources", "sets", "agent_brains", "project_bindings", "specialists"):
            bucket = knowledge_hub.get(key, [])
            if not isinstance(bucket, list):
                bucket = []
            knowledge_hub[key] = bucket
        normalized_brains: list[dict[str, Any]] = []
        for brain in knowledge_hub.get("agent_brains", []):
            if not isinstance(brain, dict):
                continue
            tools = brain.get("allowed_tools", [])
            if not isinstance(tools, list):
                tools = []
            scope = str(brain.get("memory_scope", "project")).strip().lower()
            if scope not in {"project", "client", "workspace", "global"}:
                scope = "project"
            normalized_brains.append(
                {
                    "brain_id": str(brain.get("brain_id", "")).strip() or f"brain-{uuid.uuid4().hex[:10]}",
                    "agent_key": str(brain.get("agent_key", "")).strip(),
                    "knowledge_set_ids": [str(item).strip() for item in brain.get("knowledge_set_ids", []) if str(item).strip()] if isinstance(brain.get("knowledge_set_ids", []), list) else [],
                    "top_k": max(1, min(50, int(brain.get("top_k", 8) or 8))),
                    "citation_required": bool(brain.get("citation_required", True)),
                    "fallback_behavior": str(brain.get("fallback_behavior", "ask_clarification")).strip() or "ask_clarification",
                    "allowed_tools": sorted({str(item).strip() for item in tools if str(item).strip()}),
                    "memory_scope": scope,
                    "memory_enabled": bool(brain.get("memory_enabled", True)),
                    "created_at": str(brain.get("created_at", "")).strip() or _utc_now(),
                    "updated_at": str(brain.get("updated_at", "")).strip() or _utc_now(),
                }
            )
        knowledge_hub["agent_brains"] = normalized_brains
        normalized_specialists: list[dict[str, Any]] = []
        for specialist in knowledge_hub.get("specialists", []):
            if not isinstance(specialist, dict):
                continue
            try:
                stage_hint = max(0, int(specialist.get("stage_hint", 0) or 0))
            except (TypeError, ValueError):
                stage_hint = 0
            try:
                min_match_score = max(1, min(10, int(specialist.get("min_match_score", 1) or 1)))
            except (TypeError, ValueError):
                min_match_score = 1
            intent_keywords = specialist.get("intent_keywords", [])
            if not isinstance(intent_keywords, list):
                intent_keywords = []
            file_patterns = specialist.get("file_patterns", [])
            if not isinstance(file_patterns, list):
                file_patterns = []
            artifact_triggers = specialist.get("artifact_triggers", [])
            if not isinstance(artifact_triggers, list):
                artifact_triggers = []
            tool_mode = str(specialist.get("tool_mode", "read_only")).strip().lower()
            if tool_mode not in SPECIALIST_TOOL_MODES:
                tool_mode = "read_only"
            depth_tier = str(specialist.get("depth_tier", "standard")).strip().lower()
            if depth_tier not in SPECIALIST_DEPTH_TIERS:
                depth_tier = "standard"
            normalized_specialists.append(
                {
                    "specialist_id": str(specialist.get("specialist_id", "")).strip() or f"spec-{uuid.uuid4().hex[:10]}",
                    "name": str(specialist.get("name", "")).strip() or "Unnamed Specialist",
                    "description": str(specialist.get("description", "")).strip(),
                    "domain": str(specialist.get("domain", "")).strip().lower(),
                    "linked_agent_key": str(specialist.get("linked_agent_key", "")).strip(),
                    "stage_hint": stage_hint,
                    "intent_keywords": sorted({str(item).strip().lower() for item in intent_keywords if str(item).strip()}),
                    "file_patterns": sorted({str(item).strip().lower() for item in file_patterns if str(item).strip()}),
                    "artifact_triggers": sorted({str(item).strip().lower() for item in artifact_triggers if str(item).strip()}),
                    "min_match_score": min_match_score,
                    "tool_mode": tool_mode,
                    "depth_tier": depth_tier,
                    "auto_route": bool(specialist.get("auto_route", True)),
                    "enabled": bool(specialist.get("enabled", True)),
                    "created_at": str(specialist.get("created_at", "")).strip() or _utc_now(),
                    "updated_at": str(specialist.get("updated_at", "")).strip() or _utc_now(),
                }
            )
        normalized_specialists.sort(key=lambda row: (str(row.get("name", "")).lower(), str(row.get("specialist_id", ""))))
        knowledge_hub["specialists"] = normalized_specialists
        data["knowledge_hub"] = knowledge_hub

        data.setdefault("audit_log", [])
        if not isinstance(data["audit_log"], list):
            data["audit_log"] = []
        return data

    def _save(self, data: dict[str, Any]) -> None:
        data["updated_at"] = _utc_now()
        self._state_backend.save(data)

    def _sanitize(self, data: dict[str, Any]) -> dict[str, Any]:
        payload = copy.deepcopy(data)
        integrations = payload.get("integrations", {})
        if isinstance(integrations, dict):
            for provider, secret_key in INTEGRATION_SECRET_KEYS.items():
                integration = integrations.get(provider, {})
                if not isinstance(integration, dict):
                    continue
                raw = self._read_integration_secret(provider, integration)
                integration[f"{secret_key}_masked"] = _mask_secret(raw)
                integration["has_secret"] = bool(raw)
                integration[secret_key] = ""
        llm = payload.get("llm", {})
        if isinstance(llm, dict):
            providers = llm.get("providers", {})
            if isinstance(providers, dict):
                for provider in LLM_PROVIDER_KEYS:
                    cfg = providers.get(provider, {})
                    if not isinstance(cfg, dict):
                        continue
                    raw = self._read_llm_secret(provider, cfg)
                    cfg["api_key_masked"] = _mask_secret(raw)
                    cfg["has_secret"] = bool(raw)
                    cfg["api_key"] = ""
        payload["meta"] = {
            "integration_providers": sorted(INTEGRATION_KEYS),
            "llm_providers": sorted(LLM_PROVIDER_KEYS),
            "policy_packs": sorted(POLICY_PACKS),
            "rbac_roles": sorted(RBAC_ROLES),
            "rbac_permissions": sorted(RBAC_PERMISSIONS),
            "knowledge_source_types": sorted(KNOWLEDGE_SOURCE_TYPES),
            "knowledge_scopes": sorted(KNOWLEDGE_SCOPES),
            "data_classifications": sorted(DATA_CLASSIFICATIONS),
            "knowledge_set_states": sorted(KNOWLEDGE_SET_STATES),
            "specialist_tool_modes": sorted(SPECIALIST_TOOL_MODES),
            "specialist_depth_tiers": sorted(SPECIALIST_DEPTH_TIERS),
            "user_statuses": sorted(USER_STATUSES),
            "secrets_backend": self._secrets.backend_label,
            "settings_backend": self._state_backend.backend_label,
        }
        return payload

    def _append_audit(
        self,
        data: dict[str, Any],
        action: str,
        target: str,
        actor: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        audit = data.setdefault("audit_log", [])
        if not isinstance(audit, list):
            audit = []
            data["audit_log"] = audit
        audit.insert(
            0,
            {
                "id": f"audit-{uuid.uuid4().hex[:10]}",
                "timestamp": _utc_now(),
                "actor": actor or "local-user",
                "action": action,
                "target": target,
                "details": details or {},
            },
        )
        # Keep the newest 500 items only.
        if len(audit) > 500:
            del audit[500:]

    def get_settings(self) -> dict[str, Any]:
        return self._sanitize(self._load())

    def get_integration_config(self, provider: str) -> dict[str, Any]:
        key = str(provider or "").strip().lower()
        if key not in INTEGRATION_KEYS:
            raise ValueError(f"unsupported integration provider: {provider}")
        data = self._load()
        integration = data.get("integrations", {}).get(key, {})
        if not isinstance(integration, dict):
            integration = {}
        out = copy.deepcopy(integration)
        secret_key = INTEGRATION_SECRET_KEYS[key]
        out[secret_key] = self._read_integration_secret(key, integration)
        return out

    def update_integration(self, provider: str, payload: dict[str, Any], actor: str = "local-user") -> dict[str, Any]:
        key = str(provider or "").strip().lower()
        if key not in INTEGRATION_KEYS:
            raise ValueError(f"unsupported integration provider: {provider}")
        data = self._load()
        integration = data["integrations"].get(key, {})
        if not isinstance(integration, dict):
            integration = {}

        if key == "github":
            allowed = {
                "base_url",
                "owner",
                "repository",
                "auth_type",
                "token",
                "read_only",
                "run_export_enabled",
                "export_base_url",
                "export_owner",
                "export_repository",
                "export_branch",
                "export_prefix",
            }
            required = {"base_url", "owner", "repository"}
        elif key == "jira":
            allowed = {"base_url", "project_key", "email", "api_token"}
            required = {"base_url", "project_key", "email"}
        else:
            allowed = {"base_url", "team_key", "api_token"}
            required = {"base_url", "team_key"}

        incoming = payload if isinstance(payload, dict) else {}
        secret_key = INTEGRATION_SECRET_KEYS[key]
        for field in allowed:
            if field not in incoming:
                continue
            value = incoming.get(field)
            if field == secret_key and not str(value or "").strip():
                # Keep existing secret when UI submits an empty placeholder value.
                continue
            if field == secret_key:
                self._write_integration_secret(key, integration, str(value or "").strip())
                continue
            if field in {"read_only", "run_export_enabled"}:
                integration[field] = bool(value)
            else:
                integration[field] = str(value or "").strip()

        if key == "github":
            export_owner_raw = str(integration.get("export_owner", "")).strip()
            export_repo_raw = str(integration.get("export_repository", "")).strip()
            parsed_owner, parsed_repo = _parse_github_repo_reference(export_repo_raw)
            if parsed_repo:
                integration["export_repository"] = parsed_repo
                if not export_owner_raw and parsed_owner:
                    integration["export_owner"] = parsed_owner

        has_secret = bool(self._read_integration_secret(key, integration))
        missing: list[str] = []
        for field in required:
            if field == secret_key:
                if not has_secret:
                    missing.append(field)
            elif not str(integration.get(field, "")).strip():
                missing.append(field)

        integration["connected"] = bool(has_secret and not missing)
        integration["status"] = "connected" if integration["connected"] else "incomplete"
        integration["last_error"] = "" if integration["connected"] else f"Missing: {', '.join(missing) if missing else 'credential'}"
        data["integrations"][key] = integration
        self._append_audit(
            data,
            action="integration_updated",
            target=key,
            actor=actor,
            details={"connected": integration["connected"], "status": integration["status"]},
        )
        self._save(data)
        return self._sanitize(data)

    def test_integration(self, provider: str, actor: str = "local-user") -> dict[str, Any]:
        key = str(provider or "").strip().lower()
        if key not in INTEGRATION_KEYS:
            raise ValueError(f"unsupported integration provider: {provider}")

        data = self._load()
        integration = data["integrations"].get(key, {})
        if not isinstance(integration, dict):
            integration = {}
        secret_key = INTEGRATION_SECRET_KEYS[key]

        required_by_provider = {
            "github": ["owner", "repository", secret_key],
            "jira": ["project_key", "email", secret_key],
            "linear": ["team_key", secret_key],
        }
        required = required_by_provider[key]

        checks: list[dict[str, Any]] = []
        base_url = str(integration.get("base_url", "")).strip()
        checks.append(
            {
                "name": "base_url_present",
                "ok": bool(base_url),
                "message": "Base URL is required",
            }
        )
        checks.append(
            {
                "name": "base_url_scheme",
                "ok": base_url.startswith("http://") or base_url.startswith("https://"),
                "message": "Base URL must start with http:// or https://",
            }
        )
        resolved_secret = self._read_integration_secret(key, integration)
        for field in required:
            if field == secret_key:
                ok = bool(resolved_secret)
            else:
                ok = bool(str(integration.get(field, "")).strip())
            checks.append(
                {
                    "name": field,
                    "ok": ok,
                    "message": f"{field} is required",
                }
            )
        if key == "github" and bool(integration.get("run_export_enabled", False)):
            export_owner = str(integration.get("export_owner", "")).strip()
            export_repository = str(integration.get("export_repository", "")).strip()
            parsed_owner, parsed_repo = _parse_github_repo_reference(export_repository)
            if parsed_repo:
                export_repository = parsed_repo
                integration["export_repository"] = parsed_repo
                if not export_owner and parsed_owner:
                    export_owner = parsed_owner
                    integration["export_owner"] = parsed_owner
            if bool(export_owner) != bool(export_repository):
                checks.append(
                    {
                        "name": "export_target",
                        "ok": False,
                        "message": "Set both export_owner and export_repository, or leave both empty to use source repository",
                    }
                )
            export_base_url = str(integration.get("export_base_url", "")).strip()
            if export_base_url:
                checks.append(
                    {
                        "name": "export_base_url_scheme",
                        "ok": export_base_url.startswith("http://") or export_base_url.startswith("https://"),
                        "message": "export_base_url must start with http:// or https://",
                    }
                )
            checks.append(
                {
                    "name": "read_only",
                    "ok": not bool(integration.get("read_only", True)),
                    "message": "Disable read-only access to export run artifacts to GitHub",
                }
            )

        passed = all(bool(item.get("ok")) for item in checks)
        integration["connected"] = passed
        integration["status"] = "connected" if passed else "error"
        integration["last_tested_at"] = _utc_now()
        integration["last_error"] = "" if passed else "; ".join(item["message"] for item in checks if not item.get("ok"))
        data["integrations"][key] = integration
        self._append_audit(
            data,
            action="integration_tested",
            target=key,
            actor=actor,
            details={"passed": passed},
        )
        self._save(data)

        return {
            "provider": key,
            "test_ok": passed,
            "checks": checks,
            "integration": self._sanitize(data).get("integrations", {}).get(key, {}),
            "settings": self._sanitize(data),
        }

    def disconnect_integration(self, provider: str, clear_secret: bool = False, actor: str = "local-user") -> dict[str, Any]:
        key = str(provider or "").strip().lower()
        if key not in INTEGRATION_KEYS:
            raise ValueError(f"unsupported integration provider: {provider}")
        data = self._load()
        integration = data["integrations"].get(key, {})
        if not isinstance(integration, dict):
            integration = {}
        integration["connected"] = False
        integration["status"] = "disconnected"
        integration["last_error"] = ""
        if clear_secret:
            self._clear_integration_secret(key, integration)
        data["integrations"][key] = integration
        self._append_audit(data, action="integration_disconnected", target=key, actor=actor, details={"clear_secret": bool(clear_secret)})
        self._save(data)
        return self._sanitize(data)

    def get_llm_provider_config(self, provider: str) -> dict[str, Any]:
        key = str(provider or "").strip().lower()
        if key not in LLM_PROVIDER_KEYS:
            raise ValueError(f"unsupported llm provider: {provider}")
        data = self._load()
        llm = data.get("llm", {})
        providers = llm.get("providers", {}) if isinstance(llm, dict) else {}
        cfg = providers.get(key, {}) if isinstance(providers, dict) else {}
        if not isinstance(cfg, dict):
            cfg = {}
        out = copy.deepcopy(cfg)
        out["api_key"] = self._read_llm_secret(key, cfg)
        return out

    def resolve_llm_credentials(self, provider: str, requested_model: str = "") -> dict[str, Any]:
        key = str(provider or "").strip().lower()
        if key not in LLM_PROVIDER_KEYS:
            raise ValueError(f"unsupported llm provider: {provider}")
        cfg = self.get_llm_provider_config(key)
        api_key = str(cfg.get("api_key", "")).strip()
        model = str(requested_model or "").strip() or str(cfg.get("model", "")).strip() or LLM_DEFAULT_MODELS[key]
        return {"provider": key, "api_key": api_key, "model": model, "has_secret": bool(api_key)}

    def update_llm_provider(self, provider: str, payload: dict[str, Any], actor: str = "local-user") -> dict[str, Any]:
        key = str(provider or "").strip().lower()
        if key not in LLM_PROVIDER_KEYS:
            raise ValueError(f"unsupported llm provider: {provider}")
        data = self._load()
        llm = data.get("llm", {})
        if not isinstance(llm, dict):
            llm = {}
        providers = llm.get("providers", {})
        if not isinstance(providers, dict):
            providers = {}
        cfg = providers.get(key, {})
        if not isinstance(cfg, dict):
            cfg = {}

        incoming = payload if isinstance(payload, dict) else {}
        model_value = str(incoming.get("model", "")).strip()
        if model_value:
            cfg["model"] = model_value
        elif not str(cfg.get("model", "")).strip():
            cfg["model"] = LLM_DEFAULT_MODELS[key]

        if "base_url" in incoming:
            cfg["base_url"] = str(incoming.get("base_url", "")).strip()

        # Blank secret input keeps the existing key to match UI behavior.
        if "api_key" in incoming:
            secret = str(incoming.get("api_key", "")).strip()
            if secret:
                self._write_llm_secret(key, cfg, secret)

        if bool(incoming.get("set_default_provider", False)):
            llm["default_provider"] = key

        has_secret = bool(self._read_llm_secret(key, cfg))
        has_model = bool(str(cfg.get("model", "")).strip())
        cfg["connected"] = bool(has_secret and has_model)
        cfg["status"] = "connected" if cfg["connected"] else "incomplete"
        missing = []
        if not has_model:
            missing.append("model")
        if not has_secret:
            missing.append("api_key")
        cfg["last_error"] = "" if cfg["connected"] else f"Missing: {', '.join(missing)}"

        providers[key] = cfg
        llm["providers"] = providers
        llm["default_provider"] = str(llm.get("default_provider", "anthropic")).strip().lower()
        if llm["default_provider"] not in LLM_PROVIDER_KEYS:
            llm["default_provider"] = "anthropic"
        data["llm"] = llm
        self._append_audit(
            data,
            action="llm_provider_updated",
            target=key,
            actor=actor,
            details={"connected": cfg["connected"], "status": cfg["status"]},
        )
        self._save(data)
        return self._sanitize(data)

    def test_llm_provider(self, provider: str, actor: str = "local-user") -> dict[str, Any]:
        key = str(provider or "").strip().lower()
        if key not in LLM_PROVIDER_KEYS:
            raise ValueError(f"unsupported llm provider: {provider}")
        data = self._load()
        llm = data.get("llm", {})
        providers = llm.get("providers", {}) if isinstance(llm, dict) else {}
        cfg = providers.get(key, {}) if isinstance(providers, dict) else {}
        if not isinstance(cfg, dict):
            cfg = {}

        base_url = str(cfg.get("base_url", "")).strip()
        checks: list[dict[str, Any]] = [
            {
                "name": "model",
                "ok": bool(str(cfg.get("model", "")).strip()),
                "message": "model is required",
            },
            {
                "name": "api_key",
                "ok": bool(self._read_llm_secret(key, cfg)),
                "message": "api_key is required",
            },
        ]
        if base_url:
            checks.append(
                {
                    "name": "base_url_scheme",
                    "ok": base_url.startswith("http://") or base_url.startswith("https://"),
                    "message": "base_url must start with http:// or https://",
                }
            )

        passed = all(bool(item.get("ok")) for item in checks)
        cfg["connected"] = passed
        cfg["status"] = "connected" if passed else "error"
        cfg["last_tested_at"] = _utc_now()
        cfg["last_error"] = "" if passed else "; ".join(item["message"] for item in checks if not item.get("ok"))
        providers[key] = cfg
        llm["providers"] = providers
        data["llm"] = llm
        self._append_audit(
            data,
            action="llm_provider_tested",
            target=key,
            actor=actor,
            details={"passed": passed},
        )
        self._save(data)
        sanitized = self._sanitize(data)
        return {
            "provider": key,
            "test_ok": passed,
            "checks": checks,
            "llm_provider": sanitized.get("llm", {}).get("providers", {}).get(key, {}),
            "settings": sanitized,
        }

    def disconnect_llm_provider(self, provider: str, clear_secret: bool = False, actor: str = "local-user") -> dict[str, Any]:
        key = str(provider or "").strip().lower()
        if key not in LLM_PROVIDER_KEYS:
            raise ValueError(f"unsupported llm provider: {provider}")
        data = self._load()
        llm = data.get("llm", {})
        if not isinstance(llm, dict):
            llm = {}
        providers = llm.get("providers", {})
        if not isinstance(providers, dict):
            providers = {}
        cfg = providers.get(key, {})
        if not isinstance(cfg, dict):
            cfg = {}

        cfg["connected"] = False
        cfg["status"] = "disconnected"
        cfg["last_error"] = ""
        if clear_secret:
            self._clear_llm_secret(key, cfg)
        providers[key] = cfg
        llm["providers"] = providers
        data["llm"] = llm
        self._append_audit(data, action="llm_provider_disconnected", target=key, actor=actor, details={"clear_secret": bool(clear_secret)})
        self._save(data)
        return self._sanitize(data)

    def update_policies(self, payload: dict[str, Any], actor: str = "local-user") -> dict[str, Any]:
        incoming = payload if isinstance(payload, dict) else {}
        data = self._load()
        policies = data.get("policies", {})
        if not isinstance(policies, dict):
            policies = {}

        pack = str(incoming.get("policy_pack", policies.get("policy_pack", "standard"))).strip().lower()
        policies["policy_pack"] = pack if pack in POLICY_PACKS else "standard"
        policies["quality_gate_min_pass_rate"] = max(
            0.0,
            min(1.0, float(incoming.get("quality_gate_min_pass_rate", policies.get("quality_gate_min_pass_rate", 0.85)) or 0.85)),
        )
        policies["require_human_approval"] = bool(incoming.get("require_human_approval", policies.get("require_human_approval", False)))
        policies["block_on_critical_failures"] = bool(
            incoming.get("block_on_critical_failures", policies.get("block_on_critical_failures", True))
        )
        policies["require_security_gate"] = bool(incoming.get("require_security_gate", policies.get("require_security_gate", True)))
        policies["branch_protection_required"] = bool(
            incoming.get("branch_protection_required", policies.get("branch_protection_required", False))
        )
        policies["exception_sla_hours"] = max(1, int(incoming.get("exception_sla_hours", policies.get("exception_sla_hours", 72)) or 72))
        data["policies"] = policies
        self._append_audit(
            data,
            action="policies_updated",
            target="policy_pack",
            actor=actor,
            details={"policy_pack": policies["policy_pack"]},
        )
        self._save(data)
        return self._sanitize(data)

    def add_exception(self, payload: dict[str, Any], actor: str = "local-user") -> dict[str, Any]:
        incoming = payload if isinstance(payload, dict) else {}
        rule = str(incoming.get("rule", "")).strip()
        reason = str(incoming.get("reason", "")).strip()
        owner = str(incoming.get("owner", "")).strip()
        expires_at = str(incoming.get("expires_at", "")).strip()
        if not rule:
            raise ValueError("rule is required")
        if not reason:
            raise ValueError("reason is required")
        if not owner:
            raise ValueError("owner is required")

        data = self._load()
        exceptions = data.get("exceptions", [])
        if not isinstance(exceptions, list):
            exceptions = []
        item = {
            "id": f"exc-{uuid.uuid4().hex[:10]}",
            "rule": rule,
            "reason": reason,
            "owner": owner,
            "status": "open",
            "created_at": _utc_now(),
            "expires_at": expires_at,
        }
        exceptions.insert(0, item)
        data["exceptions"] = exceptions
        self._append_audit(
            data,
            action="policy_exception_created",
            target=item["id"],
            actor=actor,
            details={"rule": rule, "owner": owner},
        )
        self._save(data)
        return self._sanitize(data)

    def resolve_exception(self, exception_id: str, actor: str = "local-user") -> dict[str, Any]:
        target_id = str(exception_id or "").strip()
        if not target_id:
            raise ValueError("exception_id is required")

        data = self._load()
        exceptions = data.get("exceptions", [])
        if not isinstance(exceptions, list):
            exceptions = []

        updated = False
        for item in exceptions:
            if not isinstance(item, dict):
                continue
            if str(item.get("id", "")) != target_id:
                continue
            item["status"] = "resolved"
            item["resolved_at"] = _utc_now()
            item["resolved_by"] = actor
            updated = True
            break

        if not updated:
            raise ValueError("exception not found")

        data["exceptions"] = exceptions
        self._append_audit(
            data,
            action="policy_exception_resolved",
            target=target_id,
            actor=actor,
            details={},
        )
        self._save(data)
        return self._sanitize(data)

    def update_role_permissions(self, role: str, permissions: list[str], actor: str = "local-user") -> dict[str, Any]:
        role_key = str(role or "").strip().lower()
        if role_key not in RBAC_ROLES:
            raise ValueError(f"unsupported role: {role}")
        if not isinstance(permissions, list):
            raise ValueError("permissions must be a list")
        normalized = sorted({str(p).strip() for p in permissions if str(p).strip() in RBAC_PERMISSIONS})

        data = self._load()
        rbac = data.get("rbac", {})
        if not isinstance(rbac, dict):
            rbac = {}
        roles = rbac.get("roles", {})
        if not isinstance(roles, dict):
            roles = {}
        roles[role_key] = normalized
        rbac["roles"] = roles
        data["rbac"] = rbac
        self._append_audit(
            data,
            action="rbac_role_updated",
            target=role_key,
            actor=actor,
            details={"permissions": normalized},
        )
        self._save(data)
        return self._sanitize(data)

    def upsert_assignment(self, email: str, role: str, actor: str = "local-user") -> dict[str, Any]:
        user_email = str(email or "").strip().lower()
        role_key = str(role or "").strip().lower()
        if "@" not in user_email:
            raise ValueError("valid email is required")
        if role_key not in RBAC_ROLES:
            raise ValueError(f"unsupported role: {role}")

        data = self._load()
        rbac = data.get("rbac", {})
        if not isinstance(rbac, dict):
            rbac = {}
        assignments = rbac.get("assignments", [])
        if not isinstance(assignments, list):
            assignments = []

        found = False
        for item in assignments:
            if not isinstance(item, dict):
                continue
            if str(item.get("email", "")).strip().lower() != user_email:
                continue
            item["role"] = role_key
            item["updated_at"] = _utc_now()
            found = True
            break
        if not found:
            assignments.append(
                {
                    "email": user_email,
                    "role": role_key,
                    "created_at": _utc_now(),
                }
            )
        assignments.sort(key=lambda x: str(x.get("email", "")))
        rbac["assignments"] = assignments
        data["rbac"] = rbac
        self._append_audit(
            data,
            action="rbac_assignment_upserted",
            target=user_email,
            actor=actor,
            details={"role": role_key},
        )
        self._save(data)
        return self._sanitize(data)

    def remove_assignment(self, email: str, actor: str = "local-user") -> dict[str, Any]:
        user_email = str(email or "").strip().lower()
        if not user_email:
            raise ValueError("email is required")
        data = self._load()
        rbac = data.get("rbac", {})
        if not isinstance(rbac, dict):
            rbac = {}
        assignments = rbac.get("assignments", [])
        if not isinstance(assignments, list):
            assignments = []
        next_assignments = [item for item in assignments if str(item.get("email", "")).strip().lower() != user_email]
        if len(next_assignments) == len(assignments):
            raise ValueError("assignment not found")
        rbac["assignments"] = next_assignments
        data["rbac"] = rbac
        self._append_audit(
            data,
            action="rbac_assignment_removed",
            target=user_email,
            actor=actor,
            details={},
        )
        self._save(data)
        return self._sanitize(data)

    @staticmethod
    def _normalize_email(email: str) -> str:
        return str(email or "").strip().lower()

    @staticmethod
    def _normalize_role(role: str, default: str = "engineering") -> str:
        role_key = str(role or "").strip().lower()
        return role_key if role_key in RBAC_ROLES else default

    @staticmethod
    def _normalize_status(status: str, default: str = "active") -> str:
        status_key = str(status or "").strip().lower()
        return status_key if status_key in USER_STATUSES else default

    def resolve_user_access(self, email: str) -> dict[str, Any]:
        user_email = self._normalize_email(email)
        data = self._load()
        users = data.get("users", [])
        assignments = data.get("rbac", {}).get("assignments", [])
        roles = data.get("rbac", {}).get("roles", {})
        if not isinstance(users, list):
            users = []
        if not isinstance(assignments, list):
            assignments = []
        if not isinstance(roles, dict):
            roles = {}

        profile: dict[str, Any] = {}
        for user in users:
            if not isinstance(user, dict):
                continue
            if self._normalize_email(user.get("email", "")) != user_email:
                continue
            profile = copy.deepcopy(user)
            break

        assigned_role = ""
        for item in assignments:
            if not isinstance(item, dict):
                continue
            if self._normalize_email(item.get("email", "")) != user_email:
                continue
            assigned_role = self._normalize_role(str(item.get("role", "")), default="")
            break

        fallback_name = user_email.split("@", 1)[0] if "@" in user_email else "Local User"
        role = assigned_role or self._normalize_role(str(profile.get("role", "")))
        permissions = roles.get(role, [])
        if not isinstance(permissions, list):
            permissions = []
        resolved_profile = {
            "id": str(profile.get("id", "")).strip() or f"usr-{fallback_name}",
            "email": user_email or "local-user@synthetix.local",
            "display_name": str(profile.get("display_name", "")).strip() or fallback_name,
            "status": self._normalize_status(str(profile.get("status", "active"))),
            "role": role,
            "permissions": sorted({str(item).strip() for item in permissions if str(item).strip()}),
            "known_user": bool(profile),
        }
        return resolved_profile

    def upsert_user(self, payload: dict[str, Any], actor: str = "local-user") -> dict[str, Any]:
        incoming = payload if isinstance(payload, dict) else {}
        user_email = self._normalize_email(str(incoming.get("email", "")))
        if "@" not in user_email:
            raise ValueError("valid email is required")
        display_name = str(incoming.get("display_name", "")).strip() or user_email.split("@", 1)[0]
        role_key = self._normalize_role(str(incoming.get("role", "engineering")))
        status_key = self._normalize_status(str(incoming.get("status", "active")))

        data = self._load()
        users = data.get("users", [])
        if not isinstance(users, list):
            users = []

        now = _utc_now()
        updated = False
        for user in users:
            if not isinstance(user, dict):
                continue
            if self._normalize_email(user.get("email", "")) != user_email:
                continue
            user["display_name"] = display_name
            user["role"] = role_key
            user["status"] = status_key
            user["updated_at"] = now
            updated = True
            break
        if not updated:
            users.append(
                {
                    "id": f"usr-{uuid.uuid4().hex[:10]}",
                    "email": user_email,
                    "display_name": display_name,
                    "status": status_key,
                    "role": role_key,
                    "created_at": now,
                    "updated_at": now,
                    "last_seen_at": "",
                }
            )
        users.sort(key=lambda row: str(row.get("email", "")))
        data["users"] = users

        # Keep RBAC assignment aligned with explicit user role.
        rbac = data.get("rbac", {})
        if not isinstance(rbac, dict):
            rbac = {}
        assignments = rbac.get("assignments", [])
        if not isinstance(assignments, list):
            assignments = []
        assignment_found = False
        for item in assignments:
            if not isinstance(item, dict):
                continue
            if self._normalize_email(item.get("email", "")) != user_email:
                continue
            item["role"] = role_key
            item["updated_at"] = now
            assignment_found = True
            break
        if not assignment_found:
            assignments.append({"email": user_email, "role": role_key, "created_at": now})
        assignments.sort(key=lambda row: str(row.get("email", "")))
        rbac["assignments"] = assignments
        data["rbac"] = rbac

        self._append_audit(
            data,
            action="user_upserted",
            target=user_email,
            actor=actor,
            details={"role": role_key, "status": status_key},
        )
        self._save(data)
        return self._sanitize(data)

    def set_user_status(self, email: str, status: str, actor: str = "local-user") -> dict[str, Any]:
        user_email = self._normalize_email(email)
        if "@" not in user_email:
            raise ValueError("valid email is required")
        status_key = self._normalize_status(status)
        data = self._load()
        users = data.get("users", [])
        if not isinstance(users, list):
            users = []
        found = False
        for user in users:
            if not isinstance(user, dict):
                continue
            if self._normalize_email(user.get("email", "")) != user_email:
                continue
            user["status"] = status_key
            user["updated_at"] = _utc_now()
            found = True
            break
        if not found:
            raise ValueError("user not found")
        data["users"] = users
        self._append_audit(
            data,
            action="user_status_updated",
            target=user_email,
            actor=actor,
            details={"status": status_key},
        )
        self._save(data)
        return self._sanitize(data)

    def remove_user(self, email: str, actor: str = "local-user") -> dict[str, Any]:
        user_email = self._normalize_email(email)
        if not user_email:
            raise ValueError("email is required")
        data = self._load()
        users = data.get("users", [])
        if not isinstance(users, list):
            users = []
        next_users = [row for row in users if self._normalize_email(row.get("email", "")) != user_email]
        if len(next_users) == len(users):
            raise ValueError("user not found")
        data["users"] = next_users

        # Also remove RBAC assignment when user is removed.
        rbac = data.get("rbac", {})
        if not isinstance(rbac, dict):
            rbac = {}
        assignments = rbac.get("assignments", [])
        if not isinstance(assignments, list):
            assignments = []
        rbac["assignments"] = [row for row in assignments if self._normalize_email(row.get("email", "")) != user_email]
        data["rbac"] = rbac

        self._append_audit(data, action="user_removed", target=user_email, actor=actor, details={})
        self._save(data)
        return self._sanitize(data)

    @staticmethod
    def _normalize_source_type(source_type: str) -> str:
        value = str(source_type or "").strip().lower()
        return value if value in KNOWLEDGE_SOURCE_TYPES else "other"

    @staticmethod
    def _normalize_scope(scope: str) -> str:
        value = str(scope or "").strip().lower()
        return value if value in KNOWLEDGE_SCOPES else "project"

    @staticmethod
    def _normalize_data_classification(classification: str) -> str:
        value = str(classification or "").strip().lower()
        return value if value in DATA_CLASSIFICATIONS else "internal"

    @staticmethod
    def _normalize_set_state(state: str) -> str:
        value = str(state or "").strip().lower()
        return value if value in KNOWLEDGE_SET_STATES else "draft"

    def upsert_knowledge_source(self, payload: dict[str, Any], actor: str = "local-user") -> dict[str, Any]:
        incoming = payload if isinstance(payload, dict) else {}
        name = str(incoming.get("name", "")).strip()
        if not name:
            raise ValueError("name is required")
        source_id = str(incoming.get("source_id", "")).strip() or f"src-{uuid.uuid4().hex[:10]}"
        source_type = self._normalize_source_type(str(incoming.get("type", "")))
        scope = self._normalize_scope(str(incoming.get("scope", "")))
        classification = self._normalize_data_classification(str(incoming.get("data_classification", "")))
        status = str(incoming.get("status", "active")).strip().lower() or "active"
        tags = incoming.get("tags", [])
        if not isinstance(tags, list):
            tags = [str(part).strip() for part in str(tags).split(",")]
        clean_tags = sorted({str(tag).strip() for tag in tags if str(tag).strip()})
        now = _utc_now()

        data = self._load()
        hub = data.get("knowledge_hub", {})
        if not isinstance(hub, dict):
            hub = {}
        sources = hub.get("sources", [])
        if not isinstance(sources, list):
            sources = []

        updated = False
        for source in sources:
            if not isinstance(source, dict):
                continue
            if str(source.get("source_id", "")).strip() != source_id:
                continue
            source.update(
                {
                    "name": name,
                    "type": source_type,
                    "scope": scope,
                    "data_classification": classification,
                    "status": status,
                    "description": str(incoming.get("description", "")).strip(),
                    "location": str(incoming.get("location", "")).strip(),
                    "refresh_policy": str(incoming.get("refresh_policy", "manual")).strip() or "manual",
                    "retention_policy": str(incoming.get("retention_policy", "persist")).strip() or "persist",
                    "tags": clean_tags,
                    "updated_at": now,
                }
            )
            updated = True
            break
        if not updated:
            sources.append(
                {
                    "source_id": source_id,
                    "name": name,
                    "type": source_type,
                    "scope": scope,
                    "data_classification": classification,
                    "status": status,
                    "description": str(incoming.get("description", "")).strip(),
                    "location": str(incoming.get("location", "")).strip(),
                    "refresh_policy": str(incoming.get("refresh_policy", "manual")).strip() or "manual",
                    "retention_policy": str(incoming.get("retention_policy", "persist")).strip() or "persist",
                    "tags": clean_tags,
                    "created_at": now,
                    "updated_at": now,
                }
            )
        sources.sort(key=lambda row: str(row.get("name", "")).lower())
        hub["sources"] = sources
        data["knowledge_hub"] = hub
        self._append_audit(
            data,
            action="knowledge_source_upserted",
            target=source_id,
            actor=actor,
            details={"name": name, "scope": scope},
        )
        self._save(data)
        return self._sanitize(data)

    def remove_knowledge_source(self, source_id: str, actor: str = "local-user") -> dict[str, Any]:
        target_id = str(source_id or "").strip()
        if not target_id:
            raise ValueError("source_id is required")
        data = self._load()
        hub = data.get("knowledge_hub", {})
        if not isinstance(hub, dict):
            hub = {}
        sources = hub.get("sources", [])
        if not isinstance(sources, list):
            sources = []
        next_sources = [row for row in sources if str(row.get("source_id", "")).strip() != target_id]
        if len(next_sources) == len(sources):
            raise ValueError("knowledge source not found")
        hub["sources"] = next_sources

        # Remove references from sets.
        sets = hub.get("sets", [])
        if not isinstance(sets, list):
            sets = []
        for set_row in sets:
            if not isinstance(set_row, dict):
                continue
            source_ids = set_row.get("source_ids", [])
            if not isinstance(source_ids, list):
                source_ids = []
            set_row["source_ids"] = [sid for sid in source_ids if str(sid).strip() != target_id]
            set_row["updated_at"] = _utc_now()
        hub["sets"] = sets
        data["knowledge_hub"] = hub

        self._append_audit(data, action="knowledge_source_removed", target=target_id, actor=actor, details={})
        self._save(data)
        return self._sanitize(data)

    def upsert_knowledge_set(self, payload: dict[str, Any], actor: str = "local-user") -> dict[str, Any]:
        incoming = payload if isinstance(payload, dict) else {}
        name = str(incoming.get("name", "")).strip()
        if not name:
            raise ValueError("name is required")
        set_id = str(incoming.get("set_id", "")).strip() or f"set-{uuid.uuid4().hex[:10]}"
        version = str(incoming.get("version", "1.0.0")).strip() or "1.0.0"
        publish_state = self._normalize_set_state(str(incoming.get("publish_state", "draft")))
        scope = self._normalize_scope(str(incoming.get("scope", "")))
        source_ids = incoming.get("source_ids", [])
        if not isinstance(source_ids, list):
            source_ids = [part.strip() for part in str(source_ids).split(",")]
        clean_source_ids = sorted({str(item).strip() for item in source_ids if str(item).strip()})
        tags = incoming.get("tags", [])
        if not isinstance(tags, list):
            tags = [part.strip() for part in str(tags).split(",")]
        clean_tags = sorted({str(item).strip() for item in tags if str(item).strip()})
        now = _utc_now()

        data = self._load()
        hub = data.get("knowledge_hub", {})
        if not isinstance(hub, dict):
            hub = {}
        sets = hub.get("sets", [])
        if not isinstance(sets, list):
            sets = []
        updated = False
        for set_row in sets:
            if not isinstance(set_row, dict):
                continue
            if str(set_row.get("set_id", "")).strip() != set_id:
                continue
            set_row.update(
                {
                    "name": name,
                    "version": version,
                    "publish_state": publish_state,
                    "scope": scope,
                    "source_ids": clean_source_ids,
                    "description": str(incoming.get("description", "")).strip(),
                    "tags": clean_tags,
                    "updated_at": now,
                }
            )
            updated = True
            break
        if not updated:
            sets.append(
                {
                    "set_id": set_id,
                    "name": name,
                    "version": version,
                    "publish_state": publish_state,
                    "scope": scope,
                    "source_ids": clean_source_ids,
                    "description": str(incoming.get("description", "")).strip(),
                    "tags": clean_tags,
                    "created_at": now,
                    "updated_at": now,
                }
            )
        sets.sort(key=lambda row: str(row.get("name", "")).lower())
        hub["sets"] = sets
        data["knowledge_hub"] = hub
        self._append_audit(
            data,
            action="knowledge_set_upserted",
            target=set_id,
            actor=actor,
            details={"name": name, "version": version},
        )
        self._save(data)
        return self._sanitize(data)

    def remove_knowledge_set(self, set_id: str, actor: str = "local-user") -> dict[str, Any]:
        target_id = str(set_id or "").strip()
        if not target_id:
            raise ValueError("set_id is required")
        data = self._load()
        hub = data.get("knowledge_hub", {})
        if not isinstance(hub, dict):
            hub = {}
        sets = hub.get("sets", [])
        if not isinstance(sets, list):
            sets = []
        next_sets = [row for row in sets if str(row.get("set_id", "")).strip() != target_id]
        if len(next_sets) == len(sets):
            raise ValueError("knowledge set not found")
        hub["sets"] = next_sets

        brains = hub.get("agent_brains", [])
        if not isinstance(brains, list):
            brains = []
        for brain in brains:
            if not isinstance(brain, dict):
                continue
            set_ids = brain.get("knowledge_set_ids", [])
            if not isinstance(set_ids, list):
                set_ids = []
            brain["knowledge_set_ids"] = [sid for sid in set_ids if str(sid).strip() != target_id]
            brain["updated_at"] = _utc_now()
        hub["agent_brains"] = brains

        bindings = hub.get("project_bindings", [])
        if not isinstance(bindings, list):
            bindings = []
        for binding in bindings:
            if not isinstance(binding, dict):
                continue
            set_ids = binding.get("knowledge_set_ids", [])
            if not isinstance(set_ids, list):
                set_ids = []
            binding["knowledge_set_ids"] = [sid for sid in set_ids if str(sid).strip() != target_id]
            binding["updated_at"] = _utc_now()
        hub["project_bindings"] = bindings
        data["knowledge_hub"] = hub

        self._append_audit(data, action="knowledge_set_removed", target=target_id, actor=actor, details={})
        self._save(data)
        return self._sanitize(data)

    def upsert_agent_brain(self, payload: dict[str, Any], actor: str = "local-user") -> dict[str, Any]:
        incoming = payload if isinstance(payload, dict) else {}
        agent_key = str(incoming.get("agent_key", "")).strip()
        if not agent_key:
            raise ValueError("agent_key is required")
        set_ids = incoming.get("knowledge_set_ids", [])
        if not isinstance(set_ids, list):
            set_ids = [part.strip() for part in str(set_ids).split(",")]
        clean_set_ids = sorted({str(item).strip() for item in set_ids if str(item).strip()})
        top_k = max(1, min(50, int(incoming.get("top_k", 8) or 8)))
        citation_required = bool(incoming.get("citation_required", True))
        fallback = str(incoming.get("fallback_behavior", "ask_clarification")).strip() or "ask_clarification"
        allowed_tools = incoming.get("allowed_tools", [])
        if not isinstance(allowed_tools, list):
            allowed_tools = [part.strip() for part in str(allowed_tools).split(",")]
        clean_tools = sorted({str(item).strip() for item in allowed_tools if str(item).strip()})
        memory_scope = str(incoming.get("memory_scope", "project")).strip().lower() or "project"
        if memory_scope not in {"project", "client", "workspace", "global"}:
            memory_scope = "project"
        memory_enabled = bool(incoming.get("memory_enabled", True))
        now = _utc_now()

        data = self._load()
        hub = data.get("knowledge_hub", {})
        if not isinstance(hub, dict):
            hub = {}
        brains = hub.get("agent_brains", [])
        if not isinstance(brains, list):
            brains = []
        updated = False
        for brain in brains:
            if not isinstance(brain, dict):
                continue
            if str(brain.get("agent_key", "")).strip().lower() != agent_key.lower():
                continue
            brain.update(
                {
                    "agent_key": agent_key,
                    "knowledge_set_ids": clean_set_ids,
                    "top_k": top_k,
                    "citation_required": citation_required,
                    "fallback_behavior": fallback,
                    "allowed_tools": clean_tools,
                    "memory_scope": memory_scope,
                    "memory_enabled": memory_enabled,
                    "updated_at": now,
                }
            )
            updated = True
            break
        if not updated:
            brains.append(
                {
                    "brain_id": f"brain-{uuid.uuid4().hex[:10]}",
                    "agent_key": agent_key,
                    "knowledge_set_ids": clean_set_ids,
                    "top_k": top_k,
                    "citation_required": citation_required,
                    "fallback_behavior": fallback,
                    "allowed_tools": clean_tools,
                    "memory_scope": memory_scope,
                    "memory_enabled": memory_enabled,
                    "created_at": now,
                    "updated_at": now,
                }
            )
        brains.sort(key=lambda row: str(row.get("agent_key", "")).lower())
        hub["agent_brains"] = brains
        data["knowledge_hub"] = hub
        self._append_audit(
            data,
            action="agent_brain_upserted",
            target=agent_key,
            actor=actor,
            details={"knowledge_set_ids": clean_set_ids, "top_k": top_k, "allowed_tools": clean_tools, "memory_scope": memory_scope},
        )
        self._save(data)
        return self._sanitize(data)

    def upsert_specialist(self, payload: dict[str, Any], actor: str = "local-user") -> dict[str, Any]:
        incoming = payload if isinstance(payload, dict) else {}
        name = str(incoming.get("name", "")).strip()
        if not name:
            raise ValueError("name is required")
        specialist_id = str(incoming.get("specialist_id", "")).strip() or f"spec-{uuid.uuid4().hex[:10]}"
        domain = str(incoming.get("domain", "")).strip().lower()
        linked_agent_key = str(incoming.get("linked_agent_key", "")).strip()
        stage_hint = max(0, int(incoming.get("stage_hint", 0) or 0))
        min_match_score = max(1, min(10, int(incoming.get("min_match_score", 1) or 1)))
        tool_mode = str(incoming.get("tool_mode", "read_only")).strip().lower()
        if tool_mode not in SPECIALIST_TOOL_MODES:
            tool_mode = "read_only"
        depth_tier = str(incoming.get("depth_tier", "standard")).strip().lower()
        if depth_tier not in SPECIALIST_DEPTH_TIERS:
            depth_tier = "standard"

        def _clean_list(value: Any) -> list[str]:
            if isinstance(value, list):
                rows = value
            else:
                rows = [part.strip() for part in str(value or "").split(",")]
            return sorted({str(item).strip().lower() for item in rows if str(item).strip()})

        intent_keywords = _clean_list(incoming.get("intent_keywords", []))
        file_patterns = _clean_list(incoming.get("file_patterns", []))
        artifact_triggers = _clean_list(incoming.get("artifact_triggers", []))
        now = _utc_now()

        data = self._load()
        hub = data.get("knowledge_hub", {})
        if not isinstance(hub, dict):
            hub = {}
        specialists = hub.get("specialists", [])
        if not isinstance(specialists, list):
            specialists = []
        updated = False
        for row in specialists:
            if not isinstance(row, dict):
                continue
            if str(row.get("specialist_id", "")).strip() != specialist_id:
                continue
            row.update(
                {
                    "name": name,
                    "description": str(incoming.get("description", "")).strip(),
                    "domain": domain,
                    "linked_agent_key": linked_agent_key,
                    "stage_hint": stage_hint,
                    "intent_keywords": intent_keywords,
                    "file_patterns": file_patterns,
                    "artifact_triggers": artifact_triggers,
                    "min_match_score": min_match_score,
                    "tool_mode": tool_mode,
                    "depth_tier": depth_tier,
                    "auto_route": bool(incoming.get("auto_route", True)),
                    "enabled": bool(incoming.get("enabled", True)),
                    "updated_at": now,
                }
            )
            updated = True
            break
        if not updated:
            specialists.append(
                {
                    "specialist_id": specialist_id,
                    "name": name,
                    "description": str(incoming.get("description", "")).strip(),
                    "domain": domain,
                    "linked_agent_key": linked_agent_key,
                    "stage_hint": stage_hint,
                    "intent_keywords": intent_keywords,
                    "file_patterns": file_patterns,
                    "artifact_triggers": artifact_triggers,
                    "min_match_score": min_match_score,
                    "tool_mode": tool_mode,
                    "depth_tier": depth_tier,
                    "auto_route": bool(incoming.get("auto_route", True)),
                    "enabled": bool(incoming.get("enabled", True)),
                    "created_at": now,
                    "updated_at": now,
                }
            )
        specialists.sort(key=lambda row: (str(row.get("name", "")).lower(), str(row.get("specialist_id", ""))))
        hub["specialists"] = specialists
        data["knowledge_hub"] = hub
        self._append_audit(
            data,
            action="specialist_upserted",
            target=specialist_id,
            actor=actor,
            details={
                "name": name,
                "domain": domain,
                "linked_agent_key": linked_agent_key,
                "auto_route": bool(incoming.get("auto_route", True)),
            },
        )
        self._save(data)
        return self._sanitize(data)

    def remove_specialist(self, specialist_id: str, actor: str = "local-user") -> dict[str, Any]:
        target_id = str(specialist_id or "").strip()
        if not target_id:
            raise ValueError("specialist_id is required")
        data = self._load()
        hub = data.get("knowledge_hub", {})
        if not isinstance(hub, dict):
            hub = {}
        specialists = hub.get("specialists", [])
        if not isinstance(specialists, list):
            specialists = []
        next_rows = [
            row
            for row in specialists
            if str((row if isinstance(row, dict) else {}).get("specialist_id", "")).strip() != target_id
        ]
        if len(next_rows) == len(specialists):
            raise ValueError("specialist not found")
        hub["specialists"] = next_rows
        data["knowledge_hub"] = hub
        self._append_audit(data, action="specialist_removed", target=target_id, actor=actor, details={})
        self._save(data)
        return self._sanitize(data)

    def upsert_project_binding(self, payload: dict[str, Any], actor: str = "local-user") -> dict[str, Any]:
        incoming = payload if isinstance(payload, dict) else {}
        workspace = str(incoming.get("workspace", "")).strip() or "default-workspace"
        project = str(incoming.get("project", "")).strip() or "default-project"
        set_ids = incoming.get("knowledge_set_ids", [])
        if not isinstance(set_ids, list):
            set_ids = [part.strip() for part in str(set_ids).split(",")]
        clean_set_ids = sorted({str(item).strip() for item in set_ids if str(item).strip()})
        policy_mode = str(incoming.get("policy_mode", "balanced")).strip().lower() or "balanced"
        now = _utc_now()

        data = self._load()
        hub = data.get("knowledge_hub", {})
        if not isinstance(hub, dict):
            hub = {}
        bindings = hub.get("project_bindings", [])
        if not isinstance(bindings, list):
            bindings = []
        updated = False
        for binding in bindings:
            if not isinstance(binding, dict):
                continue
            if str(binding.get("workspace", "")).strip() != workspace:
                continue
            if str(binding.get("project", "")).strip() != project:
                continue
            binding.update(
                {
                    "knowledge_set_ids": clean_set_ids,
                    "policy_mode": policy_mode,
                    "updated_at": now,
                }
            )
            updated = True
            break
        if not updated:
            bindings.append(
                {
                    "binding_id": f"bind-{uuid.uuid4().hex[:10]}",
                    "workspace": workspace,
                    "project": project,
                    "knowledge_set_ids": clean_set_ids,
                    "policy_mode": policy_mode,
                    "created_at": now,
                    "updated_at": now,
                }
            )
        bindings.sort(key=lambda row: (str(row.get("workspace", "")), str(row.get("project", ""))))
        hub["project_bindings"] = bindings
        data["knowledge_hub"] = hub
        self._append_audit(
            data,
            action="project_binding_upserted",
            target=f"{workspace}/{project}",
            actor=actor,
            details={"knowledge_set_ids": clean_set_ids, "policy_mode": policy_mode},
        )
        self._save(data)
        return self._sanitize(data)

    @staticmethod
    def _source_snapshot(source: dict[str, Any]) -> dict[str, Any]:
        normalized = {
            "source_id": str(source.get("source_id", "")).strip(),
            "name": str(source.get("name", "")).strip(),
            "type": str(source.get("type", "")).strip().lower(),
            "scope": str(source.get("scope", "")).strip().lower(),
            "data_classification": str(source.get("data_classification", "")).strip().lower(),
            "status": str(source.get("status", "")).strip().lower(),
            "location": str(source.get("location", "")).strip(),
            "description": str(source.get("description", "")).strip(),
            "refresh_policy": str(source.get("refresh_policy", "")).strip(),
            "retention_policy": str(source.get("retention_policy", "")).strip(),
            "tags": sorted([str(item).strip() for item in source.get("tags", []) if str(item).strip()])
            if isinstance(source.get("tags", []), list)
            else [],
            "updated_at": str(source.get("updated_at", "")).strip(),
        }
        digest = _digest(normalized)
        version_id = f"srcver-{digest[:12]}"
        return {
            **normalized,
            "version_id": version_id,
            "snapshot_hash": digest,
        }

    @staticmethod
    def _set_snapshot(knowledge_set: dict[str, Any]) -> dict[str, Any]:
        normalized = {
            "set_id": str(knowledge_set.get("set_id", "")).strip(),
            "name": str(knowledge_set.get("name", "")).strip(),
            "version": str(knowledge_set.get("version", "")).strip() or "1.0.0",
            "publish_state": str(knowledge_set.get("publish_state", "")).strip().lower(),
            "scope": str(knowledge_set.get("scope", "")).strip().lower(),
            "description": str(knowledge_set.get("description", "")).strip(),
            "source_ids": sorted(
                [str(item).strip() for item in knowledge_set.get("source_ids", []) if str(item).strip()]
            )
            if isinstance(knowledge_set.get("source_ids", []), list)
            else [],
            "tags": sorted([str(item).strip() for item in knowledge_set.get("tags", []) if str(item).strip()])
            if isinstance(knowledge_set.get("tags", []), list)
            else [],
            "updated_at": str(knowledge_set.get("updated_at", "")).strip(),
        }
        digest = _digest(normalized)
        return {
            **normalized,
            "snapshot_hash": digest,
        }

    def resolve_knowledge_run_context(
        self,
        *,
        workspace: str,
        project: str,
        stage_agent_ids: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        ws = str(workspace or "").strip() or "default-workspace"
        proj = str(project or "").strip() or "default-project"
        stage_map = stage_agent_ids if isinstance(stage_agent_ids, dict) else {}
        stage_agent_keys = sorted(
            {
                str(value).strip()
                for value in stage_map.values()
                if str(value).strip()
            }
        )

        data = self._load()
        hub = data.get("knowledge_hub", {})
        if not isinstance(hub, dict):
            hub = {}
        sources = hub.get("sources", [])
        if not isinstance(sources, list):
            sources = []
        sets = hub.get("sets", [])
        if not isinstance(sets, list):
            sets = []
        brains = hub.get("agent_brains", [])
        if not isinstance(brains, list):
            brains = []
        bindings = hub.get("project_bindings", [])
        if not isinstance(bindings, list):
            bindings = []

        source_index = {
            str(row.get("source_id", "")).strip(): row
            for row in sources
            if isinstance(row, dict) and str(row.get("source_id", "")).strip()
        }
        set_index = {
            str(row.get("set_id", "")).strip(): row
            for row in sets
            if isinstance(row, dict) and str(row.get("set_id", "")).strip()
        }
        brain_index = {
            str(row.get("agent_key", "")).strip().lower(): row
            for row in brains
            if isinstance(row, dict) and str(row.get("agent_key", "")).strip()
        }

        binding: dict[str, Any] | None = None
        for row in bindings:
            if not isinstance(row, dict):
                continue
            if str(row.get("workspace", "")).strip() != ws:
                continue
            if str(row.get("project", "")).strip() != proj:
                continue
            binding = row
            break
        binding = binding or {}
        binding_set_ids = sorted(
            {
                str(item).strip()
                for item in binding.get("knowledge_set_ids", [])
                if str(item).strip()
            }
        ) if isinstance(binding.get("knowledge_set_ids", []), list) else []

        agent_policies: list[dict[str, Any]] = []
        active_set_ids: set[str] = set(binding_set_ids)
        required_citation_agents: list[str] = []
        for agent_key in stage_agent_keys:
            brain = brain_index.get(agent_key.lower(), {})
            if not isinstance(brain, dict):
                brain = {}
            brain_set_ids = sorted(
                {
                    str(item).strip()
                    for item in brain.get("knowledge_set_ids", [])
                    if str(item).strip()
                }
            ) if isinstance(brain.get("knowledge_set_ids", []), list) else []
            citation_required = bool(brain.get("citation_required", True))
            if citation_required:
                required_citation_agents.append(agent_key)
            active_set_ids.update(brain_set_ids)
            agent_policies.append(
                {
                    "agent_key": agent_key,
                    "brain_id": str(brain.get("brain_id", "")).strip(),
                    "knowledge_set_ids": brain_set_ids,
                    "top_k": int(brain.get("top_k", 8) or 8),
                    "citation_required": citation_required,
                    "fallback_behavior": str(brain.get("fallback_behavior", "ask_clarification")).strip()
                    or "ask_clarification",
                    "memory_scope": str(brain.get("memory_scope", "project")).strip() or "project",
                }
            )

        active_set_rows: list[dict[str, Any]] = []
        active_source_ids: set[str] = set()
        for set_id in sorted(active_set_ids):
            row = set_index.get(set_id)
            if not isinstance(row, dict):
                continue
            snap = self._set_snapshot(row)
            active_set_rows.append(snap)
            for source_id in snap.get("source_ids", []):
                sid = str(source_id).strip()
                if sid:
                    active_source_ids.add(sid)

        source_rows: list[dict[str, Any]] = []
        for source_id in sorted(active_source_ids):
            row = source_index.get(source_id)
            if not isinstance(row, dict):
                continue
            source_rows.append(self._source_snapshot(row))

        integrity_material = {
            "workspace": ws,
            "project": proj,
            "binding_set_ids": binding_set_ids,
            "stage_agent_keys": stage_agent_keys,
            "required_citation_agents": sorted(required_citation_agents),
            "set_hashes": [str(row.get("snapshot_hash", "")) for row in active_set_rows],
            "source_versions": [str(row.get("version_id", "")) for row in source_rows],
        }
        snapshot_hash = _digest(integrity_material)
        snapshot_id = f"kctx-{snapshot_hash[:16]}"
        return {
            "snapshot_id": snapshot_id,
            "captured_at": _utc_now(),
            "workspace": ws,
            "project": proj,
            "binding": {
                "binding_id": str(binding.get("binding_id", "")).strip(),
                "policy_mode": str(binding.get("policy_mode", "balanced")).strip().lower() or "balanced",
                "knowledge_set_ids": binding_set_ids,
            },
            "stage_agent_keys": stage_agent_keys,
            "agent_policies": agent_policies,
            "required_citation_agents": sorted(required_citation_agents),
            "sets": active_set_rows,
            "sources": source_rows,
            "integrity": {
                "snapshot_hash": snapshot_hash,
                "set_count": len(active_set_rows),
                "source_count": len(source_rows),
                "source_version_ids": [str(row.get("version_id", "")) for row in source_rows],
            },
        }
