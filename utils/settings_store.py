"""
Persistent settings store for integrations, policy packs, and RBAC.
"""

from __future__ import annotations

import copy
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


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


def _mask_secret(secret: str) -> str:
    raw = str(secret or "").strip()
    if not raw:
        return ""
    if len(raw) <= 4:
        return "*" * len(raw)
    return ("*" * max(4, len(raw) - 4)) + raw[-4:]


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
    return {
        "version": 1,
        "updated_at": _utc_now(),
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
        "audit_log": [],
    }


@dataclass
class SettingsStore:
    root_dir: str

    def __post_init__(self) -> None:
        self.root = Path(self.root_dir)
        self.root.mkdir(parents=True, exist_ok=True)
        self.path = self.root / "settings_state.json"
        if not self.path.exists():
            _safe_json_write(self.path, _default_settings())

    def _load(self) -> dict[str, Any]:
        data = _safe_json_load(self.path, _default_settings())
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

        data.setdefault("audit_log", [])
        if not isinstance(data["audit_log"], list):
            data["audit_log"] = []
        return data

    def _save(self, data: dict[str, Any]) -> None:
        data["updated_at"] = _utc_now()
        _safe_json_write(self.path, data)

    @staticmethod
    def _sanitize(data: dict[str, Any]) -> dict[str, Any]:
        payload = copy.deepcopy(data)
        integrations = payload.get("integrations", {})
        if isinstance(integrations, dict):
            for provider, secret_key in INTEGRATION_SECRET_KEYS.items():
                integration = integrations.get(provider, {})
                if not isinstance(integration, dict):
                    continue
                raw = str(integration.get(secret_key, "")).strip()
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
                    raw = str(cfg.get("api_key", "")).strip()
                    cfg["api_key_masked"] = _mask_secret(raw)
                    cfg["has_secret"] = bool(raw)
                    cfg["api_key"] = ""
        payload["meta"] = {
            "integration_providers": sorted(INTEGRATION_KEYS),
            "llm_providers": sorted(LLM_PROVIDER_KEYS),
            "policy_packs": sorted(POLICY_PACKS),
            "rbac_roles": sorted(RBAC_ROLES),
            "rbac_permissions": sorted(RBAC_PERMISSIONS),
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
        return copy.deepcopy(integration)

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

        has_secret = bool(str(integration.get(secret_key, "")).strip())
        missing = [field for field in required if not str(integration.get(field, "")).strip()]

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
        for field in required:
            checks.append(
                {
                    "name": field,
                    "ok": bool(str(integration.get(field, "")).strip()),
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
            secret_key = INTEGRATION_SECRET_KEYS[key]
            integration[secret_key] = ""
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
        return copy.deepcopy(cfg)

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
                cfg["api_key"] = secret

        if bool(incoming.get("set_default_provider", False)):
            llm["default_provider"] = key

        has_secret = bool(str(cfg.get("api_key", "")).strip())
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
                "ok": bool(str(cfg.get("api_key", "")).strip()),
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
            cfg["api_key"] = ""
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
