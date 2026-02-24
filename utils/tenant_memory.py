"""
Tenant-isolated memory store for agent conversations and persistent constraints.
"""

from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_segment(value: str, default: str) -> str:
    raw = str(value or "").strip().lower()
    cleaned = re.sub(r"[^a-z0-9._-]+", "-", raw).strip("-")
    return cleaned or default


def _tokens(text: str) -> set[str]:
    return {token for token in re.findall(r"[a-z0-9_]+", str(text or "").lower()) if token}


def _safe_json_load(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def _safe_json_write(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=True, default=str))
    tmp.replace(path)


@dataclass
class TenantMemoryStore:
    root_dir: str

    def __post_init__(self) -> None:
        self.root = Path(self.root_dir)
        self.root.mkdir(parents=True, exist_ok=True)

    def _memory_path(self, scope: dict[str, Any]) -> Path:
        workspace = _safe_segment(str(scope.get("workspace_id", "")), "default-workspace")
        client = _safe_segment(str(scope.get("client_id", "")), "default-client")
        project = _safe_segment(str(scope.get("project_id", "")), "default-project")
        return self.root / workspace / client / project / "memory.json"

    def _load(self, scope: dict[str, Any]) -> dict[str, Any]:
        path = self._memory_path(scope)
        data = _safe_json_load(path)
        if not data:
            data = {
                "scope": {
                    "workspace_id": str(scope.get("workspace_id", "default-workspace")),
                    "client_id": str(scope.get("client_id", "default-client")),
                    "project_id": str(scope.get("project_id", "default-project")),
                },
                "updated_at": _utc_now(),
                "threads": {},
                "constraints": [],
            }
        if not isinstance(data.get("threads", {}), dict):
            data["threads"] = {}
        if not isinstance(data.get("constraints", []), list):
            data["constraints"] = []
        return data

    def _save(self, scope: dict[str, Any], data: dict[str, Any]) -> None:
        data["updated_at"] = _utc_now()
        path = self._memory_path(scope)
        _safe_json_write(path, data)

    def append_thread_message(
        self,
        scope: dict[str, Any],
        *,
        thread_id: str,
        agent_role: str,
        role: str,
        message: str,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        tid = str(thread_id or "").strip() or "default-thread"
        msg = str(message or "").strip()
        if not msg:
            raise ValueError("message is required")
        data = self._load(scope)
        threads = data.get("threads", {})
        thread = threads.get(tid, {})
        if not isinstance(thread, dict):
            thread = {}
        entries = thread.get("messages", [])
        if not isinstance(entries, list):
            entries = []
        row = {
            "id": f"memmsg-{uuid.uuid4().hex[:12]}",
            "timestamp": _utc_now(),
            "agent_role": str(agent_role or "").strip() or "analyst",
            "role": str(role or "").strip() or "user",
            "message": msg,
            "metadata": metadata if isinstance(metadata, dict) else {},
        }
        entries.append(row)
        thread["messages"] = entries[-300:]
        threads[tid] = thread
        data["threads"] = threads
        self._save(scope, data)
        return row

    def get_thread(self, scope: dict[str, Any], thread_id: str, limit: int = 40) -> list[dict[str, Any]]:
        tid = str(thread_id or "").strip() or "default-thread"
        data = self._load(scope)
        threads = data.get("threads", {})
        thread = threads.get(tid, {}) if isinstance(threads, dict) else {}
        messages = thread.get("messages", []) if isinstance(thread, dict) else []
        if not isinstance(messages, list):
            return []
        return [row for row in messages[-max(1, int(limit)):] if isinstance(row, dict)]

    def add_constraint(
        self,
        scope: dict[str, Any],
        *,
        text: str,
        source: str,
        created_by: str,
        priority: str = "medium",
        applies_to: str = "all",
    ) -> dict[str, Any]:
        body = str(text or "").strip()
        if not body:
            raise ValueError("constraint text is required")
        p = str(priority or "medium").strip().lower()
        if p not in {"low", "medium", "high", "critical"}:
            p = "medium"
        row = {
            "id": f"constraint-{uuid.uuid4().hex[:10]}",
            "text": body,
            "priority": p,
            "source": str(source or "manual"),
            "created_by": str(created_by or "local-user"),
            "created_at": _utc_now(),
            "status": "active",
            "applies_to": str(applies_to or "all"),
        }
        data = self._load(scope)
        constraints = data.get("constraints", [])
        if not isinstance(constraints, list):
            constraints = []
        constraints.insert(0, row)
        data["constraints"] = constraints[:800]
        self._save(scope, data)
        return row

    def search_constraints(self, scope: dict[str, Any], query: str, limit: int = 8) -> list[dict[str, Any]]:
        tokens = _tokens(query)
        data = self._load(scope)
        constraints = data.get("constraints", [])
        if not isinstance(constraints, list):
            return []
        scored: list[tuple[float, dict[str, Any]]] = []
        for row in constraints:
            if not isinstance(row, dict):
                continue
            if str(row.get("status", "active")).strip().lower() != "active":
                continue
            row_tokens = _tokens(str(row.get("text", "")))
            if not row_tokens:
                continue
            overlap = len(tokens.intersection(row_tokens)) if tokens else 0
            if tokens and overlap <= 0:
                continue
            score = overlap / max(1, len(tokens)) if tokens else 0.5
            scored.append((score, row))
        scored.sort(key=lambda item: item[0], reverse=True)
        return [row for _, row in scored[: max(1, int(limit))]]
