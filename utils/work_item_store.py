from __future__ import annotations

import json
import threading
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_json_load(path: Path, default: Any) -> Any:
    if not path.exists():
        return deepcopy(default)
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return deepcopy(default)


def _safe_json_write(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=True, default=str))
    tmp.replace(path)


class WorkItemStore:
    def __init__(self, root_dir: str):
        self.root = Path(root_dir)
        self.path = self.root / "work_items.json"
        self._lock = threading.Lock()
        if not self.path.exists():
            _safe_json_write(self.path, [])

    def _load(self) -> list[dict[str, Any]]:
        data = _safe_json_load(self.path, [])
        if not isinstance(data, list):
            return []
        out: list[dict[str, Any]] = []
        for row in data:
            if isinstance(row, dict):
                out.append(row)
        return out

    def _save(self, rows: list[dict[str, Any]]) -> None:
        _safe_json_write(self.path, rows)

    def list_items(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._load()
        rows.sort(key=lambda x: str(x.get("updated_at", "")), reverse=True)
        return rows

    def create_item(self, payload: dict[str, Any], actor: str = "local-user") -> dict[str, Any]:
        incoming = payload if isinstance(payload, dict) else {}
        now = _utc_now()
        row = {
            "id": f"wi-{uuid.uuid4().hex[:10]}",
            "title": str(incoming.get("title", "")).strip(),
            "description": str(incoming.get("description", "")).strip(),
            "type": str(incoming.get("type", "task")).strip().lower() or "task",
            "recommended_type": str(incoming.get("recommended_type", "task")).strip().lower() or "task",
            "status": str(incoming.get("status", "open")).strip().lower() or "open",
            "governance_tier": str(incoming.get("governance_tier", "standard")).strip().lower() or "standard",
            "risk_tier": str(incoming.get("risk_tier", "medium")).strip().lower() or "medium",
            "complexity_score": float(incoming.get("complexity_score", 0) or 0),
            "blast_radius": int(incoming.get("blast_radius", 0) or 0),
            "linked_issue": str(incoming.get("linked_issue", "")).strip(),
            "run_id": str(incoming.get("run_id", "")).strip(),
            "source": str(incoming.get("source", "manual")).strip() or "manual",
            "created_at": now,
            "updated_at": now,
            "created_by": actor,
        }
        with self._lock:
            rows = self._load()
            rows.append(row)
            self._save(rows)
        return row

    def set_status(self, item_id: str, status: str, actor: str = "local-user") -> dict[str, Any] | None:
        normalized = str(status or "").strip().lower()
        if not normalized:
            return None
        with self._lock:
            rows = self._load()
            for row in rows:
                if str(row.get("id", "")) != str(item_id):
                    continue
                row["status"] = normalized
                row["updated_at"] = _utc_now()
                row["updated_by"] = actor
                self._save(rows)
                return row
        return None
