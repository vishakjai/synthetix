"""
Versioned persona registry for agent system prompts.

Personas are stored as JSON files in a git-tracked directory so prompt changes
are reviewable and auditable.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _safe_json_load(path: Path) -> dict[str, Any] | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError:
        return None
    return data if isinstance(data, dict) else None


def _safe_json_write(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=True))
    tmp.replace(path)


def _sort_version(version: str) -> tuple[int, int, int, str]:
    raw = str(version or "").strip().lower().lstrip("v")
    parts = raw.split(".")
    nums: list[int] = []
    for idx in range(3):
        token = parts[idx] if idx < len(parts) else "0"
        try:
            nums.append(max(0, int(token)))
        except ValueError:
            nums.append(0)
    return nums[0], nums[1], nums[2], raw


class PersonaRegistry:
    def __init__(self, root_dir: str):
        self.root = Path(root_dir)
        self.root.mkdir(parents=True, exist_ok=True)

    def _iter_persona_files(self) -> list[Path]:
        files = [path for path in self.root.glob("*.json") if path.is_file()]
        files.sort(key=lambda p: p.name.lower())
        return files

    @staticmethod
    def _normalize(payload: dict[str, Any]) -> dict[str, Any]:
        data = dict(payload)
        data["id"] = str(data.get("id", "")).strip().lower()
        data["version"] = str(data.get("version", "")).strip() or "1.0.0"
        data["name"] = str(data.get("name", data["id"] or "Persona")).strip() or "Persona"
        data["role"] = str(data.get("role", "analyst")).strip().lower() or "analyst"
        tags = data.get("tags", [])
        data["tags"] = [str(x).strip() for x in tags if str(x).strip()] if isinstance(tags, list) else []
        data["system_prompt"] = str(data.get("system_prompt", "")).strip()
        output_contract = data.get("output_contract", {})
        data["output_contract"] = output_contract if isinstance(output_contract, dict) else {}
        defaults = data.get("defaults", {})
        data["defaults"] = defaults if isinstance(defaults, dict) else {}
        return data

    def list_personas(self, role: str = "") -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        target_role = str(role or "").strip().lower()
        for path in self._iter_persona_files():
            item = _safe_json_load(path)
            if not item:
                continue
            row = self._normalize(item)
            if target_role and row.get("role") != target_role:
                continue
            row["source_file"] = path.name
            rows.append(row)
        rows.sort(key=lambda row: (str(row.get("id", "")), _sort_version(str(row.get("version", ""))), str(row.get("name", ""))))
        return rows

    def get_persona(self, persona_id: str, version: str = "") -> dict[str, Any] | None:
        target_id = str(persona_id or "").strip().lower()
        if not target_id:
            return None
        rows = [row for row in self.list_personas() if str(row.get("id", "")).strip().lower() == target_id]
        if not rows:
            return None
        if version:
            target_version = str(version).strip().lower().lstrip("v")
            for row in rows:
                if str(row.get("version", "")).strip().lower().lstrip("v") == target_version:
                    return row
        rows.sort(key=lambda row: _sort_version(str(row.get("version", ""))), reverse=True)
        return rows[0]

    def upsert_persona(self, payload: dict[str, Any]) -> dict[str, Any]:
        data = self._normalize(payload if isinstance(payload, dict) else {})
        persona_id = str(data.get("id", "")).strip().lower()
        if not persona_id:
            raise ValueError("persona id is required")
        if not data.get("system_prompt"):
            raise ValueError("system_prompt is required")
        version = str(data.get("version", "1.0.0")).strip() or "1.0.0"
        filename = f"{persona_id}.v{version}.json"
        _safe_json_write(self.root / filename, data)
        saved = self.get_persona(persona_id, version=version)
        if not saved:
            raise ValueError("failed to persist persona")
        return saved
