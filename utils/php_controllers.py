from __future__ import annotations

import re
from pathlib import PurePosixPath
from typing import Any

_CLASS_RE = re.compile(r"class\s+([A-Za-z_][A-Za-z0-9_]*)", re.I)
_METHOD_RE = re.compile(r"(?:public|protected)?\s*function\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(", re.I)


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _is_controller_path(path: str) -> bool:
    low = str(path or "").replace("\\", "/").lower()
    return bool(
        low.endswith(".php")
        and (
            "controller" in low
            or PurePosixPath(low).name.endswith("controller.php")
        )
    )


def extract_php_controller_inventory(file_map: dict[str, str], entries: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    controllers: list[dict[str, Any]] = []
    seen_paths: set[str] = set()
    methods_total = 0

    for row in entries or []:
        if not isinstance(row, dict):
            continue
        normalized = _clean(row.get("path")).replace("\\", "/")
        if not _is_controller_path(normalized):
            continue
        class_name = normalized.rsplit("/", 1)[-1].rsplit(".", 1)[0]
        controllers.append(
            {
                "controller_id": f"controller:{len(controllers)+1}",
                "name": class_name,
                "path": normalized,
                "action_count": 0,
                "actions": [],
                "inferred": True,
            }
        )
        seen_paths.add(normalized)

    for path, body in (file_map or {}).items():
        if not isinstance(body, str):
            continue
        normalized = path.replace("\\", "/")
        if not _is_controller_path(normalized):
            continue
        class_name = _clean(next(iter(_CLASS_RE.findall(body)), "")) or normalized.rsplit("/", 1)[-1].rsplit(".", 1)[0]
        methods = []
        for name in _METHOD_RE.findall(body):
            method = _clean(name)
            if method and method.lower() != "__construct" and method not in methods:
                methods.append(method)
        methods_total += len(methods)
        row = (
            next((item for item in controllers if str(item.get("path")) == normalized), None)
            if normalized in seen_paths
            else None
        )
        payload = {
            "controller_id": str(row.get("controller_id")) if isinstance(row, dict) else f"controller:{len(controllers)+1}",
            "name": class_name,
            "path": normalized,
            "action_count": len(methods),
            "actions": methods[:80],
        }
        if row is not None:
            row.update(payload)
            row.pop("inferred", None)
        else:
            controllers.append(payload)
            seen_paths.add(normalized)
    return {
        "artifact_type": "php_controller_inventory_v1",
        "controller_count": len(controllers),
        "action_count": methods_total,
        "controllers": controllers[:500],
    }
