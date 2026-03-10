from __future__ import annotations

import re
from typing import Any

_CLASS_RE = re.compile(r"class\s+([A-Za-z_][A-Za-z0-9_]*)", re.I)
_METHOD_RE = re.compile(r"(?:public|protected)?\s*function\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(", re.I)


def _clean(value: Any) -> str:
    return str(value or "").strip()


def extract_php_controller_inventory(file_map: dict[str, str]) -> dict[str, Any]:
    controllers: list[dict[str, Any]] = []
    methods_total = 0
    for path, body in (file_map or {}).items():
        if not isinstance(body, str):
            continue
        normalized = path.replace("\\", "/")
        low = normalized.lower()
        if not low.endswith(".php"):
            continue
        if not (
            "controller" in low
            or normalized.rsplit("/", 1)[-1].lower().endswith("controller.php")
        ):
            continue
        class_name = _clean(next(iter(_CLASS_RE.findall(body)), "")) or normalized.rsplit("/", 1)[-1].rsplit(".", 1)[0]
        methods = []
        for name in _METHOD_RE.findall(body):
            method = _clean(name)
            if method and method.lower() != "__construct" and method not in methods:
                methods.append(method)
        methods_total += len(methods)
        controllers.append(
            {
                "controller_id": f"controller:{len(controllers)+1}",
                "name": class_name,
                "path": normalized,
                "action_count": len(methods),
                "actions": methods[:80],
            }
        )
    return {
        "artifact_type": "php_controller_inventory_v1",
        "controller_count": len(controllers),
        "action_count": methods_total,
        "controllers": controllers[:300],
    }
