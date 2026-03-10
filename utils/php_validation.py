from __future__ import annotations

import re
from typing import Any

_VALIDATION_CALL_RE = re.compile(
    r"\b(?:filter_var|preg_match|is_numeric|ctype_digit|ctype_alnum|strlen|empty|isset)\s*\(",
    re.I,
)


def _clean(value: Any) -> str:
    return str(value or "").strip()


def extract_php_validation_rules(file_map: dict[str, str]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for path, body in (file_map or {}).items():
        if not isinstance(body, str):
            continue
        normalized = str(path).replace("\\", "/")
        count = len(_VALIDATION_CALL_RE.findall(body))
        if count <= 0:
            continue
        rows.append(
            {
                "path": normalized,
                "validation_signal_count": count,
                "uses_required_checks": "empty(" in body or "isset(" in body,
                "uses_regex_checks": "preg_match(" in body,
                "uses_filter_var": "filter_var(" in body,
            }
        )
    return {
        "artifact_type": "php_validation_rules_v1",
        "file_count": len(rows),
        "entries": rows[:300],
    }
