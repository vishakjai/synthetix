from __future__ import annotations

import re
from typing import Any

_INCLUDE_RE = re.compile(r"\b(?:include|include_once|require|require_once)\s*(?:\(|)\s*['\"]([^'\"]+)['\"]", re.I)


def _clean(value: Any) -> str:
    return str(value or '').strip()


def extract_php_include_graph(file_map: dict[str, str]) -> dict[str, Any]:
    edges: list[dict[str, Any]] = []
    for path, body in (file_map or {}).items():
        if not isinstance(body, str):
            continue
        normalized = str(path).replace('\\', '/')
        for inc in _INCLUDE_RE.findall(body):
            target = _clean(inc)
            if target:
                edges.append({'source': normalized, 'target': target, 'type': 'include'})
    return {
        'artifact_type': 'php_include_graph_v1',
        'edge_count': len(edges),
        'edges': edges[:1200],
    }
