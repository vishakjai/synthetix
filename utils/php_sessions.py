from __future__ import annotations

import re
from typing import Any

_SESSION_KEY_RE = re.compile(r"\$_SESSION\s*\[\s*['\"]([^'\"]+)['\"]\s*\]")
_SUPERGLOBALS = ('$_GET', '$_POST', '$_REQUEST', '$_COOKIE', '$_FILES', '$_SESSION')


def _clean(value: Any) -> str:
    return str(value or '').strip()


def extract_php_session_state_inventory(file_map: dict[str, str]) -> dict[str, Any]:
    session_keys: set[str] = set()
    superglobal_usage: dict[str, int] = {key: 0 for key in _SUPERGLOBALS}
    session_start_files: list[str] = []
    for path, body in (file_map or {}).items():
        if not isinstance(body, str):
            continue
        normalized = str(path).replace('\\', '/')
        if 'session_start' in body:
            session_start_files.append(normalized)
        for key in _SESSION_KEY_RE.findall(body):
            token = _clean(key)
            if token:
                session_keys.add(token)
        for name in _SUPERGLOBALS:
            superglobal_usage[name] += body.count(name)
    return {
        'artifact_type': 'php_session_state_inventory_v1',
        'uses_session_state': bool(session_start_files or session_keys),
        'session_start_files': sorted(set(session_start_files))[:120],
        'session_key_count': len(session_keys),
        'session_keys': sorted(session_keys)[:200],
        'superglobal_usage': superglobal_usage,
    }
