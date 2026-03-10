from __future__ import annotations

from typing import Any

_AUTH_TOKENS = ('login', 'logout', 'signin', 'signout', 'authenticate', 'auth', 'permission', 'role', 'is_admin')


def _clean(value: Any) -> str:
    return str(value or '').strip()


def extract_php_auth_inventory(file_map: dict[str, str]) -> dict[str, Any]:
    evidence: list[dict[str, Any]] = []
    for path, body in (file_map or {}).items():
        if not isinstance(body, str):
            continue
        normalized = str(path).replace('\\', '/')
        low = body.lower()
        hits = [token for token in _AUTH_TOKENS if token in low]
        if not hits and '$_session' not in low:
            continue
        evidence.append(
            {
                'path': normalized,
                'signals': sorted(set(hits + (['session_guard'] if '$_session' in low and 'user' in low else []))),
            }
        )
    return {
        'artifact_type': 'php_authz_authn_inventory_v1',
        'auth_file_count': len(evidence),
        'evidence': evidence[:240],
    }
