from __future__ import annotations

import re
from typing import Any

_SQL_STRING_RE = re.compile(r"(['\"])(?P<sql>\s*(?:select|insert|update|delete)\b.*?)(?:\1)", re.I | re.S)
_TABLE_RE = re.compile(r"\b(?:from|join|into|update)\s+([A-Za-z_][A-Za-z0-9_\.]*)", re.I)


def _clean(value: Any) -> str:
    return str(value or '').strip()


def extract_php_sql_catalog(file_map: dict[str, str]) -> dict[str, Any]:
    statements: list[dict[str, Any]] = []
    connection_hints: list[str] = []
    seen_sql: set[str] = set()
    for path, body in (file_map or {}).items():
        if not isinstance(body, str):
            continue
        normalized = str(path).replace('\\', '/')
        low_body = body.lower()
        if any(token in low_body for token in ('pdo', 'mysqli', 'mysql:', 'sqlsrv:', 'pgsql:', 'oci_connect')):
            connection_hints.append(normalized)
        for match in _SQL_STRING_RE.finditer(body):
            raw = _clean(match.group('sql'))
            if not raw:
                continue
            normalized_sql = ' '.join(raw.split())
            dedupe = normalized_sql.lower()
            if dedupe in seen_sql:
                continue
            seen_sql.add(dedupe)
            tables = []
            for table in _TABLE_RE.findall(raw):
                token = _clean(table)
                if token and token not in tables:
                    tables.append(token)
            kind = raw.split(None, 1)[0].upper() if raw.split() else 'SQL'
            risk_flags = []
            if '.$' in raw or '.=' in body or '" .' in body or "' ." in body:
                risk_flags.append('string_concatenation')
            if '?' not in raw and ':' not in raw and any(
                op in low_body for op in ('query(', 'exec(', 'mysqli_query', 'sqlsrv_query')
            ):
                risk_flags.append('possible_unparameterized_query')
            statements.append(
                {
                    'sql_id': f'php_sql:{len(statements)+1}',
                    'kind': kind,
                    'raw': normalized_sql[:2000],
                    'tables': tables,
                    'source_file': normalized,
                    'risk_flags': risk_flags,
                }
            )
    return {
        'artifact_type': 'php_sql_catalog_v1',
        'statement_count': len(statements),
        'connection_hint_files': sorted(set(connection_hints))[:120],
        'statements': statements[:800],
    }
