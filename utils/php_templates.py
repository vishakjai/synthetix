from __future__ import annotations

import re
from typing import Any

TEMPLATE_SUFFIXES = ('.phtml', '.blade.php', '.twig', '.tpl', '.tpl.php', '.php')
_TEMPLATE_REF_RE = re.compile(
    r"(?:include|include_once|require|require_once|renderFile|view)\s*\(\s*['\"](?P<path>[^'\"]+\.(?:php|phtml|twig|tpl(?:\.php)?))['\"]",
    re.I,
)
_ASSIGNMENT_REF_RE = re.compile(
    r"(?:template|view)\s*=\s*['\"](?P<path>[^'\"]+\.(?:php|phtml|twig|tpl(?:\.php)?))['\"]",
    re.I,
)


def _clean(value: Any) -> str:
    return str(value or '').strip()


def _looks_like_template(path: str) -> bool:
    low = str(path or "").replace("\\", "/").lower()
    if not low.endswith(TEMPLATE_SUFFIXES):
        return False
    if any(token in low for token in ('/view', '/views', '/template', '/templates', '/resources/views', '/dashboard/elements/')):
        return True
    if low.startswith(('view/', 'views/', 'template/', 'templates/', 'resources/views/', 'dashboard/elements/')):
        return True
    name = low.rsplit('/', 1)[-1]
    return name.startswith('view') or 'template' in name


def extract_php_template_inventory(file_map: dict[str, str]) -> dict[str, Any]:
    templates: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add_template(path: str, *, source_file: str = "", inferred: bool = False) -> None:
        normalized = _clean(path).replace('\\', '/')
        if not normalized or normalized in seen:
            return
        low = normalized.lower()
        engine = 'blade' if low.endswith('.blade.php') else 'twig' if low.endswith('.twig') else 'php'
        templates.append(
            {
                'template_id': f'template:{len(templates)+1}',
                'name': normalized.rsplit('/', 1)[-1],
                'path': normalized,
                'engine': engine,
                **({'source_file': source_file} if source_file else {}),
                **({'inferred': True} if inferred else {}),
            }
        )
        seen.add(normalized)

    for path, body in (file_map or {}).items():
        normalized = str(path).replace('\\', '/')
        if _looks_like_template(normalized):
            add_template(normalized)
        if not isinstance(body, str):
            continue
        for pattern in (_TEMPLATE_REF_RE, _ASSIGNMENT_REF_RE):
            for match in pattern.finditer(body):
                ref = _clean(match.group('path'))
                if _looks_like_template(ref):
                    add_template(ref, source_file=normalized, inferred=True)
    return {
        'artifact_type': 'php_template_inventory_v1',
        'template_count': len(templates),
        'templates': templates[:500],
    }
