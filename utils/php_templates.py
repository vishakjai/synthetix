from __future__ import annotations

import re
from typing import Any

TEMPLATE_SUFFIXES = ('.phtml', '.blade.php', '.twig', '.tpl', '.tpl.php', '.php')
_LITERAL_TEMPLATE_REF_RE = re.compile(
    r"(?P<call>include|include_once|require|require_once|renderFile|view|renderPage|getTemplateContent)\s*\(\s*['\"](?P<path>[^'\"]+\.(?:php|phtml|twig|tpl(?:\.php)?))['\"]",
    re.I,
)
_PROPERTY_ASSIGNMENT_REF_RE = re.compile(
    r"(?:template|view|renderfilename)\s*=\s*['\"](?P<path>[^'\"]+\.(?:php|phtml|twig|tpl(?:\.php)?))['\"]",
    re.I,
)
_VARIABLE_ASSIGNMENT_REF_RE = re.compile(
    r"(?P<name>\$[A-Za-z_][A-Za-z0-9_]*)\s*=\s*['\"](?P<path>[^'\"]+\.(?:php|phtml|twig|tpl(?:\.php)?))['\"]",
    re.I,
)
_VARIABLE_TEMPLATE_CALL_RE = re.compile(
    r"(?:include|include_once|require|require_once|renderFile|view|renderPage|getTemplateContent)\s*\(\s*(?P<name>\$[A-Za-z_][A-Za-z0-9_]*)\s*[\),]",
    re.I,
)


def _clean(value: Any) -> str:
    return str(value or '').strip()


def _looks_like_template(path: str, *, semantic_hint: str = "") -> bool:
    low = str(path or "").replace("\\", "/").lower()
    if not low.endswith(TEMPLATE_SUFFIXES):
        return False
    if semantic_hint.lower() in {"renderpage", "gettemplatecontent", "renderfile"}:
        return True
    if any(
        token in low
        for token in (
            '/view',
            '/views',
            '/template',
            '/templates',
            '/resources/views',
            '/dashboard/elements/',
            '/dashboard/',
            '/src/templates/',
            '/src/eop_email_templates/',
            '/src/onb_mail_templates/',
            '/email_templates/',
            '/mail_templates/',
        )
    ):
        return True
    if low.startswith(
        (
            'view/',
            'views/',
            'template/',
            'templates/',
            'resources/views/',
            'dashboard/elements/',
            'dashboard/',
            'src/templates/',
            'src/eop_email_templates/',
            'src/onb_mail_templates/',
            'email_templates/',
            'mail_templates/',
        )
    ):
        return True
    name = low.rsplit('/', 1)[-1]
    return (
        name.startswith('view')
        or 'template' in name
        or name.startswith('extern_')
        or name.startswith('report')
        or name.endswith('_html.php')
    )


def extract_php_template_inventory(file_map: dict[str, str], entries: list[dict[str, Any]] | None = None) -> dict[str, Any]:
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

    for row in entries or []:
        if not isinstance(row, dict):
            continue
        normalized = _clean(row.get('path')).replace('\\', '/')
        if _looks_like_template(normalized):
            add_template(normalized, inferred=True)

    for path, body in (file_map or {}).items():
        normalized = str(path).replace('\\', '/')
        if _looks_like_template(normalized):
            add_template(normalized)
        if not isinstance(body, str):
            continue
        variable_templates: dict[str, str] = {}
        for match in _VARIABLE_ASSIGNMENT_REF_RE.finditer(body):
            ref = _clean(match.group('path'))
            if _looks_like_template(ref):
                variable_templates[_clean(match.group('name'))] = ref
                add_template(ref, source_file=normalized, inferred=True)
        for match in _PROPERTY_ASSIGNMENT_REF_RE.finditer(body):
            ref = _clean(match.group('path'))
            if _looks_like_template(ref):
                add_template(ref, source_file=normalized, inferred=True)
        for match in _LITERAL_TEMPLATE_REF_RE.finditer(body):
            ref = _clean(match.group('path'))
            semantic_hint = _clean(match.group('call'))
            if _looks_like_template(ref, semantic_hint=semantic_hint):
                add_template(ref, source_file=normalized, inferred=True)
        for match in _VARIABLE_TEMPLATE_CALL_RE.finditer(body):
            ref = variable_templates.get(_clean(match.group('name')))
            if ref:
                add_template(ref, source_file=normalized, inferred=True)
    return {
        'artifact_type': 'php_template_inventory_v1',
        'template_count': len(templates),
        'templates': templates[:2000],
    }
