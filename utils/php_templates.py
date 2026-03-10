from __future__ import annotations

from typing import Any

TEMPLATE_SUFFIXES = ('.phtml', '.blade.php', '.twig', '.tpl', '.tpl.php', '.php')


def _clean(value: Any) -> str:
    return str(value or '').strip()


def extract_php_template_inventory(file_map: dict[str, str]) -> dict[str, Any]:
    templates: list[dict[str, Any]] = []
    for path in (file_map or {}).keys():
        normalized = str(path).replace('\\', '/')
        low = normalized.lower()
        in_template_dir = (
            any(token in low for token in ('/view', '/views', '/template', '/templates', '/resources/views'))
            or low.startswith('view/')
            or low.startswith('views/')
            or low.startswith('template/')
            or low.startswith('templates/')
            or low.startswith('resources/views/')
        )
        is_template = low.endswith(TEMPLATE_SUFFIXES) and in_template_dir
        if not is_template:
            continue
        engine = 'blade' if low.endswith('.blade.php') else 'twig' if low.endswith('.twig') else 'php'
        templates.append(
            {
                'template_id': f'template:{len(templates)+1}',
                'name': normalized.rsplit('/', 1)[-1],
                'path': normalized,
                'engine': engine,
            }
        )
    return {
        'artifact_type': 'php_template_inventory_v1',
        'template_count': len(templates),
        'templates': templates[:500],
    }
