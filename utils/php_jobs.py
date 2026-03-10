from __future__ import annotations

from typing import Any


def extract_php_background_job_inventory(file_map: dict[str, str]) -> dict[str, Any]:
    jobs: list[dict[str, Any]] = []
    for path, body in (file_map or {}).items():
        if not isinstance(body, str):
            continue
        normalized = str(path).replace('\\', '/')
        low = normalized.lower()
        in_job_path = (
            any(token in low for token in ('/cron', '/jobs', '/commands', '/console', '/cli/'))
            or low.startswith('cron/')
            or low.startswith('jobs/')
            or low.startswith('commands/')
            or low.startswith('console/')
            or low.startswith('cli/')
        )
        if in_job_path or '$argv' in body or 'schedule(' in body.lower():
            jobs.append({'job_id': f'job:{len(jobs)+1}', 'path': normalized})
    return {
        'artifact_type': 'php_background_job_inventory_v1',
        'job_count': len(jobs),
        'jobs': jobs[:240],
    }
