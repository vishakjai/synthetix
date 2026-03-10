from __future__ import annotations

from typing import Any


def extract_php_file_io_inventory(file_map: dict[str, str]) -> dict[str, Any]:
    uploads: list[str] = []
    exports: list[str] = []
    for path, body in (file_map or {}).items():
        if not isinstance(body, str):
            continue
        normalized = str(path).replace('\\', '/')
        low = body.lower()
        if '$_files' in low or 'move_uploaded_file' in low:
            uploads.append(normalized)
        if 'fputcsv' in low or 'content-disposition' in low or 'readfile(' in low or 'dompdf' in low or 'tcpdf' in low or 'fpdf' in low:
            exports.append(normalized)
    return {
        'artifact_type': 'php_file_io_inventory_v1',
        'upload_file_count': len(set(uploads)),
        'export_file_count': len(set(exports)),
        'upload_files': sorted(set(uploads))[:120],
        'export_files': sorted(set(exports))[:120],
    }
