from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from utils.legacy_skills import extract_vb6_signals


PROC_RE = re.compile(
    r"(?im)^\s*(Public|Private|Friend|Protected)?\s*(Sub|Function|Property Get|Property Let|Property Set)\s+([A-Za-z_][A-Za-z0-9_]*)\b"
)


def build_symbol_index_v1(
    *,
    snapshot_id: str,
    file_contents: dict[str, str],
) -> dict[str, Any]:
    symbols: list[dict[str, Any]] = []
    counts_by_kind: dict[str, int] = {}
    seen: set[tuple[str, str, str]] = set()
    for path, body in sorted((file_contents or {}).items()):
        source_file = str(path or "").replace("\\", "/").strip()
        if not source_file:
            continue
        text = str(body or "")
        sig = extract_vb6_signals(source_file, text)
        form_tokens = sig.get("forms", []) if isinstance(sig, dict) and isinstance(sig.get("forms", []), list) else []
        project_def = sig.get("project_definition", {}) if isinstance(sig, dict) and isinstance(sig.get("project_definition", {}), dict) else {}

        for form_token in form_tokens:
            token = str(form_token or "").strip()
            if ":" not in token:
                continue
            kind, name = token.split(":", 1)
            _append_symbol(
                symbols,
                counts_by_kind,
                seen,
                symbol_kind=str(kind or "").lower() or "form",
                name=name.strip(),
                source_file=source_file,
                container=str(project_def.get("project_name", "")).strip(),
                line=0,
                confidence=0.95,
            )

        for scope, proc_type, name in PROC_RE.findall(text):
            _append_symbol(
                symbols,
                counts_by_kind,
                seen,
                symbol_kind="procedure",
                name=str(name or "").strip(),
                source_file=source_file,
                container=Path(source_file).stem,
                line=_find_line(text, name),
                signature=f"{str(scope or '').strip()} {str(proc_type or '').strip()}".strip(),
                confidence=0.9,
            )

        globals_rows = sig.get("module_global_declarations", []) if isinstance(sig, dict) and isinstance(sig.get("module_global_declarations", []), list) else []
        for row in globals_rows:
            if not isinstance(row, dict):
                continue
            _append_symbol(
                symbols,
                counts_by_kind,
                seen,
                symbol_kind="global",
                name=str(row.get("symbol", "")).strip(),
                source_file=source_file,
                container=Path(source_file).stem,
                line=int(row.get("line", 0) or 0),
                declared_type=str(row.get("declared_type", "")).strip(),
                scope=str(row.get("scope", "")).strip(),
                confidence=0.85,
            )

        if project_def:
            _append_symbol(
                symbols,
                counts_by_kind,
                seen,
                symbol_kind="project",
                name=str(project_def.get("project_name", "")).strip() or Path(source_file).stem,
                source_file=source_file,
                container="",
                line=0,
                project_type=str(project_def.get("project_type", "")).strip(),
                startup_object=str(project_def.get("startup_object", "")).strip(),
                confidence=0.98,
            )

    return {
        "artifact_type": "symbol_index_v1",
        "snapshot_id": snapshot_id,
        "symbol_count": len(symbols),
        "counts_by_kind": dict(sorted(counts_by_kind.items())),
        "symbols": symbols[:8000],
    }


def _append_symbol(
    symbols: list[dict[str, Any]],
    counts_by_kind: dict[str, int],
    seen: set[tuple[str, str, str]],
    *,
    symbol_kind: str,
    name: str,
    source_file: str,
    container: str,
    line: int,
    confidence: float,
    **extra: Any,
) -> None:
    clean_name = str(name or "").strip()
    if not clean_name:
        return
    key = (str(symbol_kind or "").strip().lower(), clean_name.lower(), str(source_file or "").strip().lower())
    if key in seen:
        return
    seen.add(key)
    counts_by_kind[str(symbol_kind)] = int(counts_by_kind.get(str(symbol_kind), 0) or 0) + 1
    row = {
        "symbol_kind": str(symbol_kind),
        "name": clean_name,
        "source_file": str(source_file or "").strip(),
        "container": str(container or "").strip(),
        "line": int(line or 0),
        "confidence": float(confidence or 0.0),
    }
    for key, value in extra.items():
        if value not in {None, ""}:
            row[str(key)] = value
    symbols.append(row)


def _find_line(text: str, name: str) -> int:
    target = str(name or "").strip()
    if not target:
        return 0
    for idx, raw in enumerate(str(text or "").splitlines(), start=1):
        if re.search(rf"(?i)\b{re.escape(target)}\b", str(raw or "")):
            return idx
    return 0
