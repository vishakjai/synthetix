from __future__ import annotations

from pathlib import Path
from typing import Any


CHUNKABLE_EXTENSIONS = {".frm", ".ctl", ".bas", ".cls", ".vbp", ".vbg", ".php", ".py", ".js", ".ts", ".tsx", ".java", ".cs", ".go", ".sql"}


def build_file_chunk_manifest_v1(
    *,
    snapshot_id: str,
    file_contents: dict[str, str],
    file_fetch_meta: dict[str, dict[str, Any]] | None = None,
    analysis_mode: str,
    chunk_threshold_chars: int = 25000,
    chunk_size_chars: int = 20000,
) -> dict[str, Any]:
    contents = file_contents if isinstance(file_contents, dict) else {}
    meta_index = file_fetch_meta if isinstance(file_fetch_meta, dict) else {}
    files: list[dict[str, Any]] = []
    chunked_file_count = 0
    truncated_fetch_count = 0
    total_chunk_count = 0

    threshold = max(4000, int(chunk_threshold_chars or 25000))
    chunk_size = max(4000, int(chunk_size_chars or 20000))

    for path in sorted(contents.keys()):
        text = str(contents.get(path, "") or "")
        if not text:
            continue
        ext = Path(str(path)).suffix.lower()
        meta = meta_index.get(str(path), {}) if isinstance(meta_index.get(str(path), {}), dict) else {}
        original_chars = int(meta.get("original_char_count", len(text)) or len(text))
        fetched_chars = int(meta.get("fetched_char_count", len(text)) or len(text))
        truncated = bool(meta.get("truncated_at_fetch", False)) or original_chars > fetched_chars
        if truncated:
            truncated_fetch_count += 1
        chunkable = ext in CHUNKABLE_EXTENSIONS
        chunked = chunkable and (truncated or fetched_chars > threshold)
        rows: list[dict[str, Any]] = []
        if chunked:
            for idx, start in enumerate(range(0, len(text), chunk_size), start=1):
                end = min(start + chunk_size, len(text))
                rows.append(
                    {
                        "file_chunk_id": f"{snapshot_id}:{Path(str(path)).name}:{idx}",
                        "chunk_index": idx,
                        "start_char": start,
                        "end_char": end,
                    }
                )
            if rows:
                chunked_file_count += 1
                total_chunk_count += len(rows)
        files.append(
            {
                "path": str(path),
                "extension": ext,
                "original_char_count": original_chars,
                "fetched_char_count": fetched_chars,
                "truncated_at_fetch": truncated,
                "chunked_for_analysis": bool(rows),
                "chunk_count": len(rows),
                "chunks": rows[:200],
            }
        )

    return {
        "artifact_type": "file_chunk_manifest_v1",
        "snapshot_id": snapshot_id,
        "analysis_mode": str(analysis_mode or "").strip() or "standard",
        "chunk_threshold_chars": threshold,
        "chunk_size_chars": chunk_size,
        "file_count": len(files),
        "truncated_fetch_count": truncated_fetch_count,
        "chunked_file_count": chunked_file_count,
        "chunk_count": total_chunk_count,
        "files": files[:4000],
    }
