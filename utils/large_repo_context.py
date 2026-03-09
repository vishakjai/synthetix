from __future__ import annotations

from pathlib import Path
from typing import Any


def build_large_repo_context_v1(
    *,
    snapshot_id: str,
    repo_snapshot: dict[str, Any],
    component_inventory: dict[str, Any],
    chunk_manifest: dict[str, Any],
    dependency_graph: dict[str, Any],
    file_contents: dict[str, str],
    max_total_chars: int,
) -> dict[str, Any]:
    chunks = chunk_manifest.get("chunks", []) if isinstance(chunk_manifest.get("chunks", []), list) else []
    components = component_inventory.get("components", []) if isinstance(component_inventory.get("components", []), list) else []
    lines: list[str] = []
    total = 0
    included_files: list[str] = []
    included_chunks: list[str] = []
    omitted_chunks: list[str] = []

    header = (
        "### LARGE REPO CONTEXT\n"
        f"snapshot_id={snapshot_id}\n"
        f"analysis_mode={str(repo_snapshot.get('analysis_mode', 'large_repo'))}\n"
        f"selected_files={int(repo_snapshot.get('selected_file_count', 0) or 0)}\n"
        f"fetched_files={int(repo_snapshot.get('fetched_file_count', 0) or 0)}\n"
        f"failed_fetches={int(repo_snapshot.get('failed_fetch_count', 0) or 0)}\n"
        f"components={len(components)}\n"
        f"chunks={len(chunks)}\n"
        f"dependency_nodes={int(dependency_graph.get('node_count', 0) or 0)}\n"
        f"dependency_edges={int(dependency_graph.get('edge_count', 0) or 0)}\n"
    )
    lines.append(header)
    total += len(header)

    prioritized_chunks = sorted(
        [row for row in chunks if isinstance(row, dict)],
        key=lambda row: (
            int(row.get("priority", 9) or 9),
            0 if bool(row.get("contains_project_descriptor", False)) else 1,
            -int(row.get("estimated_loc", 0) or 0),
            str(row.get("chunk_id", "")),
        ),
    )
    chunk_budget = max(int(max_total_chars or 0) - total, 0)
    if chunk_budget <= 0:
        return {
            "artifact_type": "legacy_chunk_context_v1",
            "snapshot_id": snapshot_id,
            "context_text": "\n".join(lines).strip(),
            "included_chunk_count": 0,
            "omitted_chunk_count": len(prioritized_chunks),
            "included_file_count": 0,
            "omitted_file_count": 0,
            "included_chunks": [],
            "omitted_chunks": [str(row.get("chunk_id", "")) for row in prioritized_chunks[:200]],
        }

    max_chunks_with_content = min(len(prioritized_chunks), 12)
    for idx, chunk in enumerate(prioritized_chunks, start=1):
        chunk_id = str(chunk.get("chunk_id", "")).strip()
        paths = [str(p).replace("\\", "/").strip() for p in chunk.get("paths", []) if str(p).strip()]
        if not chunk_id or not paths:
            continue
        remaining = max_total_chars - total
        if remaining <= 200 or len(included_chunks) >= max_chunks_with_content:
            omitted_chunks.append(chunk_id)
            continue
        heading = (
            f"\n## CHUNK: {chunk_id}\n"
            f"component={str(chunk.get('component_name', '')).strip()} "
            f"type={str(chunk.get('component_type', '')).strip()} "
            f"priority={int(chunk.get('priority', 0) or 0)} "
            f"estimated_loc={int(chunk.get('estimated_loc', 0) or 0)}\n"
        )
        if total + len(heading) > max_total_chars:
            omitted_chunks.append(chunk_id)
            continue
        lines.append(heading)
        total += len(heading)
        included_chunks.append(chunk_id)
        per_chunk_cap = max(6000, min(remaining - len(heading), max_total_chars // max(max_chunks_with_content, 1)))
        for path in sorted(paths, key=_bundle_order_key):
            content = str(file_contents.get(path, "") or "")
            if not content:
                continue
            block = f"\n### FILE: {path}\n{content}\n"
            if total + len(block) > max_total_chars or len(block) > per_chunk_cap:
                trimmed = min(max_total_chars - total, per_chunk_cap)
                if trimmed <= 200:
                    continue
                block = block[:trimmed]
            lines.append(block)
            total += len(block)
            included_files.append(path)
            if total >= max_total_chars:
                break
        if total >= max_total_chars:
            break

    all_paths = {
        str(path).replace("\\", "/").strip()
        for row in prioritized_chunks
        if isinstance(row, dict)
        for path in row.get("paths", [])
        if str(path).strip()
    }
    omitted_files = sorted(all_paths - set(included_files))
    return {
        "artifact_type": "legacy_chunk_context_v1",
        "snapshot_id": snapshot_id,
        "included_chunk_count": len(included_chunks),
        "omitted_chunk_count": len(omitted_chunks),
        "included_file_count": len(included_files),
        "omitted_file_count": len(omitted_files),
        "included_chunks": included_chunks[:200],
        "omitted_chunks": omitted_chunks[:200],
        "included_paths_sample": included_files[:400],
        "omitted_paths_sample": omitted_files[:400],
        "context_text": "\n".join(lines).strip()[: max_total_chars],
    }


def _bundle_order_key(path: str) -> tuple[int, str]:
    ext = Path(str(path or "")).suffix.lower()
    order = {
        ".vbp": 0,
        ".vbg": 0,
        ".frm": 1,
        ".ctl": 1,
        ".cls": 2,
        ".bas": 3,
        ".dcx": 4,
        ".dca": 4,
        ".mdb": 5,
        ".accdb": 5,
        ".frx": 6,
        ".ctx": 6,
        ".res": 6,
        ".ocx": 6,
    }
    return (order.get(ext, 9), str(path).replace("\\", "/").lower())
