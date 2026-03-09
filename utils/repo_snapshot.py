from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any


VB6_SOURCE_EXTENSIONS = {".frm", ".ctl", ".cls", ".bas", ".vbp", ".vbg", ".frx", ".ctx", ".res", ".ocx", ".dcx", ".dca", ".mdb", ".accdb"}


def classify_repo_scan_mode(
    selected_entries: list[dict[str, Any]],
    *,
    total_tree_entries: int = 0,
) -> dict[str, Any]:
    entries = [row for row in selected_entries if isinstance(row, dict)]
    counts_by_ext: Counter[str] = Counter()
    total_size = 0
    vb6_projects = 0
    vb6_forms = 0
    vb6_modules = 0
    for row in entries:
        path = str(row.get("path", "")).strip().lower()
        ext = Path(path).suffix.lower()
        if ext:
            counts_by_ext[ext] += 1
        total_size += int(row.get("size", 0) or 0)
        if ext in {".vbp", ".vbg"}:
            vb6_projects += 1
        elif ext in {".frm", ".ctl"}:
            vb6_forms += 1
        elif ext in {".bas", ".cls"}:
            vb6_modules += 1

    reasons: list[str] = []
    if len(entries) > 500:
        reasons.append(f"selected files={len(entries)}")
    if total_tree_entries > 5000:
        reasons.append(f"tree entries={int(total_tree_entries or 0)}")
    if total_size > 25_000_000:
        reasons.append(f"selected bytes={total_size}")
    if vb6_projects > 5:
        reasons.append(f"vb6 projects={vb6_projects}")
    if vb6_forms > 80:
        reasons.append(f"vb6 forms/usercontrols={vb6_forms}")
    if vb6_modules > 120:
        reasons.append(f"vb6 modules/classes={vb6_modules}")

    mode = "large_repo" if reasons else "standard"
    return {
        "analysis_mode": mode,
        "reasons": reasons,
        "selected_file_count": len(entries),
        "total_tree_entries": int(total_tree_entries or 0),
        "selected_total_bytes": total_size,
        "counts_by_extension": dict(sorted(counts_by_ext.items())),
        "vb6_projects": vb6_projects,
        "vb6_forms": vb6_forms,
        "vb6_modules": vb6_modules,
    }


def build_repo_snapshot_v1(
    *,
    snapshot_id: str,
    repo_url: str,
    owner: str,
    repository: str,
    branch: str,
    commit_sha: str,
    tree_sha: str,
    include_paths: list[str],
    exclude_paths: list[str],
    raw_entries: list[dict[str, Any]],
    selected_entries: list[dict[str, Any]],
    file_contents: dict[str, str],
    failed_paths: list[str],
    reused_paths: list[str],
    changed_paths: set[str] | list[str],
    compare_error: str,
    chunk_size: int,
    chunk_workers: int,
    family_key: str,
    bundle_summary: dict[str, Any],
    analysis_mode: str,
    analysis_mode_reasons: list[str],
) -> dict[str, Any]:
    counts_by_type: Counter[str] = Counter()
    estimated_loc_by_type: Counter[str] = Counter()
    files: list[dict[str, Any]] = []
    file_contents_map = file_contents if isinstance(file_contents, dict) else {}
    failed_set = {str(path).strip() for path in failed_paths if str(path).strip()}
    reused_set = {str(path).strip() for path in reused_paths if str(path).strip()}
    selected_paths = {str(row.get("path", "")).strip() for row in selected_entries if isinstance(row, dict) and str(row.get("path", "")).strip()}

    for row in selected_entries:
        if not isinstance(row, dict):
            continue
        path = str(row.get("path", "")).strip()
        if not path:
            continue
        ext = Path(path).suffix.lower()
        loc = len(str(file_contents_map.get(path, "") or "").splitlines()) if path in file_contents_map else 0
        kind = _kind_from_path(path)
        counts_by_type[kind] += 1
        estimated_loc_by_type[kind] += int(loc or 0)
        files.append(
            {
                "path": path,
                "ext": ext,
                "kind": kind,
                "size": int(row.get("size", 0) or 0),
                "sha": str(row.get("sha", "") or ""),
                "selected_for_analysis": True,
                "fetched": path in file_contents_map,
                "reused": path in reused_set,
                "failed_fetch": path in failed_set,
                "estimated_loc": int(loc or 0),
                "language": _path_language_hint(path),
                "is_binary": ext in {".frx", ".ctx", ".res", ".ocx", ".mdb", ".accdb"},
            }
        )

    return {
        "artifact_type": "repo_snapshot_v1",
        "snapshot_id": snapshot_id,
        "family_key": family_key,
        "repo_provider": "github",
        "repo_url": repo_url,
        "owner": owner,
        "repository": repository,
        "branch": branch,
        "commit_sha": commit_sha,
        "tree_sha": tree_sha,
        "include_paths": include_paths,
        "exclude_paths": exclude_paths,
        "analysis_mode": analysis_mode,
        "analysis_mode_reasons": analysis_mode_reasons[:20],
        "total_tree_entries": len(raw_entries),
        "selected_file_count": len(selected_paths),
        "fetched_file_count": len(file_contents_map),
        "failed_fetch_count": len(failed_set),
        "reused_file_count": len(reused_set),
        "changed_path_count": len(changed_paths or []),
        "changed_paths_sample": sorted({str(x).strip() for x in (changed_paths or []) if str(x).strip()})[:200],
        "compare_error": str(compare_error or "").strip(),
        "chunk_size": int(chunk_size or 0),
        "chunk_workers": int(chunk_workers or 0),
        "counts_by_type": dict(sorted(counts_by_type.items())),
        "estimated_loc_by_type": dict(sorted(estimated_loc_by_type.items())),
        "bundle_summary": bundle_summary if isinstance(bundle_summary, dict) else {},
        "failed_paths": sorted(failed_set)[:200],
        "reused_paths_sample": sorted(reused_set)[:200],
        "selected_paths": sorted(selected_paths)[:400],
        "files": files[:4000],
    }


def _kind_from_path(path: str) -> str:
    ext = Path(str(path or "")).suffix.lower()
    return {
        ".frm": "form",
        ".ctl": "usercontrol",
        ".bas": "module",
        ".cls": "class",
        ".vbp": "project",
        ".vbg": "project_group",
        ".frx": "form_binary",
        ".ctx": "usercontrol_binary",
        ".dsr": "designer",
        ".dca": "connection_definition",
        ".dcx": "query_definition",
        ".mdb": "database",
        ".accdb": "database",
    }.get(ext, "other")


def _path_language_hint(path: str) -> str:
    suffix = Path(path).suffix.lower()
    if suffix in VB6_SOURCE_EXTENSIONS:
        return "vb6"
    if suffix in {".py"}:
        return "python"
    if suffix in {".js", ".ts", ".tsx"}:
        return "javascript"
    if suffix in {".go"}:
        return "go"
    if suffix in {".java"}:
        return "java"
    if suffix in {".cs"}:
        return "csharp"
    if suffix in {".sql"}:
        return "sql"
    if suffix in {".md"}:
        return "markdown"
    return "unknown"
