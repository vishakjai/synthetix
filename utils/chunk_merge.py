from __future__ import annotations

from pathlib import Path
from typing import Any


def _path_kind(path: str) -> str:
    ext = Path(str(path or "")).suffix.lower()
    return {
        ".frm": "form",
        ".ctl": "form",
        ".cls": "class",
        ".bas": "module",
        ".vbp": "project",
        ".vbg": "project",
    }.get(ext, "other")


def build_chunk_qa_report_v1(
    *,
    snapshot_id: str,
    chunk_manifest: dict[str, Any],
    chunk_executions: list[dict[str, Any]],
    large_repo_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    manifest_rows = chunk_manifest.get("chunks", []) if isinstance(chunk_manifest.get("chunks", []), list) else []
    manifest_index = {
        str(row.get("chunk_id", "")).strip(): row
        for row in manifest_rows
        if isinstance(row, dict) and str(row.get("chunk_id", "")).strip()
    }
    context = large_repo_context if isinstance(large_repo_context, dict) else {}
    included_chunk_ids = {
        str(x).strip() for x in context.get("included_chunks", []) if str(x).strip()
    }
    if not included_chunk_ids:
        included_chunk_ids = set(manifest_index.keys())
    execution_index = {
        str(row.get("chunk_id", "")).strip(): row
        for row in chunk_executions
        if isinstance(row, dict) and str(row.get("chunk_id", "")).strip()
    }

    rows: list[dict[str, Any]] = []
    analyzed_chunks = 0
    total_selected_files = 0
    total_form_like_files = 0
    total_logic_files = 0
    total_extracted_forms = 0
    total_extracted_handlers = 0
    total_extracted_functions = 0
    total_omitted_chunks = 0

    for chunk_id, manifest in manifest_index.items():
        paths = [str(p).replace("\\", "/").strip() for p in manifest.get("paths", []) if str(p).strip()]
        selected_files = len(paths)
        form_like_files = len([p for p in paths if _path_kind(p) == "form"])
        logic_files = len([p for p in paths if _path_kind(p) in {"module", "class"}])
        total_selected_files += selected_files
        total_form_like_files += form_like_files
        total_logic_files += logic_files
        execution = execution_index.get(chunk_id, {})
        analyzed = chunk_id in included_chunk_ids and bool(execution)
        if analyzed:
            analyzed_chunks += 1
        else:
            total_omitted_chunks += 1
        extracted_forms = int(execution.get("forms_count", 0) or 0)
        extracted_handlers = int(execution.get("event_handlers_count", 0) or 0)
        extracted_functions = int(execution.get("functions_count", 0) or 0)
        total_extracted_forms += extracted_forms
        total_extracted_handlers += extracted_handlers
        total_extracted_functions += extracted_functions
        coverage_expectations = manifest.get("coverage_expectations", {}) if isinstance(manifest.get("coverage_expectations", {}), dict) else {}
        blocking_checks: list[dict[str, Any]] = []
        if coverage_expectations.get("should_extract_project_membership"):
            blocking_checks.append({
                "check": "project_membership_detected",
                "status": "PASS" if bool(execution.get("project_members_count", 0)) else "WARN",
                "detail": f"project_members={int(execution.get('project_members_count', 0) or 0)}",
            })
        if coverage_expectations.get("should_extract_forms"):
            blocking_checks.append({
                "check": "forms_detected",
                "status": "PASS" if extracted_forms > 0 else "WARN",
                "detail": f"expected_form_files={form_like_files}, extracted_forms={extracted_forms}",
            })
        if coverage_expectations.get("should_extract_modules"):
            blocking_checks.append({
                "check": "logic_signals_detected",
                "status": "PASS" if extracted_functions > 0 else "WARN",
                "detail": f"expected_logic_files={logic_files}, extracted_functions={extracted_functions}",
            })
        rows.append({
            "chunk_id": chunk_id,
            "component_id": str(manifest.get("component_id", "")).strip(),
            "component_name": str(manifest.get("component_name", "")).strip(),
            "component_type": str(manifest.get("component_type", "")).strip(),
            "selected_file_count": selected_files,
            "form_like_file_count": form_like_files,
            "logic_file_count": logic_files,
            "estimated_loc": int(manifest.get("estimated_loc", 0) or 0),
            "analyzed": analyzed,
            "status": "ANALYZED" if analyzed else "OMITTED",
            "extracted_forms": extracted_forms,
            "extracted_event_handlers": extracted_handlers,
            "extracted_functions": extracted_functions,
            "summary": str(execution.get("summary", "")).strip(),
            "coverage_expectations": coverage_expectations,
            "checks": blocking_checks,
        })

    return {
        "artifact_type": "chunk_qa_report_v1",
        "snapshot_id": snapshot_id,
        "chunk_count": len(rows),
        "analyzed_chunk_count": analyzed_chunks,
        "omitted_chunk_count": total_omitted_chunks,
        "selected_file_count": total_selected_files,
        "selected_form_like_file_count": total_form_like_files,
        "selected_logic_file_count": total_logic_files,
        "extracted_forms": total_extracted_forms,
        "extracted_event_handlers": total_extracted_handlers,
        "extracted_functions": total_extracted_functions,
        "chunks": rows[:1000],
    }


def build_merged_analysis_coverage_v1(
    *,
    snapshot_id: str,
    repo_scan_coverage: dict[str, Any],
    chunk_qa_report: dict[str, Any],
    forms_count_reported: int,
    event_handler_count_exact: int,
    bas_module_count: int,
) -> dict[str, Any]:
    coverage = repo_scan_coverage if isinstance(repo_scan_coverage, dict) else {}
    bundle_summary = coverage.get("bundle_summary", {}) if isinstance(coverage.get("bundle_summary", {}), dict) else {}
    large_repo = coverage.get("large_repo_context", {}) if isinstance(coverage.get("large_repo_context", {}), dict) else {}
    return {
        "artifact_type": "merged_analysis_coverage_v1",
        "snapshot_id": snapshot_id,
        "analysis_mode": str(coverage.get("analysis_mode", "standard") or "standard"),
        "selected_file_count": int(coverage.get("selected_file_count", 0) or 0),
        "fetched_file_count": int(coverage.get("fetched_file_count", 0) or 0),
        "failed_fetch_count": int(coverage.get("failed_fetch_count", 0) or 0),
        "bundle_included_file_count": int(bundle_summary.get("included_file_count", 0) or 0),
        "bundle_omitted_file_count": int(bundle_summary.get("omitted_file_count", 0) or 0),
        "chunk_manifest_count": int(coverage.get("chunk_count", 0) or 0),
        "chunk_context_included_count": int(large_repo.get("included_chunk_count", 0) or 0),
        "chunk_context_omitted_count": int(large_repo.get("omitted_chunk_count", 0) or 0),
        "analyzed_chunk_count": int(chunk_qa_report.get("analyzed_chunk_count", 0) or 0),
        "omitted_chunk_count": int(chunk_qa_report.get("omitted_chunk_count", 0) or 0),
        "forms_reported": int(forms_count_reported or 0),
        "event_handlers_reported": int(event_handler_count_exact or 0),
        "bas_module_count": int(bas_module_count or 0),
        "chunk_extracted_forms": int(chunk_qa_report.get("extracted_forms", 0) or 0),
        "chunk_extracted_event_handlers": int(chunk_qa_report.get("extracted_event_handlers", 0) or 0),
        "chunk_extracted_functions": int(chunk_qa_report.get("extracted_functions", 0) or 0),
        "failed_paths": coverage.get("failed_paths", [])[:200] if isinstance(coverage.get("failed_paths", []), list) else [],
    }
