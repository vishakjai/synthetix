#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

_ANALYST_REPORT_PATH = ROOT / "utils" / "analyst_report.py"
_ANALYST_REPORT_SPEC = importlib.util.spec_from_file_location("synthetix_analyst_report", _ANALYST_REPORT_PATH)
if _ANALYST_REPORT_SPEC is None or _ANALYST_REPORT_SPEC.loader is None:
    raise RuntimeError(f"Unable to load analyst_report module from {_ANALYST_REPORT_PATH}")
_ANALYST_REPORT_MODULE = importlib.util.module_from_spec(_ANALYST_REPORT_SPEC)
_ANALYST_REPORT_SPEC.loader.exec_module(_ANALYST_REPORT_MODULE)
build_analyst_report_v2 = _ANALYST_REPORT_MODULE.build_analyst_report_v2


DEFAULT_BASE_URL = "http://127.0.0.1:8788"
DEFAULT_REPO_URL = "https://github.com/vishakjai/TestVBProject1"
DEFAULT_OBJECTIVE = (
    "Analyze this legacy VB6 repository, identify all projects/forms/modules/ActiveX dependencies/"
    "business rules/SQL behaviors, and produce a detailed modernization-ready analyst artifact."
)
VB6_EXTENSIONS = {".cls", ".frm", ".frx", ".bas", ".ctl", ".ctx", ".vbp", ".vbg", ".res", ".ocx", ".dcx", ".dca", ".dsr", ".mdb", ".accdb"}
VB6_TEXT_EXTENSIONS = {".cls", ".frm", ".bas", ".ctl", ".vbp", ".vbg", ".dca", ".dcx", ".dsr"}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _http_json(method: str, url: str, payload: dict[str, Any] | None = None, timeout: int = 60) -> dict[str, Any]:
    data: bytes | None = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = Request(url, data=data, headers=headers, method=method)
    try:
        with urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except TimeoutError as exc:
        raise RuntimeError(f"Request timed out on {url} (timeout={timeout}s)") from exc
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
        raise RuntimeError(f"HTTP {exc.code} on {url}: {body}") from exc
    except URLError as exc:
        raise RuntimeError(f"URL error on {url}: {exc}") from exc

    try:
        parsed = json.loads(raw or "{}")
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSON from {url}: {raw[:500]}") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError(f"Unexpected response shape from {url}")
    return parsed


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _escape_pipe(value: Any) -> str:
    return _clean(value).replace("|", "\\|")


def _dependency_name(value: Any) -> str:
    text = _clean(value)
    if not text:
        return ""
    quoted_pair = re.search(r'"([^"]+)"\s*;\s*"([^"]+)"', text)
    if quoted_pair:
        return _clean(quoted_pair.group(2))
    ext_match = re.search(r"([A-Za-z0-9_.-]+\.(?:ocx|dll|dcx|dca))", text, flags=re.IGNORECASE)
    if ext_match:
        return _clean(ext_match.group(1))
    suffix_guid = re.search(r"\(([^)]*\{[0-9A-Fa-f-]{36}[^)]*)\)\s*$", text)
    if suffix_guid:
        return _clean(re.sub(r"\s*\([^)]*\{[0-9A-Fa-f-]{36}[^)]*\)\s*$", "", text))
    return text


def _normalize_open_question(row: Any, idx: int) -> dict[str, str]:
    if isinstance(row, str):
        return {
            "id": f"Q-{idx + 1:03d}",
            "question": _clean(row) or "Clarification required",
            "owner": "Unassigned",
            "severity": "medium",
            "context": "",
        }
    if not isinstance(row, dict):
        return {
            "id": f"Q-{idx + 1:03d}",
            "question": "Clarification required",
            "owner": "Unassigned",
            "severity": "medium",
            "context": "",
        }
    severity = _clean(row.get("severity") or row.get("priority") or "medium").lower()
    if severity not in {"blocker", "high", "medium", "low"}:
        severity = "medium"
    return {
        "id": _clean(row.get("id")) or f"Q-{idx + 1:03d}",
        "question": _clean(row.get("question") or row.get("text") or row.get("summary")) or "Clarification required",
        "owner": _clean(row.get("owner") or row.get("assignee")) or "Unassigned",
        "severity": severity,
        "context": _clean(row.get("context") or row.get("impact")),
    }


def build_full_markdown(output: dict[str, Any], mode: str = "full") -> str:
    revised = _clean(output.get("human_revised_document_markdown"))
    if revised:
        return revised

    report = build_analyst_report_v2(output)
    metadata = _as_dict(report.get("metadata"))
    project = _as_dict(metadata.get("project"))
    brief = _as_dict(report.get("decision_brief"))
    glance = _as_dict(brief.get("at_a_glance"))
    inventory = _as_dict(glance.get("inventory_summary"))
    source_loc_total = _as_int(inventory.get("source_loc_total"), 0)
    source_loc_forms = _as_int(inventory.get("source_loc_forms"), 0)
    source_loc_modules = _as_int(inventory.get("source_loc_modules"), 0)
    source_loc_classes = _as_int(inventory.get("source_loc_classes"), 0)
    source_files_scanned = _as_int(inventory.get("source_files_scanned"), 0)
    raw_for_loc = _as_dict(report.get("raw_artifacts")) or _as_dict(output.get("raw_artifacts"))
    raw_legacy_for_loc = _as_dict(raw_for_loc.get("legacy_inventory"))
    legacy_summary_for_loc = _as_dict(raw_legacy_for_loc.get("summary"))
    legacy_counts_for_loc = _as_dict(legacy_summary_for_loc.get("counts"))
    loc_rows_for_loc = _as_list(raw_legacy_for_loc.get("source_loc_by_file"))
    vb6_projects_for_loc = _as_list(raw_legacy_for_loc.get("vb6_projects"))
    if source_loc_total <= 0:
        source_loc_total = _as_int(legacy_counts_for_loc.get("source_loc_total"), 0)
    if source_loc_forms <= 0:
        source_loc_forms = _as_int(legacy_counts_for_loc.get("source_loc_forms"), 0)
    if source_loc_modules <= 0:
        source_loc_modules = _as_int(legacy_counts_for_loc.get("source_loc_modules"), 0)
    if source_loc_classes <= 0:
        source_loc_classes = _as_int(legacy_counts_for_loc.get("source_loc_classes"), 0)
    if source_files_scanned <= 0:
        source_files_scanned = _as_int(legacy_counts_for_loc.get("source_files_scanned"), 0)
    if (
        source_loc_total <= 0
        or source_loc_forms <= 0
        or source_loc_modules <= 0
        or source_files_scanned <= 0
    ) and vb6_projects_for_loc:
        project_loc_total = 0
        project_loc_forms = 0
        project_loc_modules = 0
        project_files_scanned = 0
        for row in vb6_projects_for_loc[:200]:
            rr = _as_dict(row)
            project_loc_total += _as_int(rr.get("source_loc_total"), 0)
            project_loc_forms += _as_int(rr.get("source_loc_forms"), 0)
            project_loc_modules += _as_int(rr.get("source_loc_modules"), 0)
            project_files_scanned += _as_int(rr.get("members"), 0)
        if source_loc_total <= 0:
            source_loc_total = project_loc_total
        if source_loc_forms <= 0:
            source_loc_forms = project_loc_forms
        if source_loc_modules <= 0:
            source_loc_modules = project_loc_modules
        if source_loc_classes <= 0:
            inferred_classes = project_loc_total - (project_loc_forms + project_loc_modules)
            if inferred_classes > 0:
                source_loc_classes = inferred_classes
        if source_files_scanned <= 0:
            source_files_scanned = project_files_scanned
    if (source_loc_total <= 0 or source_files_scanned <= 0) and loc_rows_for_loc:
        loc_by_path: dict[str, int] = {}
        for row in loc_rows_for_loc[:8000]:
            rr = _as_dict(row)
            path = _clean(rr.get("path"))
            if not path:
                continue
            loc_by_path[path] = _as_int(rr.get("loc"), 0)
        if source_loc_total <= 0:
            source_loc_total = sum(loc_by_path.values())
        if source_loc_forms <= 0:
            source_loc_forms = sum(
                loc for path, loc in loc_by_path.items()
                if path.lower().endswith((".frm", ".ctl"))
            )
        if source_loc_modules <= 0:
            source_loc_modules = sum(
                loc for path, loc in loc_by_path.items()
                if path.lower().endswith(".bas")
            )
        if source_loc_classes <= 0:
            source_loc_classes = sum(
                loc for path, loc in loc_by_path.items()
                if path.lower().endswith(".cls")
            )
        if source_files_scanned <= 0:
            source_files_scanned = len(loc_by_path)
    if source_loc_total <= 0 or source_files_scanned <= 0:
        line_hints: dict[str, int] = {}
        line_ref_rx = re.compile(r"([A-Za-z0-9_./\\\\ -]+\.(?:frm|frx|bas|cls|ctl|vb|ctx|dsr)):(\d+)", re.IGNORECASE)

        def _collect_line_hints(node: Any, depth: int = 0) -> None:
            if depth > 8:
                return
            if isinstance(node, dict):
                for vv in list(node.values())[:2000]:
                    _collect_line_hints(vv, depth + 1)
                return
            if isinstance(node, list):
                for vv in node[:4000]:
                    _collect_line_hints(vv, depth + 1)
                return
            text = _clean(node)
            if not text:
                return
            for m in line_ref_rx.finditer(text):
                path = _clean(m.group(1)).replace("\\\\", "/")
                if not path:
                    continue
                ln = _as_int(m.group(2), 0)
                if ln <= 0:
                    continue
                prev = line_hints.get(path, 0)
                if ln > prev:
                    line_hints[path] = ln

        _collect_line_hints(report)
        _collect_line_hints(output)
        if line_hints:
            if source_loc_total <= 0:
                source_loc_total = sum(int(v or 0) for v in line_hints.values())
            if source_loc_forms <= 0:
                source_loc_forms = sum(
                    int(loc or 0) for path, loc in line_hints.items()
                    if path.lower().endswith((".frm", ".ctl", ".frx"))
                )
            if source_loc_modules <= 0:
                source_loc_modules = sum(
                    int(loc or 0) for path, loc in line_hints.items()
                    if path.lower().endswith(".bas")
                )
            if source_loc_classes <= 0:
                source_loc_classes = sum(
                    int(loc or 0) for path, loc in line_hints.items()
                    if path.lower().endswith(".cls")
                )
            if source_files_scanned <= 0:
                source_files_scanned = len(line_hints)
    if source_loc_classes <= 0 and vb6_projects_for_loc:
        project_loc_total = 0
        for row in vb6_projects_for_loc[:200]:
            rr = _as_dict(row)
            project_loc_total += _as_int(rr.get("source_loc_total"), 0)
        inferred_classes = project_loc_total - (source_loc_forms + source_loc_modules)
        if inferred_classes > 0:
            source_loc_classes = inferred_classes
    strategy = _as_dict(brief.get("recommended_strategy"))
    decisions = _as_dict(brief.get("decisions_required"))
    delivery_spec = _as_dict(report.get("delivery_spec"))
    testing = _as_dict(delivery_spec.get("testing_and_evidence"))
    qa_report = _as_dict(report.get("qa_report_v1"))
    if not qa_report:
        qa_report = _as_dict(output.get("qa_report_v1"))
    qa_summary = _as_dict(qa_report.get("summary"))
    qa_structural = _as_dict(qa_report.get("structural"))
    qa_semantic = _as_dict(qa_report.get("semantic"))
    qa_structural_checks = _as_list(qa_structural.get("checks"))
    qa_semantic_checks = _as_list(qa_semantic.get("checks"))
    qa_quality_gates = _as_list(qa_report.get("quality_gates"))
    backlog = _as_list(_as_dict(delivery_spec.get("backlog")).get("items"))
    appendix = _as_dict(report.get("appendix"))
    open_questions = _as_list(delivery_spec.get("open_questions"))
    report_source_language = _clean(_as_dict(metadata.get("context_reference")).get("source_language")).strip().lower()
    raw_artifacts = _as_dict(report.get("raw_artifacts"))
    source_language = _clean(report_source_language or output.get("source_language")).strip().lower()
    is_php_summary = source_language == "php" or any(
        inventory.get(key) not in (None, "", 0)
        for key in ("controllers", "routes", "templates", "session_keys", "auth_touchpoints", "background_jobs", "file_io_flows")
    ) or any(
        key in raw_artifacts
        for key in (
            "php_framework_profile_v1",
            "php_route_inventory_v1",
            "php_controller_inventory_v1",
            "php_template_inventory_v1",
            "php_sql_catalog_v1",
            "php_session_state_inventory_v1",
            "php_authz_authn_inventory_v1",
        )
    )

    if is_php_summary:
        if source_loc_total <= 0:
            source_loc_total = _as_int(inventory.get("source_loc_total"), 0)
        if source_files_scanned <= 0:
            source_files_scanned = _as_int(inventory.get("source_files_scanned"), 0)

    if is_php_summary:
        inventory_summary_text = (
            f"{_as_int(inventory.get('applications'), 0)} application(s), "
            f"{_as_int(inventory.get('controllers'), 0)} controllers, "
            f"{_as_int(inventory.get('routes'), 0)} routes, "
            f"{_as_int(inventory.get('templates'), 0)} templates, "
            f"{_as_int(inventory.get('dependencies'), 0)} dependencies"
        )
        loc_summary_text = f"{source_loc_total} total LOC across {source_files_scanned} files"
    else:
        inventory_summary_text = (
            f"{_clean(inventory.get('projects') or 0)} project(s), "
            f"{_clean(inventory.get('forms') or 0)} forms/usercontrols, "
            f"{_clean(inventory.get('dependencies') or 0)} dependencies"
        )
        loc_summary_text = (
            f"{source_loc_total} total LOC "
            f"({source_loc_forms} form LOC, {source_loc_modules} module LOC, {source_loc_classes} class LOC) "
            f"across {source_files_scanned} files"
        )

    lines: list[str] = [
        f"# Modernization Brief - {_clean(project.get('name')) or 'Untitled Project'}",
        "",
        "## Header",
        f"- Objective: {_clean(project.get('objective')) or 'Not provided'}",
        f"- Domain: {_clean(project.get('domain')) or 'software'}",
        f"- Repo: {_clean(_as_dict(metadata.get('context_reference')).get('repo')) or 'n/a'} @ {_clean(_as_dict(metadata.get('context_reference')).get('branch')) or 'main'} ({_clean(_as_dict(metadata.get('context_reference')).get('commit_sha')) or 'n/a'})",
        f"- SIL Versions: SCM {_clean(_as_dict(metadata.get('context_reference')).get('scm_version')) or '1.0'} / CP {_clean(_as_dict(metadata.get('context_reference')).get('cp_version')) or '1.0'} / HA {_clean(_as_dict(metadata.get('context_reference')).get('ha_version')) or '1.0'}",
        f"- Generated At: {_clean(metadata.get('generated_at'))}",
        "",
        "## Decision Brief",
        "",
        "| Category | Summary |",
        "|---|---|",
        f"| Modernization readiness | {_clean(glance.get('readiness_score') or 'n/a')}/100 |",
        f"| Risk tier | {_clean(glance.get('risk_tier') or 'n/a')} |",
        f"| Inventory | {inventory_summary_text} |",
        f"| Lines of code scanned | {loc_summary_text} |",
        f"| Data touchpoints | {', '.join(_as_list(inventory.get('tables_touched')))} |",
        f"| Headline | {_escape_pipe(glance.get('headline'))} |",
        "",
        "### Recommended strategy",
        f"- {_clean(strategy.get('name')) or 'Phased modernization'}: {_clean(strategy.get('rationale'))}",
    ]

    for phase in _as_list(strategy.get("phases")):
        phase_d = _as_dict(phase)
        lines.append(
            f"- {_clean(phase_d.get('id'))} {_clean(phase_d.get('title'))}: {_clean(phase_d.get('outcome'))}"
        )

    lines.extend(["", "### Decisions Required (Blocking)"])
    blocking = _as_list(decisions.get("blocking"))
    if blocking:
        for row in blocking:
            row_d = _as_dict(row)
            lines.append(f"- {_clean(row_d.get('id')) or 'DEC'}: {_clean(row_d.get('question'))}")
            lines.append(f"  - Recommendation: {_clean(row_d.get('default_recommendation'))}")
    else:
        lines.append("- None")

    lines.extend(["", "### Decisions Required (Non-blocking)"])
    non_blocking = _as_list(decisions.get("non_blocking"))
    if non_blocking:
        for row in non_blocking:
            row_d = _as_dict(row)
            lines.append(f"- {_clean(row_d.get('id')) or 'DEC'}: {_clean(row_d.get('question'))}")
    else:
        lines.append("- None")

    lines.extend(["", "## Delivery Spec", "", "### Backlog"])
    lines.append("| ID | Pri | Type | Outcome | Acceptance |")
    lines.append("|---|---|---|---|---|")
    if backlog:
        for item in backlog[:80]:
            item_d = _as_dict(item)
            ac = " / ".join(_as_list(item_d.get("acceptance_criteria"))[:2])
            lines.append(
                "| {} | {} | {} | {} | {} |".format(
                    _clean(item_d.get("id")),
                    _clean(item_d.get("priority")),
                    _clean(item_d.get("type")),
                    _escape_pipe(item_d.get("title") or item_d.get("outcome")),
                    _escape_pipe(ac or "n/a"),
                )
            )
    else:
        lines.append("| - | - | - | No backlog items generated | - |")

    lines.extend(["", "### Testing and Evidence"])
    lines.append("- Golden flows:" if not is_php_summary else "- Route/controller parity anchors:")
    golden_flows = _as_list(testing.get("golden_flows"))
    if golden_flows:
        for flow in golden_flows:
            flow_d = _as_dict(flow)
            lines.append(
                f"  - {_clean(flow_d.get('id')) or 'GF'}: {_clean(flow_d.get('name'))} | entry={_clean(flow_d.get('entrypoint'))}"
            )
    else:
        lines.append("  - None" if not is_php_summary else "  - None derived yet from the current PHP route/controller inventory.")

    lines.append("- Quality gates:")
    quality_gates = _as_list(testing.get("quality_gates"))
    if quality_gates:
        for gate in quality_gates:
            gate_d = _as_dict(gate)
            lines.append(
                f"  - {_clean(gate_d.get('id')) or 'gate'}: {(_clean(gate_d.get('result')) or 'warn').upper()} | {_clean(gate_d.get('description'))}"
            )
    else:
        lines.append("  - None")
    lines.append("- QA summary:")
    if qa_summary:
        qa_pass = qa_summary.get("pass_count", 0)
        qa_warn = qa_summary.get("warn_count", 0)
        qa_fail = qa_summary.get("fail_count", 0)
        qa_blockers = qa_summary.get("blocker_count", 0)
        lines.append(f"  - Status: {_clean(qa_summary.get('status')) or 'PASS'}")
        lines.append(
            "  - Structural: "
            f"pass={qa_pass}, "
            f"warn={qa_warn}, "
            f"fail={qa_fail}, "
            f"blockers={qa_blockers}"
        )
        for gate in qa_quality_gates[:8]:
            g = _as_dict(gate)
            lines.append(
                f"  - QA Gate {_clean(g.get('id')) or 'qa_gate'}: "
                f"{(_clean(g.get('result')) or 'warn').upper()} | {_clean(g.get('description'))}"
            )
        if qa_structural_checks:
            blocking = sum(1 for row in qa_structural_checks if bool(_as_dict(row).get("blocking")))
            lines.append(f"  - Structural checks: {len(qa_structural_checks)} total ({blocking} blocking)")
        if qa_semantic_checks:
            lines.append(f"  - Semantic checks: {len(qa_semantic_checks)} warning(s)")
            for row in qa_semantic_checks[:8]:
                r = _as_dict(row)
                lines.append(
                    f"    - {_clean(r.get('check_id') or r.get('id') or 'semantic_check')}: "
                    f"{(_clean(r.get('severity')) or 'medium').upper()} | {_clean(r.get('detail'))}"
                )
    else:
        lines.append("  - None")
    lines.append("  - Rule consolidation notes are documented in Appendix Section E2 when duplicate rule templates are suppressed.")

    lines.extend(["", "### Open Questions"])
    if open_questions:
        for idx, q in enumerate(open_questions):
            row = _normalize_open_question(q, idx)
            lines.append(
                f"- [{row['severity'].upper()}] {row['id']}: {row['question']} (owner: {row['owner']})"
            )
            if row["context"]:
                lines.append(f"  - Context: {row['context']}")
    else:
        lines.append("- None")

    lines.extend(["", "## QA Validation Summary"])
    if qa_summary:
        qa_pass = qa_summary.get("pass_count", 0)
        qa_warn = qa_summary.get("warn_count", 0)
        qa_fail = qa_summary.get("fail_count", 0)
        qa_blockers = qa_summary.get("blocker_count", 0)
        lines.append(f"- Overall status: {_clean(qa_summary.get('status')) or 'PASS'}")
        lines.append(
            "- Structural summary: "
            f"pass={qa_pass}, "
            f"warn={qa_warn}, "
            f"fail={qa_fail}, "
            f"blockers={qa_blockers}"
        )
        auto_fixes = _as_list(qa_summary.get("auto_fixes_applied"))
        if auto_fixes:
            lines.append("- Auto-fixes applied:")
            for fix in auto_fixes[:12]:
                lines.append(f"  - {_clean(fix)}")
        if qa_semantic_checks:
            lines.append("- Active semantic warnings:")
            for row in qa_semantic_checks[:10]:
                r = _as_dict(row)
                lines.append(
                    f"  - {_clean(r.get('check_id') or r.get('id') or 'semantic_check')}: "
                    f"{_clean(r.get('detail'))}"
                )
    else:
        lines.append("- No QA artifact present.")

    lines.extend(["", "## Evidence Appendix"])
    refs = _as_dict(appendix.get("artifact_refs"))
    for key, value in refs.items():
        if _clean(value):
            lines.append(f"- {key}: {_clean(value)}")
    lines.append("- High-volume sections included in structured artifact (inventory, dependencies, event map, SQL catalog, business rules).")

    lines.extend(["", "## Appendix Snapshot"])
    hv = _as_dict(appendix.get("high_volume_sections"))
    raw = _as_dict(report.get("raw_artifacts")) or _as_dict(output.get("raw_artifacts"))
    raw_event_map = _as_list(_as_dict(raw.get("event_map")).get("entries"))
    raw_sql = _as_list(_as_dict(raw.get("sql_catalog")).get("statements"))
    raw_sql_map = _as_list(_as_dict(raw.get("sql_map")).get("entries"))
    raw_procedures = _as_list(_as_dict(raw.get("procedure_summary")).get("procedures"))
    raw_form_dossiers = _as_list(_as_dict(raw.get("form_dossier")).get("dossiers"))
    raw_deps = _as_list(_as_dict(raw.get("dependency_inventory")).get("dependencies"))
    raw_rules = _as_list(_as_dict(raw.get("business_rule_catalog")).get("rules"))
    raw_risks = _as_list(_as_dict(raw.get("risk_register")).get("risks"))
    raw_orphans = _as_list(_as_dict(raw.get("orphan_analysis")).get("orphans"))
    raw_landscape = _as_list(_as_dict(raw.get("repo_landscape")).get("projects"))
    raw_variant_inventory = _as_list(_as_dict(raw.get("variant_inventory")).get("variants"))
    raw_constitution = _as_list(_as_dict(raw.get("delivery_constitution")).get("principles"))
    raw_variant_diff = _as_dict(raw.get("variant_diff_report"))
    raw_mdb_inventory = _as_dict(raw.get("mdb_inventory"))
    raw_form_loc_profile = _as_dict(raw.get("form_loc_profile"))
    raw_connection_variants = _as_dict(raw.get("connection_string_variants"))
    raw_module_globals = _as_dict(raw.get("module_global_inventory"))
    raw_dead_form_refs = _as_dict(raw.get("dead_form_refs"))
    raw_de_report_map = _as_dict(raw.get("dataenvironment_report_mapping"))
    raw_static_risk_detectors = _as_dict(raw.get("static_risk_detectors"))
    raw_source_data_dictionary = _as_dict(raw.get("source_data_dictionary"))

    mdb_rows = _as_list(raw_mdb_inventory.get("databases"))
    form_loc_rows = _as_list(raw_form_loc_profile.get("forms"))
    designer_loc_rows = _as_list(raw_form_loc_profile.get("designer_files"))
    conn_variant_rows = _as_list(raw_connection_variants.get("variants"))
    module_rows = _as_list(raw_module_globals.get("modules"))
    module_global_rows = _as_list(raw_module_globals.get("globals"))
    dead_form_ref_rows = _as_list(raw_dead_form_refs.get("references"))
    de_report_rows = _as_list(raw_de_report_map.get("mappings"))
    static_detector_rows = _as_list(raw_static_risk_detectors.get("findings"))
    source_data_dictionary_rows = _as_list(raw_source_data_dictionary.get("rows"))
    report_meta = _as_dict(report.get("metadata"))
    context_ref = _as_dict(report_meta.get("context_reference"))
    imported_analysis_mode = any(
        "imported analysis bundle" in _clean(value).lower()
        for value in [
            report_meta.get("repo"),
            context_ref.get("repo"),
            output.get("run_context_bundle_ref"),
            _as_dict(raw.get("legacy_inventory")).get("source_mode"),
        ]
    )

    def _base_form_name(value: Any) -> str:
        text = _clean(value)
        if not text:
            return ""
        if "::" in text:
            text = text.split("::")[-1]
        text = text.split(":")[0]
        text = text.rsplit("/", 1)[-1]
        if text.lower().endswith(".frm"):
            text = text[:-4]
        return text.lower()

    def _project_from_scoped(value: Any) -> str:
        text = _clean(value)
        if "::" in text:
            return _clean(text.split("::", 1)[0])
        return ""

    def _form_key(project_name: Any, form_name: Any) -> str:
        form = _base_form_name(form_name)
        project = _clean(project_name).lower()
        return f"{project}::{form}" if project and form else form

    def _qualified_form_name(project_name: Any, form_name: Any) -> str:
        project = _clean(project_name)
        form = _clean(form_name)
        if project and form:
            return f"{project}::{form}"
        return form or "n/a"

    def _project_label(value: Any, project_path_map: dict[str, str]) -> str:
        name = _clean(value)
        if not name:
            return "n/a"
        path = _clean(project_path_map.get(name))
        return f"{name} [{path}]" if path else name

    def _extract_forms_from_text(value: Any) -> list[str]:
        text = _clean(value)
        if not text:
            return []
        forms = re.findall(r"([A-Za-z0-9_./-]+)", text)
        out: list[str] = []
        for form in forms:
            low = form.lower()
            if low.endswith((".bas", ".cls", ".ctl", ".vbp", ".vbg", ".res", ".dsr", ".mdl", ".mod")):
                continue
            if ("frm" not in low and not low.startswith("form")):
                continue
            normalized = form.rsplit("/", 1)[-1]
            normalized = normalized[:-4] if normalized.lower().endswith(".frm") else normalized
            normalized = normalized.rstrip(".,;:()[]{}")
            nlow = normalized.lower()
            if re.search(r"_(click|change|load|keypress|gotfocus|lostfocus|activate|deactivate)$", nlow):
                continue
            if normalized not in out:
                out.append(normalized)
        return out

    def _infer_form_type(*, form_name: str, purpose: str, procedures: list[dict[str, Any]], controls: list[Any], tables: set[str]) -> str:
        form_low = form_name.lower()
        purpose_low = purpose.lower()
        control_text = " ".join(_clean(c).lower() for c in controls)
        proc_names = {_clean(_as_dict(p).get("procedure_name")).lower() for p in procedures}
        if "splash" in form_low or "splash" in purpose_low:
            return "Splash"
        if (
            form_low in {"main", "mdiform"}
            or form_low.startswith("mdi")
            or "toolbar" in control_text
            or any("toolbar" in x for x in proc_names)
        ):
            return "MDI_Host"
        if (
            "login" in form_low
            or "auth" in purpose_low
            or ("form9" in form_low and ({"logi", "login"} & {t.lower() for t in tables}))
        ):
            return "Login"
        if form_low.startswith(("rpt", "datareport")):
            return "Report"
        return "Child"

    def _split_words(token: str) -> str:
        raw = _clean(token)
        lowered = raw.lower()
        if lowered.startswith("dtpicker"):
            raw = raw[len("dtpicker"):]
            raw = f"date{raw}" if raw else "date"
            lowered = raw.lower()
        for prefix in ("txt", "cbo", "cmb", "dtp", "msk", "lst", "chk", "opt", "lbl", "cmd"):
            if lowered.startswith(prefix) and len(raw) > len(prefix):
                lower_raw = raw.lower()
                if prefix == "opt" and lower_raw.startswith("option"):
                    continue
                if prefix == "chk" and lower_raw.startswith("check"):
                    continue
                if prefix == "txt" and lower_raw.startswith("text"):
                    continue
                if prefix == "cbo" and lower_raw.startswith("combo"):
                    continue
                raw = raw[len(prefix):]
                break
        raw = re.sub(r"[_\\-]+", " ", raw)
        raw = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", raw)
        raw = re.sub(r"([A-Za-z])(id|no)\b", r"\1 \2", raw, flags=re.IGNORECASE)
        raw = re.sub(r"([A-Za-z])([0-9])", r"\1 \2", raw)
        raw = re.sub(r"\s+", " ", raw).strip()
        return raw.lower()

    def _is_data_input_control(control_id: str) -> bool:
        cid = _clean(control_id).lower()
        return cid.startswith(("txt", "cbo", "cmb", "dtp", "msk", "lst", "chk", "opt"))

    def _to_business_input(control_id: str) -> str:
        words = _split_words(control_id)
        return words or _clean(control_id).lower()

    def _callable_kind(procedure_name: Any, form_name: Any, event_hint: Any = "") -> str:
        proc = _clean(procedure_name).lower()
        form = _base_form_name(form_name)
        evt = _clean(event_hint).lower()
        if form == "shared_module":
            return "shared_function"
        if evt:
            return "event_handler"
        if re.search(r"_(click|change|load|keypress|keydown|keyup|gotfocus|lostfocus|activate|deactivate)$", proc):
            return "event_handler"
        if proc.startswith(("cmd", "lbl", "txt", "cbo", "opt", "chk")):
            return "event_handler"
        return "procedure"

    def _semantic_form_alias(
        *,
        form_name: str,
        purpose: str,
        db_tables: set[str],
        procedures: list[dict[str, Any]],
        rules: list[dict[str, Any]],
        controls: list[str] | None = None,
    ) -> str:
        form_token = _clean(form_name).lower()
        if form_token in {"main", "mdiform"}:
            return "Navigation Hub"
        is_generic_form = bool(re.fullmatch(r"(form\d+|frm\d+)", form_token))
        if form_token.endswith("frmsearch") or form_token == "frmsearch":
            return "Record Search"
        if form_token.endswith("frmtransactions") or form_token.endswith("transactions"):
            return "Transaction History"
        if form_token.endswith("frmtransaction") or form_token.endswith("transaction"):
            return "Transaction Entry"
        purpose_low = _clean(purpose).lower()
        if "deposit capture" in purpose_low:
            return "Deposit Capture"
        if "withdrawal processing" in purpose_low:
            return "Withdrawal Processing"
        if "customer profile" in purpose_low:
            return "Customer Management"
        if "transaction ledger" in purpose_low:
            return "Transaction Ledger"
        if "account type maintenance" in purpose_low:
            return "Account Type Maintenance"
        token_blob = " ".join(
            [
                form_token,
                _clean(purpose).lower(),
                " ".join(_clean(x).lower() for x in db_tables if _clean(x)),
                " ".join(_clean(_as_dict(p).get("procedure_name")).lower() for p in procedures),
                " ".join(_clean(_as_dict(r).get("statement")).lower() for r in rules),
                " ".join(_clean(x).lower() for x in (controls or []) if _clean(x)),
            ]
        )
        if any(key in token_blob for key in ("login", "logi", "username", "password", "txtpass", "pass1", "credential")):
            return "Password Management" if any(key in token_blob for key in ("txtpass", "pass1", "credential")) else "Authentication"
        has_strong_transaction_signal = any(
            key in token_blob for key in ("transction", "tbltransaction", "transaction ledger", "ledger")
        )
        # Transaction/ledger semantics should win only on strong signals.
        if has_strong_transaction_signal or ("debit" in token_blob and "credit" in token_blob):
            return "Transaction Ledger"
        if any(key in token_blob for key in ("withdraw", "debit")):
            return "Withdrawal Processing"
        if any(key in token_blob for key in ("deposit", "credit", "balancedt")) and not any(
            key in token_blob for key in ("transaction", "transction", "debit")
        ):
            return "Deposit Capture"
        if any(key in token_blob for key in ("customer", "tblcustomer")) and any(
            key in token_blob for key in ("interest", "min balance", "account type", "acctype")
        ):
            return "Customer Management"
        if any(key in token_blob for key in ("accounttype", "acctype")):
            return "Account Type Maintenance"
        if any(key in token_blob for key in ("customer", "tblcustomer")):
            return "Customer Management"
        if any(key in token_blob for key in ("report", "datareport", "dataenvironment")):
            return "Reporting"
        if any(key in token_blob for key in ("search", "lookup", "find")):
            return "Record Search"
        if any(key in token_blob for key in ("main", "mdiform", "toolbar")):
            return "Navigation Hub"
        if any(key in token_blob for key in ("balance", "tblbalance")):
            return "Balance Inquiry"
        if any(key in token_blob for key in ("timer", "progressbar", "splash")):
            return "Splash/Loading"
        if is_generic_form and form_token == "form9":
            return "Authentication Entry"
        if is_generic_form and form_token == "form1":
            return "Navigation/Menu"
        if is_generic_form and any(key in token_blob for key in ("dated", "datejoined", "dtpicker", "date 1", "from date", "to date")):
            return "Date/Period Entry"
        cleaned_purpose = _clean(purpose).rstrip(".")
        if cleaned_purpose:
            short = re.sub(r"\bworkflow\b", "", cleaned_purpose, flags=re.IGNORECASE)
            short = re.sub(r"\s+", " ", short).strip(" -")
            generic_phrases = {
                "business executed through event-driven ui controls",
                "business workflow executed through event-driven ui controls",
                "application navigation and module routing",
            }
            if short.lower() in generic_phrases:
                return ""
            if short:
                return short
        return ""

    def _display_form_name(form_name: str, alias: str) -> str:
        name = _clean(form_name)
        semantic = _clean(alias)
        if not semantic:
            return name
        generic = bool(re.fullmatch(r"(form\d+|frm\d+)", name.lower()))
        if generic:
            return f"{name} [{semantic}]"
        normalized_name = re.sub(r"[^a-z0-9]+", "", name.lower())
        normalized_alias = re.sub(r"[^a-z0-9]+", "", semantic.lower())
        ambiguous_bank_form = name.lower() in {"frmtransaction", "frmtransactions", "main"}
        if ambiguous_bank_form or (normalized_alias and normalized_alias not in normalized_name):
            return f"{name} [{semantic}]"
        return name

    def _rule_business_meaning(statement: str, category: str) -> str:
        stmt = _clean(statement)
        low = stmt.lower()
        if (
            ("asc(" in low and ("< 46" in low or "<= 45" in low) and ("> 57" in low or ">= 58" in low))
            or (
                ("keyascii" in low or "keyvalue" in low)
                and (">= 48" in low and "<= 57" in low)
            )
        ):
            return "Input is restricted to numeric digits only."
        if re.search(r"keyascii\s*=\s*13", low):
            return "Pressing Enter triggers the same action flow as the primary button."
        if "case keyascii" in low:
            return "Keyboard input routing determines which action path is executed."
        if re.search(r"\.state\s*=\s*1", low):
            return "The action proceeds only when the recordset/connection is active."
        if re.search(r"\.recordcount\s*>\s*0", low):
            return "The action proceeds only when matching records are found."
        if ("max(" in low and "+ 1" in low) or ("max(" in low and "+1" in low):
            return "A new identifier is generated as current maximum value plus one."
        if "computed value rule" in low or "currbalance" in low:
            if "lblbalance.caption" in low:
                return "Balance is recalculated from the displayed balance label and entered amount (UI-derived source)."
            return "Balance is recalculated using the entered amount and current account value."
        if "case button.index" in low or "case buttonmenu.key" in low:
            return "User menu selection routes the workflow to the corresponding module."
        if "threshold decision rule" in low and "if " in low:
            cond = stmt.split("IF", 1)[-1].strip()
            cond = re.sub(r"\s*THEN.*$", "", cond, flags=re.IGNORECASE).strip()
            cond_low = cond.lower()
            if (
                ("asc(" in cond_low and ("< 46" in cond_low or "<= 45" in cond_low) and ("> 57" in cond_low or ">= 58" in cond_low))
                or (
                    ("keyascii" in cond_low or "keyvalue" in cond_low)
                    and (">= 48" in cond_low and "<= 57" in cond_low)
                )
            ):
                return "Input is restricted to numeric digits only."
            return f"The workflow continues only when this condition is true: {cond}."
        if "executes transaction workflow through procedures" in low:
            return "Workflow is orchestrated through UI event handlers and internal procedures."
        if "reads/writes persisted entities" in low:
            return "Form persists and retrieves records from the listed tables."
        if "authenticate users" in low:
            return "User authentication is required before entering the workflow."
        if category.lower() in {"data_persistence", "calculation_logic", "threshold_rule"}:
            return "Business behavior is enforced through data and calculation logic."
        return stmt

    def _business_effect_from_sql(op: str, table: str) -> str:
        op_low = _clean(op).lower()
        table_low = _clean(table).lower()
        if op_low == "select":
            if "balance" in table_low:
                return "Customer balance and account details displayed for review."
            if "customer" in table_low:
                return "Customer details displayed for review."
            if "transction" in table_low or "transaction" in table_low:
                return "Transaction history displayed for review."
            if "accounttype" in table_low:
                return "Account type details displayed for selection."
            if "logi" in table_low or "login" in table_low:
                return "User credentials validated against stored records."
        if "deposit" in table_low:
            return "Deposit transaction recorded."
        if "withdraw" in table_low:
            return "Withdrawal transaction recorded."
        if "balance" in table_low:
            return "Account balance updated."
        if "transction" in table_low or "transaction" in table_low:
            return "Transaction ledger updated."
        if "customer" in table_low:
            if op_low in {"insert", "update", "delete"}:
                return "Customer profile data updated."
        if "accounttype" in table_low:
            return "Account type configuration updated."
        if "logi" in table_low or "login" in table_low:
            return "User authentication record validated."
        if op_low == "insert":
            return f"New record created in {table}."
        if op_low == "update":
            return f"Existing records updated in {table}."
        if op_low == "delete":
            return f"Records deleted from {table}."
        return ""

    def _fallback_business_effects(
        *,
        alias: str,
        purpose: str,
        inputs: set[str],
        db_tables: list[str],
        rules: list[dict[str, Any]],
    ) -> list[str]:
        token_blob = " ".join(
            [
                _clean(alias).lower(),
                _clean(purpose).lower(),
                " ".join(_clean(x).lower() for x in sorted(inputs)),
                " ".join(_clean(x).lower() for x in db_tables),
                " ".join(_clean(_as_dict(r).get("statement")).lower() for r in rules),
            ]
        )
        effects: list[str] = []
        if any(x in token_blob for x in ["deposit", "amount deposited", "credit"]):
            effects.append("Deposit transaction recorded.")
            effects.append("Account balance recalculated.")
        if any(x in token_blob for x in ["withdraw", "amount withdrawn", "debit"]):
            effects.append("Withdrawal transaction recorded.")
            effects.append("Account balance recalculated.")
        if any(x in token_blob for x in ["transaction ledger", "transction", "transaction"]):
            effects.append("Transaction history updated.")
        if any(x in token_blob for x in ["customer management", "customer profile"]):
            effects.append("Customer profile created or updated.")
        if any(x in token_blob for x in ["account type", "acctype", "accounttype"]):
            effects.append("Account type master data maintained.")
        if any(x in token_blob for x in ["authentication", "login", "password"]):
            effects.append("User access is validated before workflow continuation.")
        if any(x in token_blob for x in ["search", "lookup"]):
            effects.append("Matching records displayed to the user.")
        if any(x in token_blob for x in ["navigation hub", "main", "toolbar"]):
            effects.append("Navigation routes the user to selected module screens.")
        if not effects and db_tables:
            if any("balance" in _clean(t).lower() for t in db_tables):
                effects.append("Account balance information refreshed.")
            if any("customer" in _clean(t).lower() for t in db_tables):
                effects.append("Customer details loaded/updated for the selected workflow.")
        # Deduplicate while preserving order
        dedup: list[str] = []
        for eff in effects:
            e = _clean(eff)
            if e and e not in dedup:
                dedup.append(e)
        return dedup[:6]

    def _dossier_business_rule_summary(
        *,
        form_name: str,
        dossier: dict[str, Any] | None,
        rule_rows: list[dict[str, Any]],
    ) -> str:
        d = _as_dict(dossier)
        if not d:
            return ""
        form_controls = _as_list(d.get("controls"))
        input_values: set[str] = set()
        for ctl in form_controls:
            ctl_name = _clean(ctl)
            if not ctl_name:
                continue
            control_id = _clean(ctl_name.split(":", 1)[-1])
            if _is_data_input_control(control_id):
                input_values.add(_to_business_input(control_id))

        db_tables = [_clean(t) for t in _as_list(d.get("db_tables")) if _clean(t)]
        purpose = _clean(d.get("purpose")).rstrip(".")
        alias = _semantic_form_alias(
            form_name=form_name,
            purpose=purpose,
            db_tables=set(db_tables),
            procedures=[],
            rules=rule_rows,
            controls=form_controls,
        )
        effects = _fallback_business_effects(
            alias=alias,
            purpose=purpose,
            inputs=input_values,
            db_tables=db_tables,
            rules=rule_rows,
        )
        parts: list[str] = []
        if input_values:
            parts.append(f"Captures {', '.join(sorted(input_values)[:6])}.")
        if effects:
            parts.append(f"Business outcome: {'; '.join(effects[:3])}.")
        if purpose and "event-driven ui controls" not in purpose.lower():
            parts.insert(0, purpose)
        return " ".join(parts).strip()

    def _base_only_key(form_name: Any) -> str:
        base = _base_form_name(form_name)
        return f"__base__::{base}" if base else ""

    def _form_keys(project_name: Any, form_name: Any) -> list[str]:
        keys: list[str] = []
        scoped = _form_key(project_name, form_name)
        if scoped:
            keys.append(scoped)
        base_key = _base_only_key(form_name)
        if base_key and base_key not in keys:
            keys.append(base_key)
        return keys

    def _lookup_rows(mapping: dict[str, list[dict[str, Any]]], project_name: Any, form_name: Any) -> list[dict[str, Any]]:
        scoped = _form_key(project_name, form_name)
        base_key = _base_only_key(form_name)
        if scoped and scoped in mapping and mapping[scoped]:
            return mapping[scoped]
        if base_key and base_key in mapping and mapping[base_key]:
            return mapping[base_key]
        return []

    def _lookup_set(mapping: dict[str, set[str]], project_name: Any, form_name: Any) -> set[str]:
        scoped = _form_key(project_name, form_name)
        base_key = _base_only_key(form_name)
        if scoped and scoped in mapping and mapping[scoped]:
            return mapping[scoped]
        if base_key and base_key in mapping and mapping[base_key]:
            return mapping[base_key]
        return set()

    def _lookup_control_map(mapping: dict[str, dict[str, str]], project_name: Any, form_name: Any) -> dict[str, str]:
        scoped = _form_key(project_name, form_name)
        base_key = _base_only_key(form_name)
        if scoped and scoped in mapping and mapping[scoped]:
            return mapping[scoped]
        if base_key and base_key in mapping and mapping[base_key]:
            return mapping[base_key]
        return {}

    raw_legacy = _as_dict(raw.get("legacy_inventory") or hv.get("legacy_inventory"))
    projects = _as_list(raw_legacy.get("projects"))

    project_path_by_name: dict[str, str] = {}
    project_dependencies_by_name: dict[str, set[str]] = {}
    project_tables_by_name: dict[str, set[str]] = {}
    for row in raw_landscape:
        r = _as_dict(row)
        raw_id = _clean(r.get("id"))
        path = _clean(r.get("path"))
        left = raw_id.split("|", 1)[0] if "|" in raw_id else raw_id
        names = [left, _clean(r.get("name")), raw_id]
        deps = {_dependency_name(x) for x in _as_list(r.get("dependencies")) if _dependency_name(x)}
        tables = set(_clean(t) for t in _as_list(r.get("db_touchpoints")) if _clean(t))
        for name in names:
            key = _clean(name)
            if not key:
                continue
            if key not in project_path_by_name and path:
                project_path_by_name[key] = path
            project_dependencies_by_name.setdefault(key, set()).update(dep for dep in deps if _clean(dep))
            project_tables_by_name.setdefault(key, set()).update(tables)

    form_sql_rows: dict[str, list[dict[str, Any]]] = {}
    form_db_tables: dict[str, set[str]] = {}
    sql_map_row_key: dict[int, str] = {}
    for row in raw_sql_map:
        r = _as_dict(row)
        tables = set(_clean(t) for t in _as_list(r.get("tables")) if _clean(t))
        project_name = _clean(r.get("variant")) or _project_from_scoped(r.get("form"))
        form_name = _clean(r.get("form_base")) or _clean(r.get("form"))
        keys = _form_keys(project_name, form_name)
        if not keys:
            continue
        sql_map_row_key[id(row)] = keys[0]
        for key in keys:
            form_sql_rows.setdefault(key, []).append(r)
            form_db_tables.setdefault(key, set()).update(tables)
        if project_name:
            project_tables_by_name.setdefault(project_name, set()).update(tables)

    form_event_rows: dict[str, list[dict[str, Any]]] = {}
    event_handler_by_key_proc: dict[tuple[str, str], set[str]] = {}
    form_shared_components: dict[str, set[str]] = {}
    for row in raw_event_map:
        r = _as_dict(row)
        container = _clean(r.get("container")) or _clean(r.get("name"))
        project_name = _project_from_scoped(container) or _project_from_scoped(_as_dict(r.get("handler")).get("symbol"))
        form_name = _base_form_name(container) or _base_form_name(_as_dict(r.get("handler")).get("symbol"))
        keys = _form_keys(project_name, form_name)
        if not keys:
            continue
        for key in keys:
            form_event_rows.setdefault(key, []).append(r)
        for call in [_clean(c) for c in _as_list(r.get("calls")) if _clean(c)]:
            for key in keys:
                event_handler_by_key_proc.setdefault((key, call), set()).add(_clean(_as_dict(r.get("handler")).get("symbol")) or _clean(r.get("entry_id")))

    form_proc_rows: dict[str, list[dict[str, Any]]] = {}
    for row in raw_procedures:
        r = _as_dict(row)
        project_name = _project_from_scoped(r.get("form"))
        form_name = _base_form_name(r.get("form"))
        keys = _form_keys(project_name, form_name)
        for key in keys:
            form_proc_rows.setdefault(key, []).append(r)

    form_risk_rows: dict[str, list[dict[str, Any]]] = {}
    for row in raw_risks:
        r = _as_dict(row)
        texts = [_clean(r.get("description")), _clean(r.get("recommended_action"))]
        for ev in _as_list(r.get("evidence")):
            e = _as_dict(ev)
            texts.append(_clean(_as_dict(e.get("external_ref")).get("ref")))
            texts.append(_clean(_as_dict(e.get("file_span")).get("path")))
        forms: set[str] = set()
        for text in texts:
            for form_name in _extract_forms_from_text(text):
                forms.add(_base_form_name(form_name))
        for dossier in raw_form_dossiers:
            d = _as_dict(dossier)
            if _base_form_name(d.get("form_name")) not in forms:
                continue
            for key in _form_keys(d.get("project_name"), d.get("form_name")):
                form_risk_rows.setdefault(key, []).append(r)

    def _rule_form_bases(rule_row: dict[str, Any]) -> set[str]:
        scope = _as_dict(rule_row.get("scope"))
        candidates: list[str] = [
            _clean(scope.get("form")),
            _clean(scope.get("form_key")),
            _clean(scope.get("component_id")),
            _clean(rule_row.get("form")),
            *[_clean(x) for x in _as_list(scope.get("forms"))],
            *[_clean(x) for x in _as_list(scope.get("form_keys"))],
        ]
        bases: set[str] = set()
        for cand in candidates:
            if not cand:
                continue
            split_tokens = [part.strip() for part in re.split(r"[,;|]", cand) if part.strip()]
            for token in (split_tokens or [cand]):
                raw = _clean(token)
                if not raw:
                    continue
                if "::" in raw:
                    raw = raw.split("::", 1)[1]
                raw = raw.split("/")[-1]
                raw = re.sub(r"\.(frm|ctl|cls|bas)$", "", raw, flags=re.IGNORECASE)
                raw = raw.rstrip(".,;:()[]{}")
                low = raw.lower()
                if not low:
                    continue
                if low.endswith((".bas", ".cls", ".ctl", ".ctx", ".vbp", ".vbg", ".res", ".dsr", ".dca", ".dcx")):
                    continue
                if re.search(r"_(click|change|load|keypress|gotfocus|lostfocus|activate|deactivate)$", low):
                    continue
                if ("frm" not in low and not low.startswith("form") and low not in {"main", "mdiform", "login"}):
                    continue
                base = _base_form_name(raw)
                if base:
                    bases.add(base)
        text_hints = [
            _clean(_as_dict(rule_row.get("scope")).get("component_id")),
            _clean(rule_row.get("statement")),
        ]
        for ev in _as_list(rule_row.get("evidence")):
            e = _as_dict(ev)
            text_hints.append(_clean(_as_dict(e.get("external_ref")).get("ref")))
            text_hints.append(_clean(_as_dict(e.get("file_span")).get("path")))
        for text in text_hints:
            for form_name in _extract_forms_from_text(text):
                base = _base_form_name(form_name)
                if base:
                    bases.add(base)
        return bases

    form_rule_rows: dict[str, list[dict[str, Any]]] = {}
    for row in raw_rules:
        r = _as_dict(row)
        forms = _rule_form_bases(r)
        for dossier in raw_form_dossiers:
            d = _as_dict(dossier)
            if _base_form_name(d.get("form_name")) not in forms:
                continue
            for key in _form_keys(d.get("project_name"), d.get("form_name")):
                form_rule_rows.setdefault(key, []).append(r)

    # Mirror rules across equivalent forms when variants use generic names (e.g., Form3 vs frmDeposits).
    semantic_by_key: dict[str, str] = {}
    for dossier in raw_form_dossiers:
        d = _as_dict(dossier)
        project_name = _clean(d.get("project_name"))
        form_name = _clean(d.get("form_name"))
        if not form_name:
            continue
        proc_rows = _lookup_rows(form_proc_rows, project_name, form_name)
        sql_rows = _lookup_rows(form_sql_rows, project_name, form_name)
        table_hints = {
            _clean(t)
            for item in sql_rows
            for t in _as_list(_as_dict(item).get("tables"))
            if _clean(t)
        }
        alias = _semantic_form_alias(
            form_name=form_name,
            purpose=_clean(d.get("purpose")),
            db_tables=table_hints,
            procedures=proc_rows,
            rules=_lookup_rows(form_rule_rows, project_name, form_name),
            controls=[_clean(x) for x in _as_list(d.get("controls")) if _clean(x)],
        )
        semantic = _clean(alias).lower()
        if semantic:
            for key in _form_keys(project_name, form_name):
                semantic_by_key[key] = semantic

    donor_rules_by_semantic: dict[str, list[dict[str, Any]]] = {}

    def _semantic_group(semantic_name: str) -> str:
        s = _clean(semantic_name).lower()
        if s in {"transaction entry", "transaction history", "transaction ledger"}:
            return "transaction_workflow"
        if s in {"password management", "authentication", "authentication entry"}:
            return "authentication_workflow"
        if s in {"record search", "search"}:
            return "record_search_workflow"
        return s

    allowed_semantics = {
        "deposit capture",
        "withdrawal processing",
        "transaction entry",
        "transaction ledger",
        "transaction history",
        "customer management",
        "account type maintenance",
        "password management",
        "authentication",
        "record search",
    }
    for key, rows in form_rule_rows.items():
        semantic = semantic_by_key.get(key, "")
        if semantic not in allowed_semantics or not rows:
            continue
        semantic_bucket = _semantic_group(semantic)
        donor_rules_by_semantic.setdefault(semantic_bucket, [])
        for r in rows:
            rid = _clean(_as_dict(r).get("rule_id") or _as_dict(r).get("id"))
            existing = {
                _clean(_as_dict(x).get("rule_id") or _as_dict(x).get("id"))
                for x in donor_rules_by_semantic[semantic_bucket]
            }
            if rid and rid in existing:
                continue
            donor_rules_by_semantic[semantic_bucket].append(r)

    for key, semantic in semantic_by_key.items():
        if semantic not in allowed_semantics:
            continue
        if form_rule_rows.get(key):
            continue
        mirrored = donor_rules_by_semantic.get(_semantic_group(semantic), [])
        if mirrored:
            form_rule_rows[key] = mirrored[:8]

    # control lookup for per-form/per-procedure ActiveX trigger checks
    form_control_type_by_key: dict[str, dict[str, str]] = {}
    for row in raw_form_dossiers:
        r = _as_dict(row)
        keys = _form_keys(r.get("project_name"), r.get("form_name"))
        if not keys:
            continue
        for key in keys:
            mapping = form_control_type_by_key.setdefault(key, {})
            for ctl in _as_list(r.get("controls")):
                ctl_text = _clean(ctl)
                if not ctl_text:
                    continue
                if ":" in ctl_text:
                    ctl_type, ctl_name = ctl_text.split(":", 1)
                else:
                    ctl_type, ctl_name = ctl_text, ctl_text
                mapping[_clean(ctl_name).lower()] = _clean(ctl_type)

    # map shared components by form
    shared_module_procs = {
        _clean(_as_dict(proc).get("procedure_name"))
        for proc in raw_procedures
        if _base_form_name(_as_dict(proc).get("form")) == "shared_module"
    }
    for key, events in form_event_rows.items():
        for e in events:
            ev = _as_dict(e)
            for call in [_clean(c) for c in _as_list(ev.get("calls")) if _clean(c)]:
                if call in shared_module_procs:
                    form_shared_components.setdefault(key, set()).add(call)

    # reports/shared tables summary
    table_to_projects: dict[str, set[str]] = {}
    for project_name, tables in project_tables_by_name.items():
        for t in tables:
            table_to_projects.setdefault(t, set()).add(project_name)

    dependency_to_forms: dict[str, set[str]] = {}
    for dossier in raw_form_dossiers:
        d = _as_dict(dossier)
        project_name = _clean(d.get("project_name"))
        qualified = _qualified_form_name(project_name, d.get("form_name"))
        deps = set(project_dependencies_by_name.get(project_name, set()))
        for ctl in _as_list(d.get("controls")):
            ctl_text = _clean(ctl)
            if not ctl_text:
                continue
            ctl_type = ctl_text.split(":", 1)[0]
            if ctl_type and not ctl_type.upper().startswith("VB"):
                deps.add(ctl_type)
        for dep in deps:
            dep_name = _dependency_name(dep)
            if not dep_name:
                continue
            dependency_to_forms.setdefault(dep_name.lower(), set()).add(qualified)

    dossier_by_key: dict[str, dict[str, Any]] = {}
    for dossier in raw_form_dossiers:
        d = _as_dict(dossier)
        for key in _form_keys(d.get("project_name"), d.get("form_name")):
            if key and key not in dossier_by_key:
                dossier_by_key[key] = d

    discovered_forms: list[dict[str, str]] = []
    seen_discovered: set[str] = set()

    def _normalize_discovered_form_name(value: Any) -> str:
        raw_name = _clean(value)
        if not raw_name:
            return ""
        leaf = Path(raw_name).name
        if ":" in leaf:
            left, right = leaf.split(":", 1)
            if _clean(left).lower() in {"form", "mdiform"} and _clean(right):
                leaf = _clean(right)
        return _clean(leaf)

    def _add_discovered(project_name: Any, form_name: Any, source: str) -> None:
        pname = _clean(project_name)
        fname = _normalize_discovered_form_name(form_name)
        if not fname:
            return
        key = _form_key(pname, fname)
        if not key or key in seen_discovered:
            return
        seen_discovered.add(key)
        discovered_forms.append({"project_name": pname, "form_name": fname, "form_key": key, "source": source})

    for proj in projects:
        p = _as_dict(proj)
        pname = _clean(p.get("name"))
        for form_name in _as_list(p.get("forms")):
            _add_discovered(pname, form_name, "project.forms")
        for asset in _as_list(p.get("ui_assets")):
            a = _as_dict(asset)
            if _clean(a.get("kind")).lower() in {"form", "screen"}:
                _add_discovered(pname, a.get("name"), "project.ui_assets")
        for member in _as_list(p.get("members")):
            m = _as_dict(member)
            kind = _clean(m.get("kind")).lower()
            path = _clean(m.get("path"))
            if kind == "form" or path.lower().endswith(".frm"):
                _add_discovered(pname, Path(path).name, "project.members")

    for dossier in raw_form_dossiers:
        d = _as_dict(dossier)
        _add_discovered(d.get("project_name"), d.get("form_name"), "form_dossier")

    orphan_by_key: dict[str, dict[str, Any]] = {}
    orphan_unmapped_count = 0
    for orphan in raw_orphans:
        o = _as_dict(orphan)
        orphan_form = _clean(o.get("form")) or Path(_clean(o.get("path"))).stem
        orphan_project = _clean(o.get("project_name"))
        if orphan_form == "(unmapped_form_files)":
            summary_text = _clean(o.get("behavior_summary"))
            m = re.search(r"(\\d+)\\s+discovered\\s+form\\s+files", summary_text, flags=re.IGNORECASE)
            if m:
                orphan_unmapped_count = int(m.group(1))
            continue
        key = _form_key(orphan_project, orphan_form)
        if key:
            orphan_by_key[key] = o
            _add_discovered(orphan_project, orphan_form, "orphan_analysis")

    lines.append(f"- Legacy inventory: {'present' if raw.get('legacy_inventory') or hv.get('legacy_inventory') else 'missing'}")
    lines.append(f"- Event map rows: {len(raw_event_map) or len(_as_list(hv.get('event_map')))}")
    lines.append(f"- SQL catalog rows: {len(raw_sql) or len(_as_list(hv.get('sql_catalog')))}")
    lines.append(f"- SQL map rows: {len(raw_sql_map)}")
    lines.append(f"- Procedure summaries: {len(raw_procedures)}")
    lines.append(f"- Form dossiers: {len(raw_form_dossiers)}")
    lines.append(f"- Dependency rows: {len(raw_deps) or len(_as_list(hv.get('dependencies')))}")
    lines.append(f"- Business rules: {len(raw_rules) or len(_as_list(hv.get('business_rules')))}")
    lines.append(f"- Risk register rows: {len(raw_risks)}")
    lines.append(f"- Orphan analysis rows: {len(raw_orphans)}")
    lines.append(f"- Repo landscape variants: {len(raw_landscape)}")
    lines.append(f"- Variant inventory rows: {len(raw_variant_inventory)}")
    lines.append(f"- Constitution principles: {len(raw_constitution)}")
    lines.append(f"- MDB inventory rows: {len(mdb_rows)}")
    lines.append(f"- Form LOC profile rows: {len(form_loc_rows)}")
    lines.append(f"- Designer LOC rows: {len(designer_loc_rows)}")
    lines.append(f"- Connection string variants: {len(conn_variant_rows)}")
    lines.append(f"- Module global inventory rows: {len(module_global_rows)}")
    lines.append(f"- Dead form references: {len(dead_form_ref_rows)}")
    lines.append(f"- DataEnvironment report mappings: {len(de_report_rows)}")
    lines.append(f"- Static risk detector findings: {len(static_detector_rows)}")
    lines.append(f"- Source data dictionary rows: {len(source_data_dictionary_rows)}")
    legacy_counts = _as_dict(_as_dict(raw.get("legacy_inventory")).get("summary")).get("counts", {})
    legacy_counts = _as_dict(legacy_counts)
    lines.append(
        "- Source LOC: {} total (forms={}, modules={}, classes={}) across {} file(s)".format(
            _as_int(legacy_counts.get("source_loc_total"), 0),
            _as_int(legacy_counts.get("source_loc_forms"), 0),
            _as_int(legacy_counts.get("source_loc_modules"), 0),
            _as_int(legacy_counts.get("source_loc_classes"), source_loc_classes),
            _as_int(legacy_counts.get("source_files_scanned"), 0),
        )
    )

    include_detailed_appendix = mode != "summary"
    if not include_detailed_appendix:
        return "\n".join(lines)

    raw_legacy = _as_dict(raw.get("legacy_inventory") or hv.get("legacy_inventory"))
    projects = _as_list(raw_legacy.get("projects"))
    event_rows = raw_event_map or _as_list(hv.get("event_map"))
    sql_rows = raw_sql or _as_list(hv.get("sql_catalog"))
    dep_rows = raw_deps or _as_list(hv.get("dependencies"))
    rule_rows = raw_rules or _as_list(hv.get("business_rules"))
    detector_rows = _as_list(_as_dict(raw.get("detector_findings")).get("findings"))
    artifact_index_rows = _as_list(_as_dict(raw.get("artifact_index")).get("artifacts"))

    lines.extend(["", "## Detailed Appendix", "", "### A. Legacy Inventory"])
    lines.append(f"- Projects: {len(projects)}")
    lines.append(f"- Data touchpoints: {', '.join(_as_list(_as_dict(raw_legacy.get('summary')).get('data_touchpoints'))) or 'None detected'}")
    legacy_counts = _as_dict(_as_dict(raw_legacy.get("summary")).get("counts"))
    lines.append(
        "- Source LOC: {} total (forms={}, modules={}, classes={}) across {} file(s)".format(
            _as_int(legacy_counts.get("source_loc_total"), 0),
            _as_int(legacy_counts.get("source_loc_forms"), 0),
            _as_int(legacy_counts.get("source_loc_modules"), 0),
            _as_int(legacy_counts.get("source_loc_classes"), source_loc_classes),
            _as_int(legacy_counts.get("source_files_scanned"), 0),
        )
    )
    if projects:
        lines.append("| Project | Type | Startup | Members | Forms | Reports | Dependencies | Source LOC | Shared tables |")
        lines.append("|---|---|---|---:|---:|---:|---:|---:|---|")
        for project in projects[:250]:
            p = _as_dict(project)
            project_name = _clean(p.get("name") or p.get("project_id"))
            members_rows = _as_list(p.get("members"))
            reports_count = sum(
                1
                for m in members_rows
                if (
                    _clean(_as_dict(m).get("kind")).lower() in {"report", "designer"}
                    or "report" in _clean(_as_dict(m).get("path")).lower()
                    or _clean(_as_dict(m).get("path")).lower().endswith(".dsr")
                )
            )
            shared_tables = sorted(
                t for t in project_tables_by_name.get(project_name, set())
                if len(table_to_projects.get(t, set())) > 1
            )
            lines.append(
                "| {} | {} | {} | {} | {} | {} | {} | {} | {} |".format(
                    _escape_pipe(project_name),
                    _escape_pipe(p.get("type")),
                    _escape_pipe(p.get("startup")),
                    len(members_rows),
                    len(_as_list(p.get("ui_assets"))),
                    reports_count,
                    len(_as_list(p.get("dependencies"))),
                    _as_int(p.get("source_loc_total"), 0),
                    _escape_pipe(", ".join(shared_tables[:8]) or "none"),
                )
            )
    else:
        lines.append("- No project rows available.")

    lines.extend(["", "### B. Dependency Inventory"])
    if dep_rows:
        lines.append("| Name | Kind | GUID / Reference | Risk | Recommended action | Forms mapped |")
        lines.append("|---|---|---|---|---|---|")
        for dep in dep_rows[:500]:
            d = _as_dict(dep)
            risk = _clean(_as_dict(d.get("risk")).get("tier") or d.get("tier") or "unknown")
            action = _clean(_as_dict(d.get("risk")).get("recommended_action") or d.get("recommended_action")) or "n/a"
            dep_name = _dependency_name(d.get("name") or dep)
            guid_ref = _clean(
                d.get("reference")
                or d.get("guid")
                or d.get("guid_reference")
                or d.get("clsid")
                or d.get("progid")
            ) or "n/a"
            mapped_forms = sorted(dependency_to_forms.get(dep_name.lower(), set()))
            lines.append(
                f"| {_escape_pipe(dep_name)} | {_escape_pipe(d.get('kind'))} | {_escape_pipe(guid_ref)} | {_escape_pipe(risk)} | {_escape_pipe(action)} | {_escape_pipe(', '.join(mapped_forms[:6]) or 'n/a')} |"
            )
    else:
        lines.append("- No dependency rows available.")

    lines.extend(["", "### C. Event Map"])
    if event_rows:
        lines.append("| Entry | Container | Trigger | Calls | Side effects |")
        lines.append("|---|---|---|---|---|")
        for entry in event_rows[:600]:
            e = _as_dict(entry)
            calls = ", ".join(_as_list(e.get("calls") or e.get("procedure_calls"))[:4])
            side_effects = ", ".join(
                _as_list(_as_dict(e.get("side_effects")).get("tables_or_files") or e.get("sql_touches"))[:4]
            )
            lines.append(
                "| {} | {} | {} | {} | {} |".format(
                    _escape_pipe(e.get("name") or e.get("entry_id") or e.get("event_handler")),
                    _escape_pipe(e.get("container") or e.get("form")),
                    _escape_pipe(_as_dict(e.get("trigger")).get("event") or e.get("event")),
                    _escape_pipe(calls or "n/a"),
                    _escape_pipe(side_effects or "n/a"),
                )
            )
    else:
        lines.append("- No event map rows available.")

    lines.extend(["", "### D. SQL Catalog"])
    if sql_rows:
        lines.append("| SQL ID | Kind | Tables | Query |")
        lines.append("|---|---|---|---|")
        for row in sql_rows[:700]:
            r = _as_dict(row)
            lines.append(
                "| {} | {} | {} | {} |".format(
                    _escape_pipe(r.get("sql_id") or "n/a"),
                    _escape_pipe(r.get("kind") or "unknown"),
                    _escape_pipe(", ".join(_as_list(r.get("tables"))[:6]) or "n/a"),
                    _escape_pipe(r.get("raw") or row),
                )
            )
    else:
        lines.append("- No SQL rows available.")

    lines.extend(["", "### D1. Source DB Column Schema"])
    if source_data_dictionary_rows:
        lines.append("| Table | Column | Type | FK Ref | Confidence | Access Evidence | Business Meaning | Evidence Ref |")
        lines.append("|---|---|---|---|---:|---|---|---|")
        sorted_rows = sorted(
            [_as_dict(row) for row in source_data_dictionary_rows if isinstance(row, dict)],
            key=lambda row: (_clean(row.get("table_name")).lower(), _clean(row.get("column_name")).lower()),
        )
        for row in sorted_rows[:4000]:
            access = _as_dict(row.get("access_patterns"))
            evidence_sql_ids = [_clean(x) for x in _as_list(row.get("evidence_sql_ids")) if _clean(x)]
            if access:
                access_text = (
                    f"SELECT={_as_int(access.get('select_count'), 0)}, "
                    f"INSERT={_as_int(access.get('insert_count'), 0)}, "
                    f"UPDATE={_as_int(access.get('update_count'), 0)}"
                )
            else:
                access_text = f"SQL refs={len(evidence_sql_ids)}"
            lines.append(
                "| {} | {} | {} | {} | {:.2f} | {} | {} | {} |".format(
                    _escape_pipe(row.get("table_name") or row.get("table") or "n/a"),
                    _escape_pipe(row.get("column_name") or row.get("column") or "n/a"),
                    _escape_pipe(row.get("inferred_type") or "n/a"),
                    _escape_pipe(row.get("fk_reference") or "n/a"),
                    _as_float(row.get("confidence"), 0.0),
                    _escape_pipe(access_text),
                    _escape_pipe(row.get("business_meaning") or "n/a"),
                    _escape_pipe(", ".join(_as_list(row.get("evidence_refs"))[:4]) or ", ".join(evidence_sql_ids[:4]) or "n/a"),
                )
            )
    else:
        lines.append("- No source data dictionary rows available.")

    lines.extend(["", "### E. Business Rules"])
    if rule_rows:
        lines.append("| Rule ID | Form | Layer | Category | Business Meaning | Implementation Evidence | Risk links |")
        lines.append("|---|---|---|---|---|---|---|")
        known_form_bases = {
            _base_form_name(_as_dict(d).get("form_name"))
            for d in raw_form_dossiers
            if _base_form_name(_as_dict(d).get("form_name"))
        }
        variant_projects_by_base: dict[str, set[str]] = {}
        for dossier in raw_form_dossiers:
            d = _as_dict(dossier)
            base = _base_form_name(d.get("form_name"))
            proj = _clean(d.get("project_name"))
            if base and proj:
                variant_projects_by_base.setdefault(base, set()).add(proj)
        rules_by_form: dict[str, list[dict[str, str]]] = {}
        raw_rules_by_form: dict[str, list[dict[str, Any]]] = {}
        seen_rule_form_pairs: set[tuple[str, str]] = set()
        seen_source_variant_pairs: set[tuple[str, str]] = set()
        emitted_form_labels: set[str] = set()
        emitted_qualified_form_labels: set[str] = set()
        existing_rule_numbers: list[int] = []
        used_output_rule_ids: set[str] = set()
        saturated_meaning_templates = {
            "Balance is recalculated using the entered amount and current account value.",
            "The action proceeds only when the recordset/connection is active.",
        }
        canonical_meaning_rule_ids: dict[str, str] = {}
        saturated_meaning_forms: dict[str, set[str]] = {}
        saturated_suppressed_count: dict[str, int] = {}

        def _rule_forms_from_row(rule_row: dict[str, Any], evidence_text: str) -> list[str]:
            scope = _as_dict(rule_row.get("scope"))
            forms_out: list[str] = []
            candidates: list[Any] = [
                scope.get("form"),
                scope.get("form_key"),
                scope.get("component_id"),
                rule_row.get("form"),
            ]
            candidates.extend(_as_list(scope.get("forms")))
            candidates.extend(_as_list(scope.get("form_keys")))
            for cand in candidates:
                raw_token = _clean(cand)
                if not raw_token:
                    continue
                split_tokens = [part.strip() for part in re.split(r"[,;|]", raw_token) if part.strip()]
                for token in (split_tokens or [raw_token]):
                    if "::" in token:
                        token = token.split("::", 1)[1]
                    token = token.split("/")[-1]
                    token = re.sub(r"\.(frm|ctl|cls|bas)$", "", token, flags=re.IGNORECASE)
                    token = token.rstrip(".,;:()[]{}")
                    token_low = token.lower()
                    if not token_low:
                        continue
                    if token_low.endswith((".bas", ".cls", ".ctl", ".ctx", ".vbp", ".vbg", ".res", ".dsr", ".dca", ".dcx")):
                        continue
                    if re.search(r"_(click|change|load|keypress|gotfocus|lostfocus|activate|deactivate)$", token_low):
                        continue
                    normalized_base = _base_form_name(token)
                    if (
                        "frm" not in token_low
                        and not token_low.startswith("form")
                        and token_low not in {"main", "mdiform", "login"}
                        and normalized_base not in known_form_bases
                    ):
                        continue
                    normalized_display = "main" if token_low == "main" else token
                    if normalized_display not in forms_out:
                        forms_out.append(normalized_display)
            for source in [_clean(rule_row.get("statement")), evidence_text]:
                for form_name in _extract_forms_from_text(source):
                    normalized_display = "main" if _base_form_name(form_name) == "main" else form_name
                    if normalized_display not in forms_out:
                        forms_out.append(normalized_display)
            return forms_out

        def _next_mirrored_rule_id() -> str:
            next_num = (max(existing_rule_numbers) + 1) if existing_rule_numbers else 1
            while f"br-{next_num:03d}" in used_output_rule_ids:
                next_num += 1
            existing_rule_numbers.append(next_num)
            rule_id = f"BR-{next_num:03d}"
            used_output_rule_ids.add(rule_id.lower())
            return rule_id

        def _allocate_rule_id(source_rule_id: str) -> tuple[str, str]:
            sid = _clean(source_rule_id)
            if sid and re.fullmatch(r"BR-\d+", sid, flags=re.IGNORECASE) and sid.lower() not in used_output_rule_ids:
                used_output_rule_ids.add(sid.lower())
                return sid, ""
            generated = _next_mirrored_rule_id()
            return generated, sid

        def _canonicalize_saturated_meaning(meaning_text: str, rule_id: str, form_label: str) -> tuple[str, bool, str]:
            if meaning_text not in saturated_meaning_templates:
                return meaning_text, False, rule_id
            first_rule = canonical_meaning_rule_ids.get(meaning_text)
            if not first_rule:
                canonical_meaning_rule_ids[meaning_text] = rule_id
                saturated_meaning_forms.setdefault(meaning_text, set()).add(form_label)
                saturated_suppressed_count.setdefault(meaning_text, 0)
                return meaning_text, False, rule_id
            saturated_meaning_forms.setdefault(meaning_text, set()).add(form_label)
            saturated_suppressed_count[meaning_text] = int(saturated_suppressed_count.get(meaning_text, 0)) + 1
            return meaning_text, True, first_rule

        def _pick_backfill_rule(candidates: list[dict[str, Any]]) -> dict[str, Any]:
            if not candidates:
                return {}
            preferred: dict[str, Any] = {}
            for row in candidates:
                r = _as_dict(row)
                category = _clean(r.get("category") or r.get("rule_type") or "other")
                meaning = _rule_business_meaning(_clean(r.get("statement")), category)
                if meaning and meaning not in saturated_meaning_templates:
                    return r
                if not preferred:
                    preferred = r
            return preferred

        for row in rule_rows[:700]:
            r = _as_dict(row)
            evidence = ""
            if isinstance(r.get("evidence"), list):
                evidence = ", ".join(
                    [
                        _clean(_as_dict(e).get("external_ref", {}).get("ref") or _as_dict(e).get("file_span", {}).get("path") or _as_dict(e).get("ref"))
                        for e in _as_list(r.get("evidence"))
                    ][:3]
                )
            else:
                evidence = _clean(r.get("evidence"))
            rule_forms = _rule_forms_from_row(r, evidence) or ["n/a"]
            category = _clean(r.get("category") or r.get("rule_type") or "other")
            layer = "Presentation"
            evidence_low = f"{_clean(r.get('statement'))} {evidence}".lower()
            if category.lower() in {"data_persistence", "calculation_logic", "threshold_rule"} or any(x in evidence_low for x in ["select ", "insert ", "update ", "delete ", "table"]):
                layer = "Data"
            if any(x in evidence_low for x in [".bas", "module", "shared"]):
                layer = "Shared"
            related_risk_ids: set[str] = set()
            rule_base_forms = {_base_form_name(f) for f in rule_forms if _base_form_name(f)}
            for dossier in raw_form_dossiers:
                d = _as_dict(dossier)
                if _base_form_name(d.get("form_name")) not in rule_base_forms:
                    continue
                for risk_row in _lookup_rows(form_risk_rows, d.get("project_name"), d.get("form_name")):
                    rid = _clean(_as_dict(risk_row).get("risk_id"))
                    if rid:
                        related_risk_ids.add(rid)
            rule_low = _clean(r.get("statement")).lower()
            for risk_row in raw_risks:
                rr = _as_dict(risk_row)
                desc = _clean(rr.get("description")).lower()
                rid = _clean(rr.get("risk_id"))
                if not rid:
                    continue
                if any(token in desc and token in rule_low for token in ["caption", "balance", "customerid", "delete", "injection", "credential", "password"]):
                    related_risk_ids.add(rid)
            meaning = _rule_business_meaning(_clean(r.get("statement")), category)
            source_rule_id = _clean(r.get("rule_id") or r.get("id") or "n/a")
            num_match = re.search(r"(\d+)$", source_rule_id)
            if num_match:
                existing_rule_numbers.append(int(num_match.group(1)))
            for form_item in rule_forms[:16]:
                form_display = "main" if _base_form_name(form_item) == "main" else _clean(form_item) or "n/a"
                base_form = _base_form_name(form_display) or _clean(form_display).lower() or "n/a"
                is_generic_base = bool(re.fullmatch(r"(form\d+|frm\d+)", base_form))
                if "::" not in form_display and is_generic_base and len(variant_projects_by_base.get(base_form, set())) > 1:
                    # Emit qualified variant rows instead of ambiguous bare generic form names.
                    continue
                pair = (source_rule_id.lower(), base_form.lower())
                if pair in seen_rule_form_pairs:
                    continue
                seen_rule_form_pairs.add(pair)
                row_meaning = meaning
                if ("splash" in _clean(form_display).lower() or "splash" in evidence.lower()) and "balance is recalculated" in row_meaning.lower():
                    row_meaning = "Splash/loading behavior advances progress state before opening workflow screens."
                output_rule_id, source_hint = _allocate_rule_id(source_rule_id)
                row_meaning, saturated_suppressed, saturated_anchor_rule = _canonicalize_saturated_meaning(
                    row_meaning,
                    output_rule_id,
                    form_display,
                )
                if saturated_suppressed:
                    rules_by_form.setdefault(base_form, []).append({"rule_id": saturated_anchor_rule, "meaning": row_meaning})
                    raw_rules_by_form.setdefault(base_form, []).append(r)
                    continue
                evidence_out = evidence or "n/a"
                if source_hint and source_hint.lower() != output_rule_id.lower():
                    evidence_out = f"{evidence_out}; source_rule={source_hint}" if evidence_out != "n/a" else f"source_rule={source_hint}"
                lines.append(
                    "| {} | {} | {} | {} | {} | {} | {} |".format(
                        _escape_pipe(output_rule_id),
                        _escape_pipe(form_display),
                        _escape_pipe(layer),
                        _escape_pipe(category),
                        _escape_pipe(row_meaning),
                        _escape_pipe(evidence_out),
                        _escape_pipe(", ".join(sorted(related_risk_ids)[:6]) or "none"),
                    )
                )
                emitted_form_labels.add(_qualified_form_name("", form_display).lower())
                if "::" in form_display:
                    emitted_qualified_form_labels.add(_clean(form_display).lower())
                rules_by_form.setdefault(base_form, []).append({"rule_id": output_rule_id, "meaning": row_meaning})
                raw_rules_by_form.setdefault(base_form, []).append(r)

        for dossier in raw_form_dossiers:
            d = _as_dict(dossier)
            project_name = _clean(d.get("project_name"))
            form_name = _clean(d.get("form_name"))
            base_name = _base_form_name(form_name)
            if not base_name:
                continue
            qualified_form = _qualified_form_name(project_name, "main" if _base_form_name(form_name) == "main" else form_name)
            if qualified_form.lower() in emitted_form_labels:
                continue
            mirrored_rows = _lookup_rows(form_rule_rows, project_name, form_name)
            for mr in mirrored_rows[:10]:
                m = _as_dict(mr)
                source_rule_id = _clean(m.get("rule_id") or m.get("id") or "n/a")
                source_pair = (source_rule_id.lower(), qualified_form.lower())
                if source_pair in seen_source_variant_pairs:
                    continue
                seen_source_variant_pairs.add(source_pair)
                category = _clean(m.get("category") or m.get("rule_type") or "other")
                statement = _clean(m.get("statement"))
                meaning = _rule_business_meaning(statement, category)
                layer = "Data" if category.lower() in {"data_persistence", "calculation_logic", "threshold_rule"} else "Presentation"
                related_risk_ids = {
                    _clean(_as_dict(risk_row).get("risk_id"))
                for risk_row in _lookup_rows(form_risk_rows, project_name, form_name)
                    if _clean(_as_dict(risk_row).get("risk_id"))
                }
                mirrored_rule_id, _ = _allocate_rule_id(source_rule_id)
                mirrored_meaning, saturated_suppressed, saturated_anchor_rule = _canonicalize_saturated_meaning(
                    meaning or statement or "n/a",
                    mirrored_rule_id,
                    qualified_form,
                )
                if saturated_suppressed:
                    rules_by_form.setdefault(base_name, []).append({"rule_id": saturated_anchor_rule, "meaning": mirrored_meaning})
                    raw_rules_by_form.setdefault(base_name, []).append(m)
                    continue
                evidence = f"mirrored_from_variant_mapping (source={source_rule_id or 'n/a'})"
                lines.append(
                    "| {} | {} | {} | {} | {} | {} | {} |".format(
                        _escape_pipe(mirrored_rule_id),
                        _escape_pipe(qualified_form),
                        _escape_pipe(layer),
                        _escape_pipe(category),
                        _escape_pipe(mirrored_meaning),
                        _escape_pipe(evidence),
                        _escape_pipe(", ".join(sorted(related_risk_ids)[:6]) or "none"),
                    )
                )
                emitted_form_labels.add(qualified_form.lower())
                emitted_qualified_form_labels.add(qualified_form.lower())
                rules_by_form.setdefault(base_name, []).append({"rule_id": mirrored_rule_id, "meaning": mirrored_meaning})
                raw_rules_by_form.setdefault(base_name, []).append(m)

        # E/Q synchronization backfill: if Q can see rules for a qualified form but E has no
        # qualified row, emit one deterministic qualified rule row.
        for dossier in raw_form_dossiers:
            d = _as_dict(dossier)
            project_name = _clean(d.get("project_name"))
            form_name = _clean(d.get("form_name"))
            base_name = _base_form_name(form_name)
            if not base_name:
                continue
            qualified_form = _qualified_form_name(project_name, "main" if base_name == "main" else form_name)
            if not qualified_form or qualified_form.lower() in emitted_qualified_form_labels:
                continue
            candidate_rows = _lookup_rows(form_rule_rows, project_name, form_name)
            if not candidate_rows:
                continue
            chosen = _pick_backfill_rule(candidate_rows)
            if not chosen:
                continue
            source_rule_id = _clean(chosen.get("rule_id") or chosen.get("id") or "n/a")
            category = _clean(chosen.get("category") or chosen.get("rule_type") or "other")
            statement = _clean(chosen.get("statement"))
            layer = "Data" if category.lower() in {"data_persistence", "calculation_logic", "threshold_rule"} else "Presentation"
            backfill_meaning = _rule_business_meaning(statement, category) or statement or "n/a"
            backfill_rule_id, source_hint = _allocate_rule_id(source_rule_id)
            related_risk_ids = {
                _clean(_as_dict(risk_row).get("risk_id"))
                for risk_row in _lookup_rows(form_risk_rows, project_name, form_name)
                if _clean(_as_dict(risk_row).get("risk_id"))
            }
            evidence = f"variant_backfill_for_eq_sync (source={source_rule_id or 'n/a'})"
            if source_hint and source_hint.lower() != backfill_rule_id.lower():
                evidence = f"{evidence}; source_rule={source_hint}"
            lines.append(
                "| {} | {} | {} | {} | {} | {} | {} |".format(
                    _escape_pipe(backfill_rule_id),
                    _escape_pipe(qualified_form),
                    _escape_pipe(layer),
                    _escape_pipe(category),
                    _escape_pipe(backfill_meaning),
                    _escape_pipe(evidence),
                    _escape_pipe(", ".join(sorted(related_risk_ids)[:6]) or "none"),
                )
            )
            emitted_form_labels.add(qualified_form.lower())
            emitted_qualified_form_labels.add(qualified_form.lower())
            rules_by_form.setdefault(base_name, []).append({"rule_id": backfill_rule_id, "meaning": backfill_meaning})
            raw_rules_by_form.setdefault(base_name, []).append(chosen)

        if rules_by_form:
            dossier_by_base_form: dict[str, dict[str, Any]] = {}
            for dossier in raw_form_dossiers:
                d = _as_dict(dossier)
                key = _base_form_name(d.get("form_name"))
                if key and key not in dossier_by_base_form:
                    dossier_by_base_form[key] = d
            lines.extend(["", "### E1. Rule Cross-Reference by Form"])
            for form_name in sorted(rules_by_form.keys())[:220]:
                rows = rules_by_form.get(form_name, [])
                rule_ids = ", ".join(sorted({_clean(x.get("rule_id")) for x in rows if _clean(x.get("rule_id"))})[:8]) or "n/a"
                summaries: list[str] = []
                for entry in rows:
                    row_meaning = _clean(entry.get("meaning"))
                    if row_meaning and row_meaning not in summaries:
                        summaries.append(row_meaning)
                dossier_summary = _dossier_business_rule_summary(
                    form_name=form_name,
                    dossier=dossier_by_base_form.get(form_name),
                    rule_rows=raw_rules_by_form.get(form_name, []),
                )
                if dossier_summary and dossier_summary not in summaries:
                    summaries.insert(0, dossier_summary)
                lines.append(
                    "- {}: rule_ids=[{}]; summary={}".format(
                        _escape_pipe(form_name),
                        _escape_pipe(rule_ids),
                        _escape_pipe(" / ".join(summaries[:3]) or "n/a"),
                    )
                )
        shared_rows = [
            (meaning, canonical_meaning_rule_ids.get(meaning), int(saturated_suppressed_count.get(meaning, 0)), sorted(saturated_meaning_forms.get(meaning, set())))
            for meaning in saturated_meaning_templates
            if int(saturated_suppressed_count.get(meaning, 0)) > 0
        ]
        if shared_rows:
            lines.extend(["", "### E2. Shared Rule Consolidation"])
            for meaning, anchor_rule, suppressed_count, forms in shared_rows:
                preview_forms = ", ".join(forms[:12]) if forms else "n/a"
                suffix = f" (+{len(forms) - 12} more)" if len(forms) > 12 else ""
                lines.append(
                    "- {}: consolidated {} duplicate row(s); applies to {} form(s): {}{}".format(
                        _clean(anchor_rule) or "n/a",
                        suppressed_count,
                        len(forms),
                        _escape_pipe(preview_forms),
                        _escape_pipe(suffix),
                    )
                )
                lines.append(f"  - Canonical meaning: {_escape_pipe(meaning)}")
    else:
        lines.append("- No business rules available.")

    lines.extend(["", "### F. Detector Findings"])
    if detector_rows:
        lines.append("| Detector | Severity | Count | Summary | Required actions |")
        lines.append("|---|---|---:|---|---|")
        for row in detector_rows[:500]:
            r = _as_dict(row)
            summary = _clean(r.get("summary") or "")
            if imported_analysis_mode:
                summary = _humanize_imported_evidence_summary(summary)
            actions = ", ".join(_as_list(r.get("required_actions"))[:4]) or "n/a"
            if imported_analysis_mode:
                actions = _humanize_imported_evidence_action(actions if actions != "n/a" else "", summary)
            lines.append(
                "| {} | {} | {} | {} | {} |".format(
                    _escape_pipe(r.get("detector_id") or "n/a"),
                    _escape_pipe(r.get("severity") or "medium"),
                    int(r.get("count") or 0),
                    _escape_pipe(summary),
                    _escape_pipe(actions),
                )
            )
    else:
        lines.append("- No detector findings available.")

    lines.extend(["", "### G. Artifact Index"])
    if artifact_index_rows:
        lines.append("| Type | Ref |")
        lines.append("|---|---|")
        for row in artifact_index_rows[:200]:
            r = _as_dict(row)
            lines.append(f"| {_escape_pipe(r.get('type'))} | {_escape_pipe(r.get('ref'))} |")
    else:
        lines.append("- No artifact index entries.")

    lines.extend(["", "### H. SQL Map"])
    if raw_sql_map:
        lines.append("| Form | Procedure | Operation | Tables | Risks | activex_trigger | trace_complete |")
        lines.append("|---|---|---|---|---|---|---|")
        for row in raw_sql_map[:700]:
            r = _as_dict(row)
            project_name = _clean(r.get("variant")) or _project_from_scoped(r.get("form"))
            form_name = _clean(r.get("form_base")) or _clean(r.get("form"))
            proc_rows_for_form = _lookup_rows(form_proc_rows, project_name, form_name)
            db_tables_for_form = sorted(_lookup_set(form_db_tables, project_name, form_name))
            alias = _semantic_form_alias(
                form_name=form_name,
                purpose=_clean(_as_dict(dossier_by_key.get(_form_key(project_name, form_name)) or {}).get("purpose")),
                db_tables=set(db_tables_for_form),
                procedures=proc_rows_for_form,
                rules=_lookup_rows(form_rule_rows, project_name, form_name),
                controls=[],
            )
            related_events = [
                _as_dict(e)
                for e in _lookup_rows(form_event_rows, project_name, form_name)
                if _clean(r.get("procedure")) and _clean(r.get("procedure")) in _clean(_as_dict(e.get("handler")).get("symbol"))
            ]
            control_map = _lookup_control_map(form_control_type_by_key, project_name, form_name)
            activex_hits: list[str] = []
            for e in related_events:
                trigger_control = _clean(_as_dict(e.get("trigger")).get("control"))
                if not trigger_control:
                    continue
                ctl_type = _clean(control_map.get(trigger_control.lower()))
                if ctl_type and not ctl_type.upper().startswith("VB"):
                    activex_hits.append(f"{trigger_control}:{ctl_type}")
            has_sql = bool(_clean(r.get("sql_id")))
            has_tables = bool(_as_list(r.get("tables")))
            trace_complete = has_sql and has_tables
            lines.append(
                "| {} | {} | {} | {} | {} | {} | {} |".format(
                    _escape_pipe(_qualified_form_name(project_name, _display_form_name(form_name, alias))),
                    _escape_pipe(r.get("procedure") or "n/a"),
                    _escape_pipe(r.get("operation") or "unknown"),
                    _escape_pipe(", ".join(_as_list(r.get("tables"))[:6]) or "n/a"),
                    _escape_pipe(", ".join(_as_list(r.get("risk_flags"))[:6]) or "none"),
                    _escape_pipe(", ".join(sorted(set(activex_hits))[:4]) or "n/a"),
                    "yes" if trace_complete else "no",
                )
            )
    else:
        lines.append("- No SQL map rows available.")

    lines.extend(["", "### I. Handler and Procedure Summaries"])
    if raw_procedures:
        lines.append("| Callable | Kind | Form | SQL IDs | Steps | Risks | Source line refs |")
        lines.append("|---|---|---|---|---|---|---|")
        for row in raw_procedures[:700]:
            r = _as_dict(row)
            proc_name = _clean(r.get("procedure_name") or r.get("procedure_id") or "n/a")
            kind = _callable_kind(proc_name, r.get("form"), _clean(_as_dict(r.get("trigger")).get("event")))
            line_refs: list[str] = []
            for ev in _as_list(r.get("evidence")):
                e = _as_dict(ev)
                fs = _as_dict(e.get("file_span"))
                path = _clean(fs.get("path"))
                ln = _as_int(fs.get("line_start"), 0)
                if path and ln > 0:
                    ref = f"{path}:{ln}"
                    if ref not in line_refs:
                        line_refs.append(ref)
            # Fallback from event map rows when procedure evidence is sparse.
            form_name = _clean(r.get("form"))
            form_project = _project_from_scoped(form_name)
            form_base = _base_form_name(form_name)
            for event_row in _lookup_rows(form_event_rows, form_project, form_base):
                er = _as_dict(event_row)
                symbol = _clean(_as_dict(er.get("handler")).get("symbol"))
                if proc_name and proc_name not in symbol:
                    continue
                path = _clean(er.get("source_file"))
                ln = _as_int(er.get("line"), 0)
                if path and ln > 0:
                    ref = f"{path}:{ln}"
                    if ref not in line_refs:
                        line_refs.append(ref)
            lines.append(
                "| {} | {} | {} | {} | {} | {} | {} |".format(
                    _escape_pipe(proc_name),
                    _escape_pipe(kind),
                    _escape_pipe(r.get("form") or "n/a"),
                    _escape_pipe(", ".join(_as_list(r.get("sql_ids"))[:6]) or "n/a"),
                    _escape_pipe(" / ".join(_as_list(r.get("steps"))[:2]) or "n/a"),
                    _escape_pipe(", ".join(_as_list(r.get("risks"))[:5]) or "none"),
                    _escape_pipe(", ".join(line_refs[:4]) or "n/a"),
                )
            )
    else:
        lines.append("- No procedure summaries available.")

    lines.extend(["", "### J. Delivery Constitution"])
    if raw_constitution:
        lines.extend([f"- {_clean(x)}" for x in raw_constitution[:100]])
    else:
        lines.append("- No delivery constitution principles available.")

    lines.extend(["", "### K. Form Dossiers"])
    if discovered_forms:
        lines.append("| Form | Display Name | Project | form_type | Status | Purpose | Inputs (data) | Outputs (effects) | ActiveX used | DB tables | Actions | Coverage | Confidence | Exclusion reason |")
        lines.append("|---|---|---|---|---|---|---|---|---|---|---:|---:|---:|---|")
        excluded_rows: list[dict[str, str]] = []
        for form_ref in sorted(discovered_forms, key=lambda x: (_clean(x.get("project_name")), _clean(x.get("form_name")).lower()))[:900]:
            project_name = _clean(form_ref.get("project_name"))
            form_name = _clean(form_ref.get("form_name"))
            form_key = _clean(form_ref.get("form_key")) or _form_key(project_name, form_name)
            base_key = _base_only_key(form_name)
            dossier = _as_dict(dossier_by_key.get(form_key) or dossier_by_key.get(base_key))
            orphan_row = _as_dict(orphan_by_key.get(form_key) or orphan_by_key.get(base_key))

            status = "mapped"
            exclusion_reason = "none"
            if not dossier:
                if orphan_row:
                    status = "orphan"
                    exclusion_reason = _clean(orphan_row.get("recommendation")) or _clean(orphan_row.get("reason")) or "orphaned_form"
                else:
                    status = "excluded"
                    exclusion_reason = "missing_from_form_dossier"

            proc_rows = _lookup_rows(form_proc_rows, project_name, form_name)
            sql_rows_for_form = _lookup_rows(form_sql_rows, project_name, form_name)
            form_rules = _lookup_rows(form_rule_rows, project_name, form_name)
            form_controls = _as_list(dossier.get("controls")) if dossier else []

            db_tables_set = _lookup_set(form_db_tables, project_name, form_name)
            if not db_tables_set:
                db_tables_set = {
                    _clean(t)
                    for sql_row in sql_rows_for_form
                    for t in _as_list(_as_dict(sql_row).get("tables"))
                    if _clean(t)
                }
            db_tables = sorted(db_tables_set)

            purpose = _clean(dossier.get("purpose"))
            alias = _semantic_form_alias(
                form_name=form_name or "n/a",
                purpose=purpose,
                db_tables=set(db_tables),
                procedures=proc_rows,
                rules=form_rules,
                controls=form_controls,
            )
            if not purpose and alias:
                purpose = f"{alias} workflow."
            display_name = _display_form_name(form_name or "n/a", alias)

            input_values: set[str] = set()
            for ctl in form_controls:
                ctl_name = _clean(ctl)
                if not ctl_name:
                    continue
                control_id = _clean(ctl_name.split(":", 1)[-1])
                if _is_data_input_control(control_id):
                    input_values.add(_to_business_input(control_id))
            for proc in proc_rows:
                for item in _as_list(_as_dict(proc).get("inputs")):
                    token = _clean(item)
                    if _is_data_input_control(token):
                        input_values.add(_to_business_input(token))

            output_values: set[str] = set()
            for sql_row in sql_rows_for_form:
                s = _as_dict(sql_row)
                op = _clean(s.get("operation")).lower()
                tables = [_clean(t) for t in _as_list(s.get("tables")) if _clean(t)]
                if not tables:
                    continue
                for table in tables:
                    effect = _business_effect_from_sql(op, table)
                    if effect:
                        output_values.add(effect)
            if not output_values:
                for proc in proc_rows:
                    for table in _as_list(_as_dict(proc).get("data_mutations")):
                        if _clean(table):
                            effect = _business_effect_from_sql("update", _clean(table))
                            if effect:
                                output_values.add(effect)
                if not output_values:
                    for effect in _fallback_business_effects(
                        alias=alias,
                        purpose=purpose,
                        inputs=input_values,
                        db_tables=db_tables,
                        rules=form_rules,
                    ):
                        output_values.add(effect)

            form_activex: set[str] = set()
            for ctl in form_controls:
                ctl_name = _clean(ctl)
                if not ctl_name:
                    continue
                prefix = ctl_name.split(":", 1)[0]
                if prefix and not prefix.upper().startswith("VB"):
                    form_activex.add(prefix)
            for dep in project_dependencies_by_name.get(project_name, set()):
                dep_name = _clean(dep)
                if dep_name.lower().endswith((".ocx", ".dll")) or "MSCOM" in dep_name.upper() or "MSFLEX" in dep_name.upper():
                    form_activex.add(dep_name)

            form_type = _infer_form_type(
                form_name=form_name or "n/a",
                purpose=purpose,
                procedures=proc_rows,
                controls=form_controls,
                tables=set(db_tables),
            )
            coverage = float(_as_dict(dossier.get("coverage")).get("coverage_score") or 0)
            raw_conf = float(_as_dict(dossier.get("coverage")).get("confidence_score") or 0)
            action_count = len(_as_list(dossier.get("actions"))) if dossier else len(proc_rows)
            generic_purpose = _clean(purpose).lower() in {
                "business workflow executed through event-driven ui controls.",
                "business workflow executed through event-driven ui controls",
                "potential orphan flow detected.",
                "potential orphan flow detected",
            }
            coverage_clamped = max(0.0, min(1.0, coverage))
            confidence = 0.22 + (0.45 * coverage_clamped)
            confidence += min(0.14, 0.02 * action_count)
            confidence += min(0.08, 0.015 * len(proc_rows))
            confidence += min(0.08, 0.02 * len(db_tables))
            confidence += 0.09 if sql_rows_for_form else -0.08
            confidence += 0.08 if not generic_purpose else -0.12
            if not input_values:
                confidence -= 0.06
            if action_count == 0:
                confidence -= 0.16
            if bool(re.fullmatch(r"(form\d+|frm\d+)", form_name.lower())) and not _clean(alias):
                confidence -= 0.08
            if 0 < raw_conf <= 1 and abs(raw_conf - 0.92) > 1e-4:
                confidence = (confidence * 0.8) + (raw_conf * 0.2)
            confidence = max(0.1, min(0.98, confidence))

            if status != "mapped":
                excluded_rows.append(
                    {
                        "form": _qualified_form_name(project_name, form_name),
                        "reason": exclusion_reason,
                        "source": _clean(form_ref.get("source")) or "detected",
                    }
                )

            lines.append(
                "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {:.2f} | {:.2f} | {} |".format(
                    _escape_pipe(form_name or "n/a"),
                    _escape_pipe(display_name or "n/a"),
                    _escape_pipe(_project_label(project_name, project_path_by_name)),
                    _escape_pipe(form_type),
                    _escape_pipe(status),
                    _escape_pipe(purpose or "n/a"),
                    _escape_pipe(", ".join(sorted(input_values)[:8]) or "n/a"),
                    _escape_pipe(", ".join(sorted(output_values)[:8]) or "n/a"),
                    _escape_pipe(", ".join(sorted(form_activex)[:6]) or "n/a"),
                    _escape_pipe(", ".join(db_tables[:8]) or "n/a"),
                    action_count,
                    coverage,
                    confidence,
                    _escape_pipe(exclusion_reason),
                )
            )

        summary_counts = _as_dict(_as_dict(raw_legacy.get("summary")).get("counts"))
        expected_forms = int(summary_counts.get("forms_or_screens") or 0)
        rendered_forms = len(discovered_forms)
        if expected_forms > rendered_forms or orphan_unmapped_count > 0:
            gap_text = (
                f"expected_forms={expected_forms}, rendered_forms={rendered_forms}, "
                f"unmapped_form_files={orphan_unmapped_count or max(0, expected_forms - rendered_forms)}"
            )
            lines.append("")
            lines.append(f"- Coverage note: {gap_text}. Unmapped/placeholder forms are listed below.")

        if excluded_rows or orphan_unmapped_count > 0:
            lines.extend(["", "#### K1. Excluded/Unresolved Forms", "| Form | Reason | Source |", "|---|---|---|"])
            for row in excluded_rows[:400]:
                lines.append(
                    "| {} | {} | {} |".format(
                        _escape_pipe(row.get("form") or "n/a"),
                        _escape_pipe(row.get("reason") or "excluded"),
                        _escape_pipe(row.get("source") or "detected"),
                    )
                )
            if orphan_unmapped_count > 0:
                lines.append(
                    "| (unmapped_form_files) | reconcile_project_membership ({} unresolved form files) | orphan_analysis |".format(
                        orphan_unmapped_count
                    )
                )
    else:
        lines.append("- No form dossier rows available.")

    lines.extend(["", "### L. Risk Register"])
    if raw_risks:
        lines.append("| Risk ID | Severity | Description | Recommended action |")
        lines.append("|---|---|---|---|")
        for row in raw_risks[:700]:
            r = _as_dict(row)
            description = _clean(r.get("description") or "")
            action = _clean(r.get("recommended_action") or "")
            if imported_analysis_mode:
                description = _humanize_imported_evidence_summary(description)
                action = _humanize_imported_evidence_action(action, description)
            lines.append(
                "| {} | {} | {} | {} |".format(
                    _escape_pipe(r.get("risk_id") or "n/a"),
                    _escape_pipe(r.get("severity") or "medium"),
                    _escape_pipe(description),
                    _escape_pipe(action),
                )
            )
    else:
        lines.append("- No risk register rows available.")

    lines.extend(["", "### M. Orphan Analysis"])
    if raw_orphans:
        lines.append("| Path | SQL IDs | Tables touched | Recommendation |")
        lines.append("|---|---|---|---|")
        for row in raw_orphans[:500]:
            r = _as_dict(row)
            lines.append(
                "| {} | {} | {} | {} |".format(
                    _escape_pipe(r.get("path") or "n/a"),
                    _escape_pipe(", ".join(_as_list(r.get("sql_ids"))[:6]) or "n/a"),
                    _escape_pipe(", ".join(_as_list(r.get("tables_touched"))[:6]) or "n/a"),
                    _escape_pipe(r.get("recommendation") or "verify"),
                )
            )
    else:
        lines.append("- No orphan analysis rows available.")

    lines.extend(["", "### N. Repository Landscape and Variant Inventory"])
    if raw_landscape:
        lines.append("| Variant | Path | Startup | Forms | Members | Dependencies |")
        lines.append("|---|---|---|---:|---:|---:|")
        for row in raw_landscape[:200]:
            r = _as_dict(row)
            counts = _as_dict(r.get("counts"))
            lines.append(
                "| {} | {} | {} | {} | {} | {} |".format(
                    _escape_pipe(((_clean(r.get("id")).split("|", 1)[0] if "|" in _clean(r.get("id")) else _clean(r.get("id"))) or "variant")),
                    _escape_pipe(r.get("path") or ""),
                    _escape_pipe(r.get("startup") or ""),
                    int(counts.get("forms") or 0),
                    int(counts.get("members") or 0),
                    int(counts.get("dependencies") or 0),
                )
            )
    else:
        lines.append("- No repository landscape rows available.")

    if raw_variant_inventory:
        lines.extend(["", "| Variant | Forms | Modules | Tables touched | Dependency summary |", "|---|---:|---:|---:|---|"])
        for row in raw_variant_inventory[:200]:
            r = _as_dict(row)
            dep_summary = _as_dict(r.get("dependencies_summary"))
            lines.append(
                "| {} | {} | {} | {} | total={}, ocx={}, dll={} |".format(
                    _escape_pipe(r.get("name") or r.get("id") or "variant"),
                    len(_as_list(r.get("forms"))),
                    len(_as_list(r.get("modules"))),
                    len(_as_list(r.get("tables_touched"))),
                    int(dep_summary.get("total") or 0),
                    int(dep_summary.get("ocx") or 0),
                    int(dep_summary.get("dll") or 0),
                )
            )

    lines.extend(["", "### O. Project Dependency Map"])
    dependency_rows: list[dict[str, str]] = []
    seen_dependency_keys: set[tuple[str, str, str, str]] = set()

    for entry in raw_event_map:
        e = _as_dict(entry)
        source = _clean(e.get("container") or e.get("form") or e.get("name")) or "n/a"
        source_trigger = _clean(_as_dict(e.get("trigger")).get("control"))
        calls = [_clean(c) for c in _as_list(e.get("calls")) if _clean(c)]
        for call in calls:
            dep_type = ""
            call_norm = _clean(call)
            call_low = call_norm.lower()
            if call in shared_module_procs:
                dep_type = "shared_module_call"
            elif (
                "main" in source.lower()
                or "toolbar" in source.lower()
                or "toolbar" in source_trigger.lower()
            ) and call.lower().startswith(("frm", "form", "rpt", "datareport")):
                if call_low.startswith(("rpt", "datareport")):
                    dep_type = "report_navigation"
                elif call_low in {"frm", "form"}:
                    dep_type = "mdi_navigation_unresolved"
                    call_norm = f"{call_norm} [Unresolved]"
                else:
                    dep_type = "mdi_navigation"
            if not dep_type:
                continue
            stable_evidence = _clean(_as_dict(e.get("handler")).get("symbol")) or f"{source}->{call}"
            key = (source, call, dep_type, stable_evidence)
            if key in seen_dependency_keys:
                continue
            seen_dependency_keys.add(key)
            blocks_sprint = "Sprint 1"
            if dep_type == "report_navigation":
                blocks_sprint = "Sprint 2"
            if dep_type == "mdi_navigation_unresolved":
                blocks_sprint = "n/a (unresolved)"
            if dep_type == "cross_variant_schema_conflict":
                blocks_sprint = "Sprint 0"
            dependency_rows.append(
                {
                    "from": source,
                    "to": call_norm,
                    "type": dep_type,
                    "evidence": stable_evidence,
                    "blocks_sprint": blocks_sprint,
                }
            )

    schema_divergence = _as_dict(raw_variant_diff.get("schema_divergence"))
    for pair in _as_list(schema_divergence.get("blocking_pairs") or schema_divergence.get("pairs")):
        p = _as_dict(pair)
        left = _clean(p.get("left_project"))
        right = _clean(p.get("right_project"))
        if not left or not right:
            continue
        alias_count = len(_as_list(p.get("alias_mismatches")))
        near_miss_count = len(_as_list(p.get("near_miss_names")))
        txn_conflict = bool(p.get("transaction_schema_conflict"))
        evidence = f"alias_mismatches={alias_count}, near_miss={near_miss_count}, transaction_conflict={'yes' if txn_conflict else 'no'}"
        key = (left, right, "cross_variant_schema_conflict", evidence)
        if key in seen_dependency_keys:
            continue
        seen_dependency_keys.add(key)
        dependency_rows.append(
            {
                "from": left,
                "to": right,
                "type": "cross_variant_schema_conflict",
                "evidence": evidence,
                "blocks_sprint": "Sprint 0",
            }
        )

    if dependency_rows:
        lines.append("| From | To | Type | Evidence | Blocks Sprint |")
        lines.append("|---|---|---|---|---|")
        for row in dependency_rows[:800]:
            lines.append(
                "| {} | {} | {} | {} | {} |".format(
                    _escape_pipe(row.get("from")),
                    _escape_pipe(row.get("to")),
                    _escape_pipe(row.get("type")),
                    _escape_pipe(row.get("evidence")),
                    _escape_pipe(row.get("blocks_sprint") or "Sprint 1"),
                )
            )
    else:
        lines.append("- No project dependency rows available.")

    lines.extend(["", "### O1. Form User Flow (Spec-Kit Style)"])
    known_forms: set[str] = set()
    for d in discovered_forms:
        name = _clean(_as_dict(d).get("form_name")).lower()
        if name:
            known_forms.add(name)
    for row in raw_form_dossiers:
        name = _clean(_as_dict(row).get("form_name")).lower()
        if name:
            known_forms.add(name)

    flow_graph: dict[str, dict[str, set[str]]] = {}

    def _flow_note(entry: dict[str, Any]) -> str:
        trig = _as_dict(entry.get("trigger"))
        parts = [
            _clean(trig.get("control")),
            _clean(trig.get("event")),
            _clean(_as_dict(entry.get("handler")).get("symbol")),
        ]
        for p in parts:
            if p:
                return p
        return ""

    for entry in raw_event_map:
        e = _as_dict(entry)
        source = _clean(e.get("container") or e.get("form") or e.get("name")) or "n/a"
        source_low = source.lower()
        note = _flow_note(e)
        calls = [_clean(c) for c in _as_list(e.get("calls")) if _clean(c)]
        for call in calls:
            target = _clean(call)
            target_low = target.lower()
            if not target:
                continue
            if target in shared_module_procs:
                continue

            nav_target = ""
            if target_low in {"end", "quit", "app.end", "endapp"}:
                nav_target = "End"
            elif target_low in {"frm", "form"}:
                nav_target = "frm [Unresolved]"
            elif target_low.startswith(("rpt", "datareport")):
                nav_target = target
            elif target_low.startswith(("frm", "form")) or target_low == "main" or target_low in known_forms:
                nav_target = target
            else:
                continue

            if source_low == nav_target.lower():
                continue
            flow_graph.setdefault(source, {}).setdefault(nav_target, set())
            if note:
                flow_graph[source][nav_target].add(note)

    if flow_graph:
        def _sort_targets(values: list[str]) -> list[str]:
            return sorted(
                values,
                key=lambda x: (
                    2 if x == "End" else (1 if x.startswith("frm [Unresolved]") else 0),
                    x.lower(),
                ),
            )

        for source in sorted(flow_graph.keys(), key=lambda x: x.lower()):
            lines.append(f"{source}")
            targets = _sort_targets(list(flow_graph[source].keys()))
            for idx, target in enumerate(targets):
                is_last = idx == len(targets) - 1
                branch = "'- ->" if is_last else "|- ->"
                notes = sorted(flow_graph[source][target])
                suffix = f" [via {notes[0]}]" if notes else ""
                lines.append(f"  {branch} {target}{suffix}")
            lines.append("")
    else:
        lines.append("- No explicit form-to-form navigation links detected.")

    lines.extend(["", "### P. Form Flow Traces"])
    if raw_form_dossiers:
        for row in raw_form_dossiers[:250]:
            r = _as_dict(row)
            form_name = _clean(r.get("form_name")) or "n/a"
            project_name = _clean(r.get("project_name"))
            form_controls = _as_list(r.get("controls"))
            control_map = _lookup_control_map(form_control_type_by_key, project_name, form_name)

            events = _lookup_rows(form_event_rows, project_name, form_name)
            procedures = _lookup_rows(form_proc_rows, project_name, form_name)
            sql_entries = _lookup_rows(form_sql_rows, project_name, form_name)
            procedure_names: set[str] = set()
            for proc in procedures:
                name = _clean(_as_dict(proc).get("procedure_name"))
                if name:
                    procedure_names.add(name)
            for sql_entry in sql_entries:
                name = _clean(_as_dict(sql_entry).get("procedure"))
                if name:
                    procedure_names.add(name)

            lines.extend(
                [
                    f"#### {form_name} ({_project_label(project_name, project_path_by_name)})",
                    "| Callable | Kind | Event | ActiveX | SQL IDs | Tables | Source line refs | Trace status |",
                    "|---|---|---|---|---|---|---|---|",
                ]
            )

            if not procedure_names:
                lines.append(
                    "| n/a | n/a | n/a | {} | n/a | {} | n/a | TRACE_GAP |".format(
                        "n/a",
                        _escape_pipe(", ".join(sorted(_lookup_set(form_db_tables, project_name, form_name))[:8]) or "n/a"),
                    )
                )
                continue

            for proc_name in sorted(procedure_names)[:120]:
                related_events = [
                    _as_dict(e)
                    for e in events
                    if proc_name in _clean(_as_dict(e).get("handler", {}).get("symbol"))
                ]
                related_sql = [
                    _as_dict(s)
                    for s in sql_entries
                    if _clean(_as_dict(s).get("procedure")) == proc_name
                ]
                activex_hits: list[str] = []
                for e in related_events:
                    trigger_control = _clean(_as_dict(e.get("trigger")).get("control"))
                    ctl_type = _clean(control_map.get(trigger_control.lower()))
                    if trigger_control and ctl_type and not ctl_type.upper().startswith("VB"):
                        activex_hits.append(f"{trigger_control}:{ctl_type}")
                line_refs: list[str] = []
                for e in related_events:
                    source_file = _clean(_as_dict(e).get("source_file"))
                    line_no = _as_int(_as_dict(e).get("line"), 0)
                    if source_file and line_no > 0:
                        ref = f"{source_file}:{line_no}"
                        if ref not in line_refs:
                            line_refs.append(ref)
                    for ev in _as_list(_as_dict(_as_dict(e).get("handler")).get("evidence")):
                        fs = _as_dict(_as_dict(ev).get("file_span"))
                        path = _clean(fs.get("path"))
                        ln = _as_int(fs.get("line_start"), 0)
                        if path and ln > 0:
                            ref = f"{path}:{ln}"
                            if ref not in line_refs:
                                line_refs.append(ref)
                sql_ids = sorted({_clean(x.get("sql_id")) for x in related_sql if _clean(x.get("sql_id"))})
                table_names: set[str] = set()
                for sql_row in related_sql:
                    table_names.update(_clean(t) for t in _as_list(sql_row.get("tables")) if _clean(t))
                trace_ok = bool(sql_ids) and bool(table_names)
                lines.append(
                    "| {} | {} | {} | {} | {} | {} | {} | {} |".format(
                        _escape_pipe(proc_name),
                        _escape_pipe(_callable_kind(proc_name, form_name)),
                        _escape_pipe(", ".join(_clean(_as_dict(e.get("handler")).get("symbol")) for e in related_events[:3]) or "n/a"),
                        _escape_pipe(", ".join(sorted(set(activex_hits))[:5]) or "n/a"),
                        _escape_pipe(", ".join(sql_ids[:6]) or "n/a"),
                        _escape_pipe(", ".join(sorted(table_names)[:8]) or "n/a"),
                        _escape_pipe(", ".join(line_refs[:4]) or "n/a"),
                        "OK" if trace_ok else "TRACE_GAP",
                    )
                )
    else:
        lines.append("- No form dossiers available for flow traces.")

    lines.extend(["", "### Q. Form Traceability Matrix"])
    traceability_rows: list[dict[str, Any]] = []
    if raw_form_dossiers:
        lines.append("| Form | Project | Source LOC | has_event_map | has_sql_map | has_business_rules | has_risk_entry | completeness_score | missing_links |")
        lines.append("|---|---|---:|---|---|---|---|---:|---|")
        seen_form_keys: set[str] = set()
        for row in raw_form_dossiers[:400]:
            r = _as_dict(row)
            form_name = _clean(r.get("form_name")) or "n/a"
            project_name = _clean(r.get("project_name"))
            form_key = _form_key(project_name, form_name)
            if form_key in seen_form_keys:
                continue
            seen_form_keys.add(form_key)
            has_event = bool(_lookup_rows(form_event_rows, project_name, form_name))
            has_sql = bool(_lookup_rows(form_sql_rows, project_name, form_name))
            has_rules = bool(_lookup_rows(form_rule_rows, project_name, form_name))
            has_risk = bool(_lookup_rows(form_risk_rows, project_name, form_name))
            has_proc = bool(_lookup_rows(form_proc_rows, project_name, form_name))
            missing: list[str] = []
            if not has_event:
                missing.append("event_map")
            if not has_sql:
                missing.append("sql_map")
            if not has_rules:
                missing.append("business_rules")
            if not has_risk:
                missing.append("risk_register")
            if not has_proc:
                missing.append("procedure_summary")
            completeness_score = int((int(has_event) + int(has_sql) + int(has_rules) + int(has_risk) + int(has_proc)) * 20)
            lines.append(
                "| {} | {} | {} | {} | {} | {} | {} | {} | {} |".format(
                    _escape_pipe(_qualified_form_name(project_name, form_name)),
                    _escape_pipe(_project_label(project_name, project_path_by_name)),
                    _as_int(r.get("source_loc"), 0),
                    "yes" if has_event else "no",
                    "yes" if has_sql else "no",
                    "yes" if has_rules else "no",
                    "yes" if has_risk else "no",
                    completeness_score,
                    _escape_pipe(", ".join(missing) or "none"),
                )
            )
            traceability_rows.append(
                {
                    "form_name": form_name,
                    "project_name": project_name,
                    "has_event": has_event,
                    "has_sql": has_sql,
                    "has_rules": has_rules,
                    "has_risk": has_risk,
                    "form_key": form_key,
                    "qualified_form": _qualified_form_name(project_name, form_name),
                    "completeness_score": completeness_score,
                    "missing": missing,
                    "risk_ids": [
                        _clean(_as_dict(rr).get("risk_id"))
                        for rr in _lookup_rows(form_risk_rows, project_name, form_name)[:4]
                        if _clean(_as_dict(rr).get("risk_id"))
                    ],
                }
            )
    else:
        lines.append("- No traceability rows available.")

    lines.extend(["", "### R. Sprint Dependency Map"])
    if traceability_rows:
        lines.append("| Form | Suggested sprint | Depends on | Shared Components Required | Rationale |")
        lines.append("|---|---|---|---|---|")
        variant_gate_needed = bool(raw_variant_diff.get("decision_required"))
        sorted_rows = sorted(
            traceability_rows,
            key=lambda row: (
                len(_as_list(row.get("missing"))),
                0 if _as_list(row.get("risk_ids")) else 1,
                _clean(row.get("qualified_form")),
            ),
            reverse=True,
        )
        emitted: set[str] = set()
        for row in sorted_rows[:400]:
            form_key = _clean(row.get("form_key"))
            if not form_key or form_key in emitted:
                continue
            emitted.add(form_key)
            missing = _as_list(row.get("missing"))
            risk_ids = _as_list(row.get("risk_ids"))
            depends_on: list[str] = []
            if variant_gate_needed:
                depends_on.append("DEC-VARIANT-001")
            if "sql_map" in missing:
                depends_on.append("Q.sql_map")
            if "event_map" in missing:
                depends_on.append("Q.event_map")
            if "business_rules" in missing:
                depends_on.append("Q.business_rules")
            if risk_ids:
                depends_on.extend(risk_ids[:2])

            if "event_map" in missing or "sql_map" in missing:
                sprint = "Sprint 0 (Discovery closure)"
                rationale = "Close traceability gaps before modernization changes."
            elif risk_ids:
                sprint = "Sprint 1 (Risk-first modernization)"
                rationale = "Implement remediation-first changes for high-risk legacy behavior."
            else:
                sprint = "Sprint 2 (Parity hardening)"
                rationale = "Complete hardening, regression validation, and release evidence for production readiness."

            shared_required = sorted(_lookup_set(form_shared_components, row.get("project_name"), row.get("form_name")))
            lines.append(
                "| {} | {} | {} | {} | {} |".format(
                    _escape_pipe(_clean(row.get("qualified_form")) or _clean(row.get("form_name"))),
                    _escape_pipe(sprint),
                    _escape_pipe(", ".join(depends_on) or "none"),
                    _escape_pipe(", ".join(shared_required[:5]) or "none"),
                    _escape_pipe(rationale),
                )
            )
    else:
        lines.append("- No sprint dependency rows available.")

    lines.extend(["", "### S. MDB Inventory"])
    if mdb_rows:
        mdb_summary = _as_dict(raw_mdb_inventory.get("summary"))
        lines.append(
            "- Databases detected: {} | forms referenced: {} | module refs: {}".format(
                _as_int(mdb_summary.get("database_files_detected"), len(mdb_rows)),
                _as_int(mdb_summary.get("forms_with_db_refs"), 0),
                _as_int(mdb_summary.get("module_refs"), 0),
            )
        )
        lines.append("| DB ID | Path | Name | Ext | LOC proxy | Detected from | Referenced by forms | Referenced by modules | Evidence refs |")
        lines.append("|---|---|---|---|---:|---|---|---|---|")
        for row in mdb_rows[:500]:
            r = _as_dict(row)
            lines.append(
                "| {} | {} | {} | {} | {} | {} | {} | {} | {} |".format(
                    _escape_pipe(r.get("db_id") or "n/a"),
                    _escape_pipe(r.get("path") or "n/a"),
                    _escape_pipe(r.get("name") or "n/a"),
                    _escape_pipe(r.get("extension") or "n/a"),
                    _as_int(r.get("source_loc_proxy"), 0),
                    _escape_pipe(", ".join(_as_list(r.get("detected_from"))[:8]) or "n/a"),
                    _escape_pipe(", ".join(_as_list(r.get("referenced_by_forms"))[:8]) or "n/a"),
                    _escape_pipe(", ".join(_as_list(r.get("referenced_by_modules"))[:8]) or "n/a"),
                    _escape_pipe(", ".join(_as_list(r.get("evidence_refs"))[:6]) or "n/a"),
                )
            )
    else:
        lines.append("- No MDB/ACCDB files detected in this run.")

    lines.extend(["", "### T. Form LOC Profile"])
    if form_loc_rows or raw_form_dossiers:
        loc_summary = _as_dict(raw_form_loc_profile.get("summary"))

        def _canon_project(value: Any) -> str:
            text = _clean(value).strip().lower()
            return text or "n/a"

        def _canon_form(value: Any) -> str:
            return _base_form_name(value) or "n/a"

        active_loc_candidates = [
            _as_dict(r) for r in form_loc_rows
            if str(_as_dict(r).get("active_or_orphan") or "").strip().lower() == "active"
            or bool(_as_dict(r).get("in_vbp"))
        ]
        orphan_loc_candidates = [
            _as_dict(r) for r in form_loc_rows
            if not (
                str(_as_dict(r).get("active_or_orphan") or "").strip().lower() == "active"
                or bool(_as_dict(r).get("in_vbp"))
            )
        ]

        loc_by_exact: dict[tuple[str, str], dict[str, Any]] = {}
        loc_by_form: dict[str, list[dict[str, Any]]] = {}
        for row in active_loc_candidates + orphan_loc_candidates:
            key = (_canon_project(row.get("project")), _canon_form(row.get("form") or row.get("base_form")))
            loc_by_exact.setdefault(key, row)
            loc_by_form.setdefault(key[1], []).append(row)

        def _pick_loc_row(project_name: Any, form_name: Any) -> dict[str, Any]:
            exact = loc_by_exact.get((_canon_project(project_name), _canon_form(form_name)))
            if exact:
                return exact
            options = loc_by_form.get(_canon_form(form_name), [])
            if not options:
                return {}
            resolved = [r for r in options if _canon_project(r.get("project")) not in {"n/a", "(unmapped)"}]
            return resolved[0] if resolved else options[0]

        canonical_active_loc_rows: list[dict[str, Any]] = []
        for dossier in raw_form_dossiers:
            d = _as_dict(dossier)
            loc_row = _pick_loc_row(d.get("project_name"), d.get("form_name"))
            loc_value = _as_int(d.get("source_loc"), _as_int(loc_row.get("loc"), 0))
            confidence_value = _as_float(_as_dict(d.get("coverage")).get("confidence_score"), _as_float(loc_row.get("confidence"), 0.0))
            canonical_active_loc_rows.append(
                {
                    "form_id": _clean(d.get("dossier_id")) or f"form_loc:{_clean(d.get('form_name'))}",
                    "form": _qualified_form_name(d.get("project_name"), d.get("form_name")),
                    "base_form": _clean(d.get("form_name")) or "n/a",
                    "project": _clean(d.get("project_name")) or "n/a",
                    "source_file": _clean(loc_row.get("source_file")) or "n/a",
                    "loc": loc_value,
                    "in_vbp": True,
                    "active_or_orphan": "active",
                    "confidence": confidence_value,
                    "evidence": _clean(d.get("dossier_id")) or "n/a",
                }
            )

        canonical_orphan_loc_rows: list[dict[str, Any]] = []
        for row in orphan_loc_candidates:
            r = _as_dict(row)
            canonical_orphan_loc_rows.append(
                {
                    "form_id": _clean(r.get("form_id")) or f"form_loc:{_clean(r.get('form') or r.get('base_form'))}",
                    "form": _clean(r.get("form")) or _clean(r.get("base_form")) or "n/a",
                    "base_form": _clean(r.get("base_form")) or _clean(r.get("form")) or "n/a",
                    "project": _clean(r.get("project")) or "n/a",
                    "source_file": _clean(r.get("source_file")) or "n/a",
                    "loc": _as_int(r.get("loc"), 0),
                    "in_vbp": False,
                    "active_or_orphan": "orphan",
                    "confidence": _as_float(r.get("confidence"), 0.0),
                    "evidence": _clean(r.get("form_id")) or "n/a",
                }
            )

        canonical_form_loc_rows = canonical_active_loc_rows + canonical_orphan_loc_rows
        lines.append(
            "- Forms discovered: {} | active: {} | orphan: {} | canonical active forms LOC total: {} | designer LOC total: {}".format(
                len(canonical_form_loc_rows),
                len(canonical_active_loc_rows),
                len(canonical_orphan_loc_rows),
                sum(_as_int(r.get("loc"), 0) for r in canonical_active_loc_rows),
                _as_int(loc_summary.get("designer_loc_total"), 0),
            )
        )
        lines.append("| Form ID | Form | Base form | Project | Source file | LOC | In VBP | Active/Orphan | Confidence | Evidence |")
        lines.append("|---|---|---|---|---|---:|---|---|---:|---|")
        for row in canonical_form_loc_rows[:2000]:
            r = _as_dict(row)
            lines.append(
                "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |".format(
                    _escape_pipe(r.get("form_id") or "n/a"),
                    _escape_pipe(r.get("form") or "n/a"),
                    _escape_pipe(r.get("base_form") or "n/a"),
                    _escape_pipe(r.get("project") or "n/a"),
                    _escape_pipe(r.get("source_file") or "n/a"),
                    _as_int(r.get("loc"), 0),
                    "yes" if bool(r.get("in_vbp")) else "no",
                    _escape_pipe(r.get("active_or_orphan") or "n/a"),
                    "{:.2f}".format(_as_float(r.get("confidence"), 0.0)),
                    _escape_pipe(f"{_clean(r.get('form_id') or 'n/a')} | conf {_as_float(r.get('confidence'), 0.0):.2f}"),
                )
            )
    else:
        lines.append("- No form LOC profile rows available.")

    lines.extend(["", "### T1. Designer LOC Profile"])
    if designer_loc_rows:
        lines.append("| File | Kind | LOC |")
        lines.append("|---|---|---:|")
        for row in designer_loc_rows[:2000]:
            r = _as_dict(row)
            lines.append(
                "| {} | {} | {} |".format(
                    _escape_pipe(r.get("file") or "n/a"),
                    _escape_pipe(r.get("kind") or "designer"),
                    _as_int(r.get("loc"), 0),
                )
            )
    else:
        lines.append("- No designer LOC rows available.")

    lines.extend(["", "### U. Connection String Variants"])
    if conn_variant_rows:
        conn_summary = _as_dict(raw_connection_variants.get("summary"))
        lines.append(
            "- Variants: {} | relative-path risks: {} | embedded-credential risks: {}".format(
                _as_int(conn_summary.get("variant_count"), len(conn_variant_rows)),
                _as_int(conn_summary.get("relative_path_risks"), 0),
                _as_int(conn_summary.get("embedded_credential_risks"), 0),
            )
        )
        lines.append("| Variant ID | Normalized pattern | Risk flags | Source refs | Example |")
        lines.append("|---|---|---|---|---|")
        for row in conn_variant_rows[:600]:
            r = _as_dict(row)
            lines.append(
                "| {} | {} | {} | {} | {} |".format(
                    _escape_pipe(r.get("variant_id") or "n/a"),
                    _escape_pipe(r.get("normalized_pattern") or "n/a"),
                    _escape_pipe(", ".join(_as_list(r.get("risk_flags"))[:6]) or "none"),
                    _escape_pipe(", ".join(_as_list(r.get("source_refs"))[:8]) or "n/a"),
                    _escape_pipe(_as_list(r.get("examples"))[0] if _as_list(r.get("examples")) else "n/a"),
                )
            )
    else:
        lines.append("- No connection-string variants detected.")

    lines.extend(["", "### V. Module Global Inventory"])
    if module_global_rows:
        globals_summary = _as_dict(raw_module_globals.get("summary"))
        lines.append(
            "- Modules: {} | global candidates: {} | extraction status: {}".format(
                _as_int(globals_summary.get("module_count"), len(module_rows)),
                _as_int(globals_summary.get("global_candidates"), len(module_global_rows)),
                _clean(globals_summary.get("extraction_status") or "unknown"),
            )
        )
        lines.append("| Symbol | Declared type | Scope | Inferred purpose | Evidence refs |")
        lines.append("|---|---|---|---|---|")
        for row in module_global_rows[:1500]:
            r = _as_dict(row)
            lines.append(
                "| {} | {} | {} | {} | {} |".format(
                    _escape_pipe(r.get("symbol") or "n/a"),
                    _escape_pipe(r.get("declared_type") or "n/a"),
                    _escape_pipe(r.get("scope") or "n/a"),
                    _escape_pipe(r.get("inferred_purpose") or "n/a"),
                    _escape_pipe(", ".join(_as_list(r.get("evidence_refs"))[:8]) or "n/a"),
                )
            )
    else:
        lines.append("- No module global inventory rows available.")

    lines.extend(["", "### V1. Module Inventory"])
    if module_rows:
        lines.append("| Module |")
        lines.append("|---|")
        for row in module_rows[:1500]:
            r = _as_dict(row)
            lines.append(f"| {_escape_pipe(r.get('module') or 'n/a')} |")
    else:
        lines.append("- No module inventory rows available.")

    lines.extend(["", "### W. Dead Form References"])
    if dead_form_ref_rows:
        dead_summary = _as_dict(raw_dead_form_refs.get("summary"))
        lines.append(
            "- Unresolved references: {} | callers impacted: {}".format(
                _as_int(dead_summary.get("unresolved_reference_count"), len(dead_form_ref_rows)),
                _as_int(dead_summary.get("callers_impacted"), 0),
            )
        )
        lines.append("| Ref ID | Caller form | Caller handler | Target token | Status | Rationale | Evidence ref |")
        lines.append("|---|---|---|---|---|---|---|")
        for row in dead_form_ref_rows[:1500]:
            r = _as_dict(row)
            lines.append(
                "| {} | {} | {} | {} | {} | {} | {} |".format(
                    _escape_pipe(r.get("ref_id") or "n/a"),
                    _escape_pipe(r.get("caller_form") or "n/a"),
                    _escape_pipe(r.get("caller_handler") or "n/a"),
                    _escape_pipe(r.get("target_token") or "n/a"),
                    _escape_pipe(r.get("status") or "n/a"),
                    _escape_pipe(r.get("rationale") or "n/a"),
                    _escape_pipe(r.get("evidence_ref") or "n/a"),
                )
            )
    else:
        lines.append("- No dead-form references detected.")

    lines.extend(["", "### X. DataEnvironment Report Mapping"])
    if de_report_rows:
        de_summary = _as_dict(raw_de_report_map.get("summary"))
        designer_assets = _as_dict(raw_de_report_map.get("designer_assets"))
        lines.append(
            "- DataEnvironments: {} | reports: {} | mapped calls: {}".format(
                _as_int(de_summary.get("dataenvironment_count"), len(_as_list(designer_assets.get("dataenvironments")))),
                _as_int(de_summary.get("report_object_count"), len(_as_list(designer_assets.get("reports")))),
                _as_int(de_summary.get("mapped_calls"), len(de_report_rows)),
            )
        )
        lines.append("| Mapping ID | Caller form | Caller handler | Report object | DataEnvironment | Kind | Confidence | Evidence ref |")
        lines.append("|---|---|---|---|---|---|---:|---|")
        for row in de_report_rows[:1500]:
            r = _as_dict(row)
            lines.append(
                "| {} | {} | {} | {} | {} | {} | {} | {} |".format(
                    _escape_pipe(r.get("mapping_id") or "n/a"),
                    _escape_pipe(r.get("caller_form") or "n/a"),
                    _escape_pipe(r.get("caller_handler") or "n/a"),
                    _escape_pipe(r.get("report_object") or "n/a"),
                    _escape_pipe(r.get("dataenvironment_object") or "n/a"),
                    _escape_pipe(r.get("mapping_kind") or "n/a"),
                    "{:.2f}".format(_as_float(r.get("confidence"), 0.0)),
                    _escape_pipe(r.get("evidence_ref") or "n/a"),
                )
            )
    else:
        lines.append("- No DataEnvironment/report mappings detected.")

    lines.extend(["", "### Y. Static Risk Detectors"])
    if static_detector_rows:
        static_summary = _as_dict(raw_static_risk_detectors.get("summary"))
        lines.append(
            "- Detector checks: {} | findings: {} | high severity: {}".format(
                _as_int(static_summary.get("detector_count"), 0),
                _as_int(static_summary.get("findings_count"), len(static_detector_rows)),
                _as_int(static_summary.get("high_findings"), 0),
            )
        )
        lines.append("| Detector ID | Severity | Summary | Evidence |")
        lines.append("|---|---|---|---|")
        for row in static_detector_rows[:1200]:
            r = _as_dict(row)
            lines.append(
                "| {} | {} | {} | {} |".format(
                    _escape_pipe(r.get("detector_id") or "n/a"),
                    _escape_pipe(r.get("severity") or "n/a"),
                    _escape_pipe(r.get("summary") or "n/a"),
                    _escape_pipe(json.dumps(_as_dict(r.get("evidence")), ensure_ascii=True)),
                )
            )
    else:
        lines.append("- No static detector findings were emitted.")

    lines.extend(["", "### Y1. Raw UI Control Inventory"])
    control_rows: list[dict[str, str]] = []
    seen_control_rows: set[str] = set()
    for dossier in raw_form_dossiers[:2000]:
        d = _as_dict(dossier)
        project_name = _clean(d.get("project_name")) or "n/a"
        form_name = _clean(d.get("form_name")) or "n/a"
        form_key = _form_key(project_name, form_name)
        control_map = form_control_type_by_key.get(form_key) or form_control_type_by_key.get(_base_only_key(form_name)) or {}
        for ctl in _as_list(d.get("controls")):
            ctl_text = _clean(ctl)
            if not ctl_text:
                continue
            if ":" in ctl_text:
                ctl_type, ctl_name = ctl_text.split(":", 1)
            else:
                ctl_type, ctl_name = control_map.get(ctl_text.lower()) or ctl_text, ctl_text
            ctl_type = _clean(ctl_type) or "unknown"
            ctl_name = _clean(ctl_name) or "n/a"
            lower_name = ctl_name.lower()
            lower_type = ctl_type.lower()
            role = "display"
            values = "n/a"
            if lower_name.startswith(("txt", "msk", "dtp")):
                role = "data_input"
            elif lower_name.startswith(("cmd", "btn")):
                role = "action"
            elif lower_name.startswith(("cbo", "cmb", "lst", "combo")) or "combobox" in lower_type or "list" in lower_type:
                role = "selection"
                values = "designer list values not statically recovered"
            elif lower_name.startswith(("lbl", "img", "pic")):
                role = "display"
            key = f"{project_name.lower()}|{form_name.lower()}|{ctl_name.lower()}|{ctl_type.lower()}"
            if key in seen_control_rows:
                continue
            seen_control_rows.add(key)
            control_rows.append(
                {
                    "project": _project_label(project_name, project_path_by_name),
                    "form": form_name,
                    "control_name": ctl_name,
                    "control_type": ctl_type,
                    "role": role,
                    "values": values,
                }
            )
    if control_rows:
        lines.append("- Controls discovered from raw form dossiers. Selection/list controls are preserved even when list values are not statically recoverable.")
        lines.append("| Project | Form | Control Name | Control Type | Role | Values / Notes |")
        lines.append("|---|---|---|---|---|---|")
        for row in control_rows[:3000]:
            lines.append(
                "| {} | {} | {} | {} | {} | {} |".format(
                    _escape_pipe(row.get("project") or "n/a"),
                    _escape_pipe(row.get("form") or "n/a"),
                    _escape_pipe(row.get("control_name") or "n/a"),
                    _escape_pipe(row.get("control_type") or "n/a"),
                    _escape_pipe(row.get("role") or "n/a"),
                    _escape_pipe(row.get("values") or "n/a"),
                )
            )
    else:
        lines.append("- No raw control inventory rows available.")

    return "\n".join(lines)


def _latest_stage_output(run: dict[str, Any], stage: int = 1) -> dict[str, Any]:
    pipeline_state = _as_dict(run.get("pipeline_state"))
    results = _as_list(pipeline_state.get("agent_results"))
    for row in reversed(results):
        row_d = _as_dict(row)
        if int(row_d.get("stage") or 0) == stage and isinstance(row_d.get("output"), dict):
            return _as_dict(row_d.get("output"))
    fallback = pipeline_state.get("analyst_output")
    return _as_dict(fallback)


def _has_stage_result(run: dict[str, Any], stage: int = 1) -> bool:
    pipeline_state = _as_dict(run.get("pipeline_state"))
    results = _as_list(pipeline_state.get("agent_results"))
    return any(int(_as_dict(r).get("stage") or 0) == stage for r in results)


def _resolve_api_key(provider: str, explicit: str) -> tuple[str, str]:
    if explicit.strip():
        return explicit.strip(), ""
    from utils.settings_store import SettingsStore

    settings = SettingsStore(str(ROOT / "team_data"))
    cfg = settings.resolve_llm_credentials(provider, requested_model="")
    if str(cfg.get("api_key", "")).strip():
        return str(cfg.get("api_key", "")).strip(), "settings"
    env_var = "OPENAI_API_KEY" if provider == "openai" else "ANTHROPIC_API_KEY"
    env_val = os.getenv(env_var, "").strip()
    if env_val:
        return env_val, env_var
    return "", ""


def _build_pipeline_config(provider: str, model: str, api_key: str):
    from config import LLMProvider, PipelineConfig

    if provider == "openai":
        return PipelineConfig(
            provider=LLMProvider.OPENAI,
            openai_api_key=api_key,
            openai_model=model or "gpt-4o",
            anthropic_api_key="",
            live_deploy=False,
            max_retries=2,
            developer_parallel_agents=3,
            temperature=0.2,
        )
    return PipelineConfig(
        provider=LLMProvider.ANTHROPIC,
        anthropic_api_key=api_key,
        anthropic_model=model or "claude-sonnet-4-20250514",
        openai_api_key="",
        live_deploy=False,
        max_retries=2,
        developer_parallel_agents=3,
        temperature=0.2,
    )


def _repo_url_from_settings() -> str:
    settings_path = ROOT / "team_data" / "settings_state.json"
    if not settings_path.exists():
        return ""
    try:
        data = json.loads(settings_path.read_text(encoding="utf-8"))
    except Exception:
        return ""
    github = _as_dict(_as_dict(data.get("integrations")).get("github"))
    owner = _clean(github.get("owner"))
    repo = _clean(github.get("repository"))
    if owner and repo:
        return f"https://github.com/{owner}/{repo}"
    return ""


def _repo_url_from_latest_pipeline_run() -> str:
    runs_root = ROOT / "pipeline_runs"
    if not runs_root.exists():
        return ""
    candidates = sorted(
        [p for p in runs_root.iterdir() if p.is_dir()],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    for run_dir in candidates[:120]:
        state_path = run_dir / "state.json"
        if not state_path.exists():
            continue
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        pipeline_state = _as_dict(state.get("pipeline_state"))
        integration = _as_dict(pipeline_state.get("integration_context"))
        if _as_dict(integration.get("local_bundle_inventory")):
            # Skip local CLI-direct runs; prefer web/api brownfield runs.
            continue
        brownfield = _as_dict(integration.get("brownfield"))
        repo_url = _clean(
            brownfield.get("repo_url")
            or pipeline_state.get("repo_url")
            or _as_dict(pipeline_state.get("context_reference")).get("repo_url")
        )
        if repo_url:
            return repo_url
    return ""


def _repo_url_from_api_latest_run(base_url: str) -> str:
    base = _clean(base_url).rstrip("/")
    if not base:
        return ""
    try:
        runs_data = _http_json("GET", f"{base}/api/runs", timeout=30)
    except Exception:
        return ""
    runs = _as_list(runs_data.get("runs"))
    if not runs:
        return ""
    for row in runs[:40]:
        run_id = _clean(_as_dict(row).get("run_id"))
        if not run_id:
            continue
        try:
            detail = _http_json("GET", f"{base}/api/runs/{run_id}", timeout=30)
        except Exception:
            continue
        run = _as_dict(detail.get("run"))
        integration = _as_dict(run.get("integration_context"))
        brownfield = _as_dict(integration.get("brownfield"))
        discover_cache = _as_dict(integration.get("discover_cache"))
        analyst_repo = _as_dict(discover_cache.get("analyst_repo"))
        pipeline_state = _as_dict(run.get("pipeline_state"))
        state_integration = _as_dict(pipeline_state.get("integration_context"))
        state_brownfield = _as_dict(state_integration.get("brownfield"))
        repo_url = _clean(
            brownfield.get("repo_url")
            or analyst_repo.get("url")
            or state_brownfield.get("repo_url")
            or pipeline_state.get("repo_url")
            or _as_dict(pipeline_state.get("context_reference")).get("repo_url")
        )
        if repo_url:
            return repo_url
    return ""


def _resolve_repo_url(repo_arg: str, *, base_url: str = "", prefer_api: bool = False) -> tuple[str, str]:
    explicit = _clean(repo_arg)
    if explicit and explicit.lower() not in {"auto", "ui", "web", "latest"}:
        return explicit, "cli"
    if prefer_api:
        from_api = _repo_url_from_api_latest_run(base_url)
        if from_api:
            return from_api, "latest_api_run"
    from_runs = _repo_url_from_latest_pipeline_run()
    if from_runs:
        return from_runs, "latest_pipeline_run"
    from_settings = _repo_url_from_settings()
    if from_settings:
        return from_settings, "settings"
    return DEFAULT_REPO_URL, "default"


def _clone_repo(repo_url: str) -> Path:
    temp_root = Path(tempfile.mkdtemp(prefix="synthetix-vb6-"))
    repo_dir = temp_root / "repo"
    try:
        subprocess.run(
            ["git", "clone", "--depth", "1", repo_url, str(repo_dir)],
            check=True,
            capture_output=True,
            text=True,
            timeout=240,
        )
    except subprocess.TimeoutExpired as exc:
        shutil.rmtree(temp_root, ignore_errors=True)
        raise RuntimeError(f"git clone timed out for {repo_url}") from exc
    except subprocess.CalledProcessError as exc:
        shutil.rmtree(temp_root, ignore_errors=True)
        stderr = (exc.stderr or "").strip()
        raise RuntimeError(f"git clone failed for {repo_url}: {stderr}") from exc
    return repo_dir


def _build_legacy_bundle_from_repo(repo_dir: Path, max_text_chars: int = 40000) -> tuple[str, dict[str, Any]]:
    sections: list[str] = []
    file_rows: list[dict[str, Any]] = []

    files = sorted(
        [p for p in repo_dir.rglob("*") if p.is_file() and p.suffix.lower() in VB6_EXTENSIONS],
        key=lambda p: p.as_posix().lower(),
    )
    if not files:
        raise RuntimeError(f"No VB6 files found in repository: {repo_dir}")

    for path in files:
        rel = path.relative_to(repo_dir).as_posix()
        ext = path.suffix.lower()
        size = path.stat().st_size
        row = {"path": rel, "extension": ext, "size_bytes": size, "is_text": ext in VB6_TEXT_EXTENSIONS}
        file_rows.append(row)

        header = f"### FILE: {rel}"
        if ext in VB6_TEXT_EXTENSIONS:
            text = path.read_text(encoding="utf-8", errors="replace")
            if len(text) > max_text_chars:
                text = text[:max_text_chars] + "\n... [truncated]"
            sections.append(f"{header}\n{text}")
        else:
            sections.append(f"{header}\n[binary file: {size} bytes]")

    counts_by_ext: dict[str, int] = {}
    for row in file_rows:
        ext = str(row["extension"])
        counts_by_ext[ext] = counts_by_ext.get(ext, 0) + 1

    summary = {
        "total_files": len(file_rows),
        "counts_by_extension": counts_by_ext,
        "text_files": sum(1 for r in file_rows if r["is_text"]),
        "binary_files": sum(1 for r in file_rows if not r["is_text"]),
    }
    bundle = "\n\n".join(sections)
    return bundle, {"summary": summary, "files": file_rows}


def _discover_prefetch(base_url: str, objectives: str, repo_url: str) -> dict[str, Any]:
    integration = {
        "project_state_mode": "brownfield",
        "project_state_detected": "brownfield",
        "brownfield": {
            "repo_provider": "github",
            "repo_url": repo_url,
            "issue_provider": "",
            "issue_project": "",
            "docs_url": "",
            "runtime_telemetry": False,
        },
        "scan_scope": {
            "analysis_depth": "deep",
            "telemetry_mode": "off",
            "modernization_source_mode": "repo_scan",
            "include_paths": [],
            "exclude_paths": [],
        },
        "sample_dataset_enabled": False,
    }

    payload = {
        "use_case": "code_modernization",
        "objectives": objectives,
        "repo_provider": "github",
        "repo_url": repo_url,
        "integration_context": integration,
    }
    data = _http_json("POST", f"{base_url}/api/discover/analyst-brief", payload=payload, timeout=300)
    if not bool(data.get("ok")):
        raise RuntimeError(f"Discover analyst brief failed: {_clean(data.get('error')) or data}")

    summary = _as_dict(_as_dict(data.get("analyst_brief")).get("summary"))
    aas = _as_dict(data.get("aas"))
    requirements_pack = _as_dict(data.get("requirements_pack"))
    discover_cache = {
        "analyst_source": _clean(data.get("source")),
        "analyst_repo": _as_dict(data.get("repo")),
        "analyst_thread_id": _clean(data.get("thread_id") or aas.get("thread_id")),
        "analyst_aas_summary": _clean(data.get("assistant_summary") or aas.get("assistant_summary")),
        "analyst_summary": {
            "overview": _clean(summary.get("overview")),
            "likely_capabilities": _as_list(summary.get("likely_capabilities"))[:12],
            "key_components": _as_list(summary.get("key_components"))[:20],
            "evidence_files": _as_list(summary.get("evidence_files"))[:80],
            "input_output_contracts": _as_list(summary.get("input_output_contracts"))[:24],
            "domain_functions": _as_list(summary.get("domain_functions"))[:80],
            "data_entities": _as_list(summary.get("data_entities"))[:80],
            "legacy_skill_profile": _as_dict(summary.get("legacy_skill_profile")),
            "vb6_analysis": _as_dict(summary.get("vb6_analysis")),
        },
        "analyst_requirements_pack": requirements_pack or None,
    }

    integration["discover_cache"] = discover_cache
    return integration


def _write_outputs(
    *,
    analyst_output: dict[str, Any],
    mode: str,
    output_dir: str,
    run_id_hint: str,
) -> tuple[Path, Path]:
    markdown = build_full_markdown(analyst_output, mode=mode)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    safe_run_id = _clean(run_id_hint) or f"local-{stamp}"
    out_root = Path(output_dir).expanduser().resolve()
    run_dir = out_root / f"bundle-{safe_run_id}-{stamp}"
    run_dir.mkdir(parents=True, exist_ok=True)
    md_path = run_dir / f"analyst-tech-req-{mode}-{safe_run_id}-{stamp}.md"
    json_path = run_dir / f"analyst-output-{safe_run_id}-{stamp}.json"

    md_path.write_text(markdown, encoding="utf-8")
    json_path.write_text(json.dumps(analyst_output, indent=2, ensure_ascii=True, default=str), encoding="utf-8")
    return md_path, json_path


def _write_db_design_artifacts(
    *,
    analyst_output: dict[str, Any],
    out_dir: Path,
) -> list[Path]:
    """Materialize DB archaeology + target-schema artifacts as standalone JSON files."""
    report = build_analyst_report_v2(analyst_output)
    raw = _as_dict(report.get("raw_artifacts"))
    db_keys = [
        "source_db_profile",
        "source_schema_model",
        "source_erd",
        "source_query_catalog",
        "source_relationship_candidates",
        "source_data_dictionary",
        "source_data_dictionary_markdown",
        "source_hotspot_report",
        "target_schema_model",
        "target_erd",
        "target_data_dictionary",
        "schema_mapping_matrix",
        "migration_plan",
        "validation_harness_spec",
        "db_qa_report",
        "schema_approval_record",
        "schema_drift_report",
    ]
    written: list[Path] = []
    for key in db_keys:
        payload = raw.get(key)
        if payload is None:
            continue
        target = out_dir / f"{key}.json"
        target.write_text(
            json.dumps(payload, indent=2, ensure_ascii=True, default=str),
            encoding="utf-8",
        )
        written.append(target)

    # Compatibility outputs for SourceSchemaAgent contract.
    source_schema_model = _as_dict(raw.get("source_schema_model"))
    if source_schema_model:
        schema_alias = out_dir / "source_schema.json"
        schema_alias.write_text(
            json.dumps(source_schema_model, indent=2, ensure_ascii=True, default=str),
            encoding="utf-8",
        )
        written.append(schema_alias)
    source_erd = _as_dict(raw.get("source_erd"))
    source_erd_mermaid = _clean(source_erd.get("mermaid"))
    if source_erd_mermaid:
        erd_alias = out_dir / "source_erd.mmd"
        erd_alias.write_text(source_erd_mermaid.rstrip() + "\n", encoding="utf-8")
        written.append(erd_alias)
    source_dict_md = _as_dict(raw.get("source_data_dictionary_markdown"))
    source_dict_text = _clean(source_dict_md.get("markdown"))
    if source_dict_text:
        dict_alias = out_dir / "data_dictionary.md"
        dict_alias.write_text(source_dict_text.rstrip() + "\n", encoding="utf-8")
        written.append(dict_alias)
    return written


def _humanize_imported_evidence_summary(value: Any) -> str:
    text = _clean(value)
    low = text.lower()
    if low == "vbdepend dead code report":
        return "Imported structural analysis indicates potential dead code that may be removable or may hide undocumented legacy behavior."
    if low == "evidence coverage report":
        return "Imported analysis does not include enough behavioral or data evidence to guarantee functional parity without additional verification."
    return text


def _humanize_imported_evidence_action(value: Any, summary: str) -> str:
    text = _clean(value)
    low_summary = _clean(summary).lower()
    if "dead code" in low_summary:
        return "Review dead-code candidates with engineering and business owners before finalizing parity scope."
    if "behavioral or data evidence" in low_summary or "functional parity" in low_summary:
        return "Obtain SQL, schema, or SME walkthrough evidence before committing build scope."
    return text or "Add targeted remediation task."


def _generate_docx_bundle(md_path: Path, out_dir: Path) -> tuple[Path, Path, Path, Path | None] | None:
    docgen_dir = ROOT / "synthetix-docgen"
    index_js = docgen_dir / "index.js"
    if not index_js.exists():
        print(f"[warn] synthetix-docgen not found at {docgen_dir}; skipping doc generation")
        return None

    cmd = [
        "node",
        str(index_js),
        "--md",
        str(md_path),
        "--out",
        str(out_dir),
    ]
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(docgen_dir),
            capture_output=True,
            text=True,
            check=True,
            timeout=300,
        )
        if proc.stdout.strip():
            print(proc.stdout.strip())
        if proc.stderr.strip():
            print(proc.stderr.strip())
    except Exception as exc:
        print(f"[warn] doc generation failed: {exc}")
        return None

    data_json = out_dir / "data.json"
    ba_doc = out_dir / "ba_brief.docx"
    tech_doc = out_dir / "tech_workbook.docx"
    brd_doc = out_dir / "brd.docx"
    if not (data_json.exists() and ba_doc.exists() and tech_doc.exists()):
        print("[warn] doc generation completed but expected outputs not found")
        return None
    return data_json, ba_doc, tech_doc, (brd_doc if brd_doc.exists() else None)


def run_and_export_api(args: argparse.Namespace) -> int:
    base_url = args.base_url.rstrip("/")
    objectives = args.objectives.strip() or DEFAULT_OBJECTIVE

    # health check
    health = _http_json("GET", f"{base_url}/api/health", timeout=20)
    if not bool(health.get("ok", True)):
        raise RuntimeError(f"Health check failed: {health}")

    if not args.skip_discover_prefetch:
        try:
            integration_context = _discover_prefetch(base_url, objectives, args.repo_url)
            print("[info] discover prefetch completed")
        except Exception as exc:
            print(f"[warn] discover prefetch failed, continuing with direct repo-scan start: {exc}")
            integration_context = {
                "project_state_mode": "brownfield",
                "project_state_detected": "brownfield",
                "brownfield": {
                    "repo_provider": "github",
                    "repo_url": args.repo_url,
                    "issue_provider": "",
                    "issue_project": "",
                    "docs_url": "",
                    "runtime_telemetry": False,
                },
                "scan_scope": {
                    "analysis_depth": "deep",
                    "telemetry_mode": "off",
                    "modernization_source_mode": "repo_scan",
                    "include_paths": [],
                    "exclude_paths": [],
                },
                "sample_dataset_enabled": False,
            }
    else:
        print("[info] discover prefetch skipped by flag")
        integration_context = {
            "project_state_mode": "brownfield",
            "project_state_detected": "brownfield",
            "brownfield": {
                "repo_provider": "github",
                "repo_url": args.repo_url,
                "issue_provider": "",
                "issue_project": "",
                "docs_url": "",
                "runtime_telemetry": False,
            },
            "scan_scope": {
                "analysis_depth": "deep",
                "telemetry_mode": "off",
                "modernization_source_mode": "repo_scan",
                "include_paths": [],
                "exclude_paths": [],
            },
            "sample_dataset_enabled": False,
        }

    run_payload = {
        "use_case": "code_modernization",
        "objectives": objectives,
        "legacy_code": "",
        "modernization_language": args.target_language,
        "deployment_target": "local",
        "human_approval": False,
        "strict_security_mode": False,
        "provider": args.provider,
        "model": args.model,
        "integration_context": integration_context,
    }

    run_start = _http_json("POST", f"{base_url}/api/runs", payload=run_payload, timeout=120)
    if not bool(run_start.get("ok")):
        raise RuntimeError(f"Run start failed: {_clean(run_start.get('error')) or run_start}")

    run_id = _clean(run_start.get("run_id"))
    if not run_id:
        raise RuntimeError(f"Run start response missing run_id: {run_start}")

    print(f"[info] run started: {run_id}")
    deadline = time.time() + max(30, int(args.timeout_seconds))
    final_run: dict[str, Any] | None = None

    while time.time() < deadline:
        row = _http_json("GET", f"{base_url}/api/runs/{run_id}", timeout=30)
        if not bool(row.get("ok")):
            raise RuntimeError(f"Failed to read run status: {row}")
        run = _as_dict(row.get("run"))
        status = _clean(run.get("status")).lower()
        has_stage1 = _has_stage_result(run, stage=1)

        current_stage = int(run.get("current_stage") or 0)
        print(f"[info] status={status or 'unknown'} stage={current_stage} stage1_ready={has_stage1}")

        if has_stage1:
            final_run = run
            if args.stop_after_analyst:
                try:
                    _http_json(
                        "POST",
                        f"{base_url}/api/runs/{run_id}/abort",
                        payload={"reason": "Analyst stage exported via CLI script"},
                        timeout=30,
                    )
                    print("[info] run aborted after analyst stage export")
                except Exception as exc:
                    print(f"[warn] could not abort run after stage 1: {exc}")
            break

        if status in {"completed", "failed", "aborted"}:
            final_run = run
            break

        time.sleep(max(1, int(args.poll_seconds)))

    if final_run is None:
        raise RuntimeError(f"Timed out waiting for analyst stage for run {run_id}")

    analyst_output = _latest_stage_output(final_run, stage=1)
    if not analyst_output:
        raise RuntimeError(
            f"Run {run_id} did not produce analyst output. Final status={_clean(final_run.get('status'))}."
        )

    md_path, json_path = _write_outputs(
        analyst_output=analyst_output,
        mode=args.mode,
        output_dir=args.output_dir,
        run_id_hint=run_id,
    )
    db_artifacts = _write_db_design_artifacts(analyst_output=analyst_output, out_dir=md_path.parent)

    print(f"[ok] markdown: {md_path}")
    print(f"[ok] analyst output json: {json_path}")
    if db_artifacts:
        print(f"[ok] DB design artifacts: {len(db_artifacts)} files in {md_path.parent}")
    if not args.skip_docgen:
        generated = _generate_docx_bundle(md_path, md_path.parent)
        if generated:
            data_json, ba_doc, tech_doc, brd_doc = generated
            print(f"[ok] docgen data: {data_json}")
            print(f"[ok] BA Brief: {ba_doc}")
            print(f"[ok] Tech Workbook: {tech_doc}")
            if brd_doc:
                print(f"[ok] BRD DOCX: {brd_doc}")
    return 0


def run_and_export_direct(args: argparse.Namespace) -> int:
    from orchestrator.pipeline import make_initial_state, run_single_stage

    objectives = args.objectives.strip() or DEFAULT_OBJECTIVE
    print("[warn] direct mode may diverge from web UI synthesis path. Use --execution-mode api for strict parity.")
    api_key, source = _resolve_api_key(args.provider, args.api_key)
    if not api_key:
        raise RuntimeError(
            f"No API key configured for provider `{args.provider}`. "
            f"Set it in Settings > LLM, pass --api-key, or set environment variable."
        )
    print(f"[info] using {args.provider} credentials from {source or 'cli flag'}")

    repo_dir = _clone_repo(args.repo_url)
    temp_root = repo_dir.parent
    try:
        legacy_code, inventory = _build_legacy_bundle_from_repo(repo_dir, max_text_chars=max(5000, int(args.max_text_chars)))
        print(
            "[info] legacy bundle prepared: "
            f"{inventory.get('summary', {}).get('total_files', 0)} files "
            f"({inventory.get('summary', {}).get('text_files', 0)} text / "
            f"{inventory.get('summary', {}).get('binary_files', 0)} binary)"
        )

        cfg = _build_pipeline_config(args.provider, args.model, api_key)
        state = make_initial_state(objectives)
        state["run_id"] = f"cli_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
        state["use_case"] = "code_modernization"
        state["legacy_code"] = legacy_code
        state["modernization_language"] = args.target_language
        state["deployment_target"] = "local"
        state["integration_context"] = {
            "project_state_mode": "brownfield",
            "project_state_detected": "brownfield",
            "brownfield": {"repo_provider": "github", "repo_url": args.repo_url},
            "scan_scope": {"modernization_source_mode": "repo_scan", "analysis_depth": "deep"},
            "local_bundle_inventory": inventory.get("summary", {}),
            "sample_dataset_enabled": False,
        }

        result_state = run_single_stage(cfg, state, stage_index=0)
        analyst_output = _as_dict(result_state.get("analyst_output"))
        if not analyst_output:
            stage_results = _as_list(result_state.get("agent_results"))
            msg = "unknown failure"
            for row in reversed(stage_results):
                r = _as_dict(row)
                if int(r.get("stage") or 0) == 1:
                    msg = _clean(r.get("summary")) or msg
                    break
            raise RuntimeError(f"Analyst stage did not produce output: {msg}")

        md_path, json_path = _write_outputs(
            analyst_output=analyst_output,
            mode=args.mode,
            output_dir=args.output_dir,
            run_id_hint=_clean(result_state.get("run_id")) or "local",
        )
        db_artifacts = _write_db_design_artifacts(analyst_output=analyst_output, out_dir=md_path.parent)
        print(f"[ok] markdown: {md_path}")
        print(f"[ok] analyst output json: {json_path}")
        if db_artifacts:
            print(f"[ok] DB design artifacts: {len(db_artifacts)} files in {md_path.parent}")
        if not args.skip_docgen:
            generated = _generate_docx_bundle(md_path, md_path.parent)
            if generated:
                data_json, ba_doc, tech_doc, brd_doc = generated
                print(f"[ok] docgen data: {data_json}")
                print(f"[ok] BA Brief: {ba_doc}")
                print(f"[ok] Tech Workbook: {tech_doc}")
                if brd_doc:
                    print(f"[ok] BRD DOCX: {brd_doc}")
        return 0
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)


def run_and_export(args: argparse.Namespace) -> int:
    prefer_api = _clean(args.execution_mode).lower() == "api"
    resolved_repo, source = _resolve_repo_url(args.repo_url, base_url=args.base_url, prefer_api=prefer_api)
    args.repo_url = resolved_repo
    print(f"[info] repo source={source} url={resolved_repo}")
    mode = _clean(args.execution_mode).lower() or "direct"
    if mode == "api":
        return run_and_export_api(args)
    return run_and_export_direct(args)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Synthetix Analyst stage against a VB6 GitHub repo and export detailed markdown locally."
    )
    parser.add_argument(
        "--execution-mode",
        default="api",
        choices=["direct", "api"],
        help="api (default): call running Synthetix server endpoints; direct: run analyst stage locally without API server",
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Synthetix API base URL")
    parser.add_argument(
        "--repo-url",
        default="auto",
        help="Legacy VB6 GitHub repository URL. Use 'auto' to reuse the latest web/API run repo (default).",
    )
    parser.add_argument("--objectives", default=DEFAULT_OBJECTIVE, help="Business objective text for the run")
    parser.add_argument("--provider", default="openai", choices=["openai", "anthropic"], help="LLM provider")
    parser.add_argument("--model", default="gpt-4o", help="LLM model")
    parser.add_argument("--api-key", default="", help="Optional LLM API key override for direct mode")
    parser.add_argument("--target-language", default="C#", help="Target modernization language")
    parser.add_argument("--mode", default="full", choices=["summary", "full"], help="Markdown detail level")
    parser.add_argument("--output-dir", default=str(ROOT / "run_artifacts" / "manual_exports"), help="Output directory")
    parser.add_argument(
        "--skip-docgen",
        action="store_true",
        help="Skip BA Brief/Tech Workbook doc generation (enabled by default).",
    )
    parser.add_argument("--max-text-chars", type=int, default=40000, help="Per-file text cap when building local legacy bundle")
    parser.add_argument("--timeout-seconds", type=int, default=1200, help="Max wait time for stage output")
    parser.add_argument("--poll-seconds", type=int, default=5, help="Polling interval")
    parser.add_argument(
        "--skip-discover-prefetch",
        action="store_true",
        help="API mode only: skip /api/discover/analyst-brief prefetch and start run directly.",
    )
    parser.add_argument(
        "--stop-after-analyst",
        action="store_true",
        default=True,
        help="Abort run immediately after stage 1 output is available (default true)",
    )
    parser.add_argument(
        "--continue-pipeline",
        action="store_true",
        help="Do not abort after analyst stage (overrides --stop-after-analyst).",
    )

    args = parser.parse_args(argv)
    if args.continue_pipeline:
        args.stop_after_analyst = False
    return args


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    try:
        return run_and_export(args)
    except Exception as exc:
        print(f"[error] {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
