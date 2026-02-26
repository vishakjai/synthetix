from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from .catalog import VB6_DETECTOR_POLICY, VB6_KEYWORDS

_VB6_FILE_TYPE_LABELS = {
    ".cls": "class_module",
    ".frm": "form",
    ".frx": "form_binary",
    ".bas": "standard_module",
    ".ctl": "user_control",
    ".ctx": "user_control_binary",
    ".vbp": "project_file",
    ".vbg": "project_group",
    ".res": "resource_file",
    ".ocx": "activex_binary",
    ".dcx": "db_query_definition",
    ".dca": "db_connection_definition",
}


_SQL_KEYWORD_RE = re.compile(r"(?i)\b(select|insert|update|delete)\b")


def vb6_skill_pack_manifest() -> dict[str, Any]:
    return {
        "skill_pack_id": "legacy-vb6",
        "version": "0.1.0",
        "supported_inputs": [".vbp", ".vbg", ".bas", ".cls", ".frm", ".frx", ".ctl", ".ctx", ".res", ".ocx", ".dcx", ".dca"],
        "extractors": [
            "vb6_project_inventory",
            "vb6_call_graph_builder",
            "vb6_ui_event_map_builder",
            "vb6_com_reference_scanner",
            "vb6_win32_declare_scanner",
            "vb6_sql_string_extractor",
            "vb6_error_semantics_profiler",
        ],
        "detectors": list(VB6_DETECTOR_POLICY),
        "required_outputs": [
            "vb6_system_context_model",
            "vb6_ui_event_map",
            "vb6_com_surface_map",
            "vb6_error_handling_profile",
            "vb6_modernization_readiness",
            "bdd_contract_pack",
        ],
        "default_questions": [
            "What critical business flows must preserve behavior first?",
            "Which third-party OCX/COM components must be retained or replaced?",
            "Which databases and transaction boundaries are in scope?",
            "Is migration phased (strangler/interoperability) or one-time cutover?",
        ],
    }


def _strip_vb_comment(line: str) -> str:
    text = str(line or "")
    in_quotes = False
    out: list[str] = []
    i = 0
    while i < len(text):
        ch = text[i]
        if ch == '"':
            in_quotes = not in_quotes
            out.append(ch)
            i += 1
            continue
        if ch == "'" and not in_quotes:
            break
        out.append(ch)
        i += 1
    return "".join(out).strip()


def _normalize_vb6_sql_expr(expr: str) -> str:
    text = str(expr or "").strip()
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text)
    parts = [p.strip() for p in re.split(r"\s*&\s*", text) if p.strip()]
    if not parts:
        return ""
    out_parts: list[str] = []
    for part in parts:
        lower = part.lower()
        if lower in {"vbcrlf", "vbnewline", "vbtab", "space$(1)"}:
            out_parts.append(" ")
            continue
        if part.startswith('"') and part.endswith('"') and len(part) >= 2:
            literal = part[1:-1].replace('""', '"')
            if literal:
                out_parts.append(literal)
            continue
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*", part):
            out_parts.append(":expr")
            continue
        if re.search(r"(?i)\b(cstr|trim|val|format|left|right|mid|replace|date\$|time\$|now)\b", part):
            out_parts.append(":expr")
            continue
        out_parts.append(part)
    query = "".join(out_parts)
    query = re.sub(r"\s+", " ", query).strip()
    if _SQL_KEYWORD_RE.search(query):
        return query[:360]
    return ""


def _extract_sql_candidates_from_lines(lines: list[str]) -> list[str]:
    logical_lines: list[str] = []
    buffer = ""
    for raw in lines:
        cleaned = _strip_vb_comment(raw)
        if not cleaned:
            continue
        if buffer:
            buffer = f"{buffer} {cleaned}".strip()
        else:
            buffer = cleaned
        if buffer.endswith("_"):
            buffer = buffer[:-1].strip()
            continue
        logical_lines.append(buffer)
        buffer = ""
    if buffer:
        logical_lines.append(buffer)

    queries: list[str] = []
    seen: set[str] = set()
    for line in logical_lines:
        expr_candidates: list[str] = []
        if "=" in line:
            left, right = line.split("=", 1)
            left_l = left.lower()
            if (
                "sql" in left_l
                or "query" in left_l
                or "recordsource" in left_l
                or "commandtext" in left_l
                or _SQL_KEYWORD_RE.search(right)
            ):
                expr_candidates.append(right.strip())
        open_match = re.search(r"(?i)\b(?:open|execute)\s+(.+)$", line)
        if open_match and _SQL_KEYWORD_RE.search(line):
            expr_candidates.append(open_match.group(1).strip())
        if not expr_candidates and _SQL_KEYWORD_RE.search(line):
            expr_candidates.append(line.strip())
        for expr in expr_candidates:
            normalized = _normalize_vb6_sql_expr(expr)
            if normalized and normalized.lower() not in seen:
                seen.add(normalized.lower())
                queries.append(normalized)
        for frag in re.findall(r'(?is)"([^"\n]*(?:select|insert|update|delete)[^"\n]*)"', line):
            query = " ".join(str(frag or "").split())
            if query and query.lower() not in seen:
                seen.add(query.lower())
                queries.append(query[:360])
    return queries[:200]


def _extract_procedure_blocks(text: str) -> dict[str, Any]:
    procedure_bodies: dict[str, list[str]] = {}
    procedure_lines: dict[str, int] = {}
    current_name = ""
    buffer: list[str] = []
    start_line = 0
    start_rx = re.compile(
        r"(?im)^\s*(?:Public|Private|Friend|Protected)?\s*(Function|Sub|Property Get|Property Let|Property Set)\s+([A-Za-z_][A-Za-z0-9_]*)\b"
    )
    end_rx = re.compile(r"(?im)^\s*End\s+(Function|Sub|Property)\b")
    for idx, raw in enumerate(str(text or "").splitlines(), start=1):
        line = str(raw or "")
        start = start_rx.match(line)
        if start and not current_name:
            current_name = str(start.group(2) or "").strip()
            start_line = idx
            buffer = [line]
            continue
        if current_name:
            buffer.append(line)
            if end_rx.match(line):
                procedure_bodies[current_name] = buffer[:]
                procedure_lines[current_name] = start_line
                current_name = ""
                buffer = []
    if current_name and buffer:
        procedure_bodies[current_name] = buffer[:]
        procedure_lines[current_name] = start_line or 1

    procedure_calls: dict[str, list[str]] = {}
    procedure_sql: dict[str, list[str]] = {}
    procedure_effects: dict[str, list[str]] = {}
    call_rx = re.compile(r"(?im)^\s*(?:Call\s+)?([A-Za-z_][A-Za-z0-9_]*)\b")
    for proc, body_lines in procedure_bodies.items():
        body = "\n".join(body_lines)
        calls: list[str] = []
        for match in call_rx.findall(body):
            token = str(match or "").strip()
            if not token:
                continue
            if token.lower() in VB6_KEYWORDS:
                continue
            if token.lower() == proc.lower():
                continue
            if token not in calls:
                calls.append(token)
            if len(calls) >= 40:
                break
        sqls = _extract_sql_candidates_from_lines(body_lines)[:40]
        effects: list[str] = []
        lower = body.lower()
        markers = [
            ("insert ", "insert"),
            ("update ", "update"),
            ("delete ", "delete"),
            ("createobject(", "createobject"),
            ("save", "save_operation"),
            ("print #", "file_write"),
        ]
        for token, label in markers:
            if token in lower and label not in effects:
                effects.append(label)
        procedure_calls[proc] = calls
        procedure_sql[proc] = sqls
        procedure_effects[proc] = effects
    return {
        "procedure_bodies": procedure_bodies,
        "procedure_lines": procedure_lines,
        "procedure_calls": procedure_calls,
        "procedure_sql": procedure_sql,
        "procedure_effects": procedure_effects,
    }


def _map_vb6_detectors(
    *,
    path: str,
    on_error_resume_next: int,
    control_arrays: int,
    late_binding: int,
    win32_declares: int,
    doevents: int,
    variant_decls: int,
    default_instances: int,
) -> list[dict[str, Any]]:
    hits: list[dict[str, Any]] = []
    if on_error_resume_next > 0:
        hits.append(
            {
                "id": "VB6-ERR-001",
                "severity": "high",
                "count": on_error_resume_next,
                "requires": ["error_model_plan"],
                "evidence": f"{path}: On Error Resume Next occurrences",
            }
        )
    if control_arrays > 0:
        hits.append(
            {
                "id": "VB6-UI-002",
                "severity": "medium",
                "count": control_arrays,
                "requires": ["ui_migration_strategy"],
                "evidence": f"{path}: control array index markers",
            }
        )
    if late_binding > 0:
        hits.append(
            {
                "id": "VB6-COM-003",
                "severity": "high",
                "count": late_binding,
                "requires": ["com_dependency_plan"],
                "evidence": f"{path}: CreateObject/GetObject/CallByName usage",
            }
        )
    if win32_declares > 0:
        hits.append(
            {
                "id": "VB6-API-004",
                "severity": "high",
                "count": win32_declares,
                "requires": ["interop_risk_assessment"],
                "evidence": f"{path}: Win32 API Declare usage",
            }
        )
    if doevents > 0:
        hits.append(
            {
                "id": "VB6-CONC-005",
                "severity": "medium",
                "count": doevents,
                "requires": ["event_loop_plan"],
                "evidence": f"{path}: DoEvents/Timer-driven execution",
            }
        )
    if variant_decls > 0:
        hits.append(
            {
                "id": "VB6-TYPE-006",
                "severity": "medium",
                "count": variant_decls,
                "requires": ["type_normalization_plan"],
                "evidence": f"{path}: Variant declaration density",
            }
        )
    if default_instances > 0:
        hits.append(
            {
                "id": "VB6-OOP-007",
                "severity": "medium",
                "count": default_instances,
                "requires": ["default_instance_refactor_plan"],
                "evidence": f"{path}: default instance references",
            }
        )
    return hits


def build_vb6_readiness_assessment(
    *,
    vb6_by_path: dict[str, dict[str, Any]] | None,
    vb6_projects: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    by_path = vb6_by_path or {}
    projects = vb6_projects or []

    forms_total = 0
    controls_total = 0
    activex_deps: set[str] = set()
    sql_queries_total = 0
    ui_events_total = 0
    win32_declares_total = 0
    late_binding_total = 0
    on_error_resume_next_total = 0
    control_arrays_total = 0
    variant_total = 0
    default_instances_total = 0
    doevents_total = 0
    detector_counts: dict[str, dict[str, Any]] = {}

    for sig in by_path.values():
        if not isinstance(sig, dict):
            continue
        forms_total += len(sig.get("forms", []) if isinstance(sig.get("forms", []), list) else [])
        controls_total += len(sig.get("controls", []) if isinstance(sig.get("controls", []), list) else [])
        for dep in sig.get("activex_dependencies", []) if isinstance(sig.get("activex_dependencies", []), list) else []:
            activex_deps.add(str(dep))
        sql_queries_total += len(sig.get("sql_queries", []) if isinstance(sig.get("sql_queries", []), list) else [])
        ui_events_total += len(sig.get("ui_event_map", []) if isinstance(sig.get("ui_event_map", []), list) else [])
        win32_declares_total += len(sig.get("win32_declares", []) if isinstance(sig.get("win32_declares", []), list) else [])
        profile = sig.get("error_handling_profile", {}) if isinstance(sig.get("error_handling_profile", {}), dict) else {}
        on_error_resume_next_total += int(profile.get("on_error_resume_next", 0) or 0)
        late_binding_total += int(profile.get("late_bound_com_calls", 0) or 0)
        control_arrays_total += int(profile.get("control_array_index_markers", 0) or 0)
        variant_total += int(profile.get("variant_declarations", 0) or 0)
        default_instances_total += int(profile.get("default_instance_references", 0) or 0)
        doevents_total += int(profile.get("doevents_calls", 0) or 0)
        for hit in sig.get("pitfall_detectors", []) if isinstance(sig.get("pitfall_detectors", []), list) else []:
            if not isinstance(hit, dict):
                continue
            hid = str(hit.get("id", "")).strip()
            if not hid:
                continue
            row = detector_counts.setdefault(
                hid,
                {
                    "id": hid,
                    "severity": str(hit.get("severity", "medium")),
                    "count": 0,
                    "requires": hit.get("requires", []) if isinstance(hit.get("requires", []), list) else [],
                    "samples": [],
                },
            )
            row["count"] = int(row.get("count", 0) or 0) + int(hit.get("count", 1) or 1)
            evidence = str(hit.get("evidence", "")).strip()
            if evidence and len(row["samples"]) < 6 and evidence not in row["samples"]:
                row["samples"].append(evidence)

    if projects:
        forms_total = max(forms_total, sum(int(p.get("forms_count", 0) or 0) for p in projects if isinstance(p, dict)))

    penalties = {
        "on_error_resume_next": min(30, on_error_resume_next_total * 6),
        "late_bound_com": min(20, late_binding_total * 5),
        "win32_declares": min(20, win32_declares_total * 4),
        "control_arrays": min(12, control_arrays_total * 3),
        "variant_heavy": min(12, variant_total * 2),
        "default_instances": min(10, default_instances_total * 2),
        "doevents_timer": min(10, doevents_total * 2),
        "activex_dependency_density": min(20, max(0, len(activex_deps) - 3) * 2),
        "inline_sql_density": min(10, sql_queries_total // 8),
    }
    score = max(0, 100 - sum(int(v) for v in penalties.values()))

    if score < 45:
        risk_tier = "high"
        strategy = "rehost_stabilize"
        strategy_title = "Rehost / Stabilize"
        rationale = "Risk profile indicates heavy runtime and dependency coupling; stabilize first, then modernize incrementally."
    elif late_binding_total + win32_declares_total > 8 or len(activex_deps) > 10:
        risk_tier = "high"
        strategy = "strangler_wrap"
        strategy_title = "Strangler / Wrap (Interop-first)"
        rationale = "COM/Win32 coupling is significant; wrap legacy components behind stable contracts and migrate flow-by-flow."
    elif score >= 75 and len(activex_deps) <= 6 and win32_declares_total == 0:
        risk_tier = "medium"
        strategy = "full_upgrade_translation"
        strategy_title = "Full upgrade / translation + remediation"
        rationale = "Readiness is relatively high; controlled translation with remediation and equivalence testing is viable."
    else:
        risk_tier = "medium"
        strategy = "phased_ui_migration"
        strategy_title = "Phased UI migration"
        rationale = "Incremental form-by-form migration balances risk and delivery speed while preserving behavior."

    required_actions: list[str] = []
    for row in detector_counts.values():
        for req in row.get("requires", []) if isinstance(row.get("requires", []), list) else []:
            action = str(req).strip()
            if action and action not in required_actions:
                required_actions.append(action)

    return {
        "score": score,
        "risk_tier": risk_tier,
        "recommended_strategy": {
            "id": strategy,
            "name": strategy_title,
            "rationale": rationale,
        },
        "totals": {
            "projects": len([p for p in projects if isinstance(p, dict)]),
            "forms": forms_total,
            "controls": controls_total,
            "activex_dependencies": len(activex_deps),
            "ui_event_map_entries": ui_events_total,
            "sql_queries": sql_queries_total,
            "win32_declares": win32_declares_total,
        },
        "penalties": penalties,
        "detectors": sorted(detector_counts.values(), key=lambda row: str(row.get("id", ""))),
        "required_actions": required_actions[:20],
    }


def extract_vb6_signals(path: str, text: str) -> dict[str, Any]:
    lower_path = str(path or "").strip().lower()
    suffix = ""
    if "." in lower_path:
        suffix = "." + lower_path.rsplit(".", 1)[1]
    lowered = str(text or "").lower()
    is_vb6_path = suffix in {".frm", ".frx", ".bas", ".cls", ".ctl", ".ctx", ".vbp", ".vbg", ".res", ".ocx", ".dcx", ".dca"}
    is_vb6_text = (
        "attribute vb_name" in lowered
        or "begin vb.form" in lowered
        or "begin vb.mdiform" in lowered
        or "begin vb.usercontrol" in lowered
    )
    if not (is_vb6_path or is_vb6_text):
        return {}

    file_path = str(path or "").strip()
    vb6_file_type = _VB6_FILE_TYPE_LABELS.get(suffix, "unknown")
    is_binary_companion = suffix in {".frx", ".ctx", ".res", ".ocx"}
    if is_binary_companion:
        return {
            "forms": [],
            "controls": [],
            "activex_dependencies": [],
            "event_handlers": [],
            "event_handler_keys": [],
            "procedures": [],
            "sql_queries": [],
            "ui_event_map": [],
            "project_members": [],
            "com_surface_map": {
                "late_bound_progids": [],
                "call_by_name_sites": 0,
                "createobject_getobject_sites": 0,
                "references": [],
            },
            "win32_declares": [],
            "error_handling_profile": {
                "on_error_resume_next": 0,
                "on_error_goto": 0,
                "on_error_goto0": 0,
                "control_array_index_markers": 0,
                "late_bound_com_calls": 0,
                "variant_declarations": 0,
                "default_instance_references": 0,
                "doevents_calls": 0,
                "registry_operations": 0,
            },
            "pitfall_detectors": [],
            "project_definition": {},
            "vb6_file_type": vb6_file_type,
            "is_binary_companion": True,
            "binary_companion_info": {
                "path": file_path,
                "extension": suffix,
                "note": (
                    "ActiveX binary component detected; analyzed as structural dependency."
                    if suffix == ".ocx"
                    else "Binary companion file detected; analyzed as structural dependency."
                ),
            },
        }
    forms: set[str] = set()
    controls: set[str] = set()
    activex_dependencies: set[str] = set()
    event_handlers: set[str] = set()
    event_handler_keys: set[str] = set()
    project_members: set[str] = set()
    procedures: set[str] = set()
    sql_queries: list[str] = []
    win32_declares: list[str] = []
    late_bound_progids: set[str] = set()
    call_by_name_sites = 0
    control_array_index_markers = 0
    doevents_calls = 0
    variant_declarations = 0
    default_instance_references = 0
    on_error_resume_next = 0
    on_error_goto = 0
    on_error_goto0 = 0
    registry_ops = 0
    project_definition: dict[str, Any] = {}
    procedure_meta = _extract_procedure_blocks(text)
    procedure_calls = procedure_meta.get("procedure_calls", {}) if isinstance(procedure_meta.get("procedure_calls", {}), dict) else {}
    procedure_sql = procedure_meta.get("procedure_sql", {}) if isinstance(procedure_meta.get("procedure_sql", {}), dict) else {}
    procedure_effects = procedure_meta.get("procedure_effects", {}) if isinstance(procedure_meta.get("procedure_effects", {}), dict) else {}

    for kind, form_name in re.findall(
        r"(?im)^\s*Begin\s+VB\.(Form|MDIForm|UserControl)\s+([A-Za-z_][A-Za-z0-9_]*)",
        text,
    ):
        forms.add(f"{kind}:{form_name}")

    for control_type, control_name in re.findall(
        r"(?im)^\s*Begin\s+([A-Za-z_][A-Za-z0-9_\.]*)\s+([A-Za-z_][A-Za-z0-9_]*)",
        text,
    ):
        ct = str(control_type or "").strip()
        if ct in {"VB.Form", "VB.MDIForm", "VB.UserControl"}:
            continue
        controls.add(f"{ct}:{control_name}")
        if not ct.startswith("VB."):
            activex_dependencies.add(ct)

    for object_ref, binary_ref in re.findall(
        r'(?im)^\s*Object\s*=\s*"([^"]+)"\s*;\s*"([^"]+)"',
        text,
    ):
        dep = str(binary_ref or "").strip()
        dep_upper = dep.upper()
        if dep_upper.endswith(".OCX") or dep_upper.endswith(".DLL") or dep_upper.endswith(".DCX") or dep_upper.endswith(".DCA"):
            activex_dependencies.add(dep)
        elif dep:
            activex_dependencies.add(f"{dep} ({str(object_ref or '').strip()})")

    form_contexts = [str(x).split(":", 1)[-1] for x in sorted(forms) if ":" in str(x)]
    file_context = Path(file_path).stem if file_path else ""
    for event_name in re.findall(
        r"(?im)^\s*(?:Public|Private|Friend)?\s*Sub\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(",
        text,
    ):
        event = str(event_name).strip()
        if not event:
            continue
        if len(event_handlers) < 480:
            event_handlers.add(event)
        if form_contexts:
            for form_name in form_contexts[:6]:
                if len(event_handler_keys) >= 1200:
                    break
                event_handler_keys.add(f"{form_name}::{event}")
        elif file_context:
            if len(event_handler_keys) < 1200:
                event_handler_keys.add(f"{file_context}::{event}")
        else:
            if len(event_handler_keys) < 1200:
                event_handler_keys.add(event)
    for proc_name in re.findall(
        r"(?im)^\s*(?:Public|Private|Friend|Protected)?\s*(?:Function|Sub|Property Get|Property Let|Property Set)\s+([A-Za-z_][A-Za-z0-9_]*)\b",
        text,
    ):
        pname = str(proc_name).strip()
        if pname:
            procedures.add(pname)

    for member_type, member_raw in re.findall(
        r"(?im)^\s*(Form|Module|Class|UserControl|Designer|PropertyPage|UserDocument)\s*=\s*(.+)$",
        text,
    ):
        raw_value = str(member_raw or "").strip()
        member_path = raw_value.split(";")[-1].strip().strip('"')
        project_members.add(f"{member_type}:{member_path or raw_value}")

    if suffix == ".vbp":
        name_match = re.search(r"(?im)^\s*Name\s*=\s*(.+)$", text)
        type_match = re.search(r"(?im)^\s*Type\s*=\s*(.+)$", text)
        startup_match = re.search(r"(?im)^\s*Startup\s*=\s*(.+)$", text)
        project_name = str(name_match.group(1) or "").strip().strip('"') if name_match else ""
        if not project_name:
            project_name = Path(file_path).stem or "VB6Project"
        project_type = str(type_match.group(1) or "").strip().strip('"') if type_match else ""
        startup_object = str(startup_match.group(1) or "").strip().strip('"') if startup_match else ""
        members: list[dict[str, str]] = []
        for member_type, member_raw in re.findall(
            r"(?im)^\s*(Form|Module|Class|UserControl|Designer|PropertyPage|UserDocument)\s*=\s*(.+)$",
            text,
        ):
            raw_value = str(member_raw or "").strip()
            member_path = raw_value.split(";")[-1].strip().strip('"')
            members.append(
                {
                    "member_type": str(member_type or "").strip(),
                    "member_path": member_path or raw_value,
                    "raw": raw_value,
                }
            )
        references: list[str] = []
        for ref in re.findall(r'(?im)^\s*Reference\s*=\s*(.+)$', text):
            ref_text = str(ref or "").strip()
            if ref_text:
                references.append(ref_text)
        for obj in re.findall(r'(?im)^\s*Object\s*=\s*(.+)$', text):
            obj_text = str(obj or "").strip()
            if obj_text:
                references.append(obj_text)
        resource_file = ""
        res_match = re.search(r'(?im)^\s*ResFile32\s*=\s*"([^"]+)"', text)
        if res_match:
            resource_file = str(res_match.group(1) or "").strip()
            if resource_file:
                members.append(
                    {
                        "member_type": "Resource",
                        "member_path": resource_file,
                        "raw": resource_file,
                    }
                )
        project_definition = {
            "project_name": project_name,
            "project_type": project_type,
            "startup_object": startup_object,
            "project_file": file_path,
            "members": members,
            "references": references[:80],
            "resource_file": resource_file,
        }
    elif suffix == ".vbg":
        project_name = Path(file_path).stem or "VB6ProjectGroup"
        members: list[dict[str, str]] = []
        for project_alias, member_raw in re.findall(r"(?im)^\s*([^=]+)\s*=\s*(.+\.vbp)\s*$", text):
            raw_value = str(member_raw or "").strip().strip('"')
            alias = str(project_alias or "").strip() or "Project"
            if not raw_value:
                continue
            members.append(
                {
                    "member_type": "Project",
                    "member_path": raw_value,
                    "raw": f"{alias}={raw_value}",
                }
            )
            project_members.add(f"Project:{raw_value}")
        project_definition = {
            "project_name": project_name,
            "project_type": "ProjectGroup",
            "startup_object": "",
            "project_file": file_path,
            "members": members,
            "references": [],
            "resource_file": "",
        }

    form_names = [str(x).split(":", 1)[-1] for x in forms if ":" in str(x)]
    source_lines = str(text or "").splitlines()
    for raw in source_lines:
        line = str(raw or "")
        lower = line.lower()
        if "on error resume next" in lower:
            on_error_resume_next += 1
        if re.search(r"(?i)\bon\s+error\s+goto\s+0\b", line):
            on_error_goto0 += 1
        elif re.search(r"(?i)\bon\s+error\s+goto\s+[-a-zA-Z0-9_]+\b", line):
            on_error_goto += 1
        if re.search(r"(?i)\bindex\s*=\s*\d+\b", line):
            control_array_index_markers += 1
        if re.search(r"(?i)\bdim\s+[A-Za-z_][A-Za-z0-9_]*\s+as\s+variant\b", line):
            variant_declarations += 1
        if re.search(r"(?i)\bdoevents\b", line):
            doevents_calls += 1
        if re.search(r"(?i)\b(?:createsetting|savesetting|getsetting|deletesetting|regread|regwrite|regdelete)\b", line):
            registry_ops += 1
        progid_match = re.search(r'(?i)\b(?:CreateObject|GetObject)\s*\(\s*"([^"]+)"', line)
        if progid_match:
            progid = str(progid_match.group(1) or "").strip()
            if progid:
                late_bound_progids.add(progid)
        if re.search(r"(?i)\bcallbyname\s*\(", line):
            call_by_name_sites += 1
        if line.strip().startswith("'"):
            continue
        if "begin vb." in lower:
            continue
        for fname in form_names:
            if fname and re.search(rf"(?i)\b{re.escape(fname)}\.", line):
                default_instance_references += 1
                break
        declare_match = re.search(
            r'(?i)^\s*(?:Public|Private)?\s*Declare\s+(?:PtrSafe\s+)?(?:Function|Sub)\s+([A-Za-z_][A-Za-z0-9_]*)\s+Lib\s+"([^"]+)"',
            line,
        )
        if declare_match:
            sig = f"{str(declare_match.group(1) or '').strip()}@{str(declare_match.group(2) or '').strip()}"
            if sig and sig not in win32_declares:
                win32_declares.append(sig)
    for query in _extract_sql_candidates_from_lines(source_lines):
        if query and query not in sql_queries:
            sql_queries.append(query[:360])
        if len(sql_queries) >= 200:
            break

    ui_event_map: list[dict[str, Any]] = []
    control_to_form: dict[str, str] = {}
    for form_key in forms:
        if ":" not in str(form_key):
            continue
        _, form_name = str(form_key).split(":", 1)
        control_to_form[form_name.lower()] = str(form_key)
    for control in controls:
        ctext = str(control)
        if ":" not in ctext:
            continue
        _, cname = ctext.split(":", 1)
        for form_key in forms:
            form_name = str(form_key).split(":", 1)[-1]
            if cname.lower().startswith(form_name.lower()):
                control_to_form[cname.lower()] = str(form_key)
                break
    for event_name in sorted(event_handlers):
        control_name = event_name.split("_", 1)[0].strip() if "_" in event_name else event_name
        evt = event_name.split("_", 1)[1].strip() if "_" in event_name else ""
        mapped_form = control_to_form.get(control_name.lower(), "")
        sql_touches = procedure_sql.get(event_name, []) if isinstance(procedure_sql.get(event_name, []), list) else []
        ui_event_map.append(
            {
                "event_handler": event_name,
                "form": mapped_form,
                "control": control_name,
                "event": evt,
                "procedure_calls": procedure_calls.get(event_name, []) if isinstance(procedure_calls.get(event_name, []), list) else [],
                "sql_touches": sql_touches[:8],
                "side_effects": procedure_effects.get(event_name, []) if isinstance(procedure_effects.get(event_name, []), list) else [],
            }
        )

    pitfall_detectors = _map_vb6_detectors(
        path=file_path or "(inline)",
        on_error_resume_next=on_error_resume_next,
        control_arrays=control_array_index_markers,
        late_binding=(len(late_bound_progids) + call_by_name_sites),
        win32_declares=len(win32_declares),
        doevents=doevents_calls,
        variant_decls=variant_declarations,
        default_instances=default_instance_references,
    )

    return {
        "forms": sorted(forms),
        "controls": sorted(controls),
        "activex_dependencies": sorted(activex_dependencies),
        "event_handlers": sorted(event_handlers),
        "event_handler_keys": sorted(event_handler_keys)[:1200],
        "procedures": sorted(procedures)[:160],
        "sql_queries": sql_queries[:120],
        "ui_event_map": ui_event_map[:200],
        "project_members": sorted(project_members),
        "com_surface_map": {
            "late_bound_progids": sorted(late_bound_progids)[:120],
            "call_by_name_sites": call_by_name_sites,
            "createobject_getobject_sites": len(late_bound_progids),
            "references": sorted(activex_dependencies)[:120],
        },
        "win32_declares": win32_declares[:120],
        "error_handling_profile": {
            "on_error_resume_next": on_error_resume_next,
            "on_error_goto": on_error_goto,
            "on_error_goto0": on_error_goto0,
            "control_array_index_markers": control_array_index_markers,
            "late_bound_com_calls": len(late_bound_progids) + call_by_name_sites,
            "variant_declarations": variant_declarations,
            "default_instance_references": default_instance_references,
            "doevents_calls": doevents_calls,
            "registry_operations": registry_ops,
        },
        "pitfall_detectors": pitfall_detectors,
        "project_definition": project_definition,
        "vb6_file_type": vb6_file_type,
        "is_binary_companion": False,
        "module_profile": {
            "module_type": vb6_file_type,
            "procedure_count": len(procedures),
            "event_handler_count": max(len(event_handler_keys), len(event_handlers)),
            "sql_query_count": len(sql_queries),
        },
    }
