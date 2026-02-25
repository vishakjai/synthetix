from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from utils.analyst_report import build_analyst_report_v2, build_raw_artifact_set_v1


SQL_KEYWORD_RE = re.compile(r"^\s*(select|insert|update|delete|create|alter|drop|truncate)\b", re.IGNORECASE)
TABLE_RE = re.compile(r"\b(?:from|join|into|update)\s+([a-zA-Z_][\w.]*)", re.IGNORECASE)
DEP_RE = re.compile(r"\b([A-Za-z0-9_.-]+\.(?:ocx|dll|dcx|dca))\b", re.IGNORECASE)
FORM_NAME_RE = re.compile(r"\b([A-Za-z0-9_]+\.frm)\b", re.IGNORECASE)
PROJECT_FILE_RE = re.compile(r"\b([A-Za-z0-9_./ -]+\.vbp)\b", re.IGNORECASE)
EVENT_ENTRY_RE = re.compile(r"^\s*[-*]?\s*([^|]+?)\s*\|\s*(.+)$")
DETECTOR_RE = re.compile(
    r"^\s*[-*]?\s*(?P<id>VB6-[A-Za-z0-9-]+)\s*(?:\[(?P<sev>low|medium|high)\])?\s*(?:count\s*=\s*(?P<count>\d+))?\s*\|?\s*(?P<rest>.*)$",
    re.IGNORECASE,
)
OPEN_QUESTION_RE = re.compile(
    r"^\s*[-*]?\s*(?:\[(?P<sev>[A-Za-z]+)\]\s*)?(?:(?P<qid>Q[-A-Za-z0-9_]+)\s*:\s*)?(?P<question>.+?)(?:\s*\(owner:\s*(?P<owner>[^)]+)\))?\s*$"
)
RULE_RE = re.compile(
    r"^\s*[-*]?\s*(?P<rid>BR[-A-Za-z0-9_]+)?\s*(?:\[(?P<cat>[A-Za-z_ ]+)\])?\s*[:\-]?\s*(?P<stmt>.+?)(?:\s*\|\s*evidence[:=]\s*(?P<evd>.+))?$",
    re.IGNORECASE,
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _normalize_priority(value: str) -> str:
    p = _clean(value).upper()
    return p if p in {"P0", "P1", "P2", "P3"} else "P1"


def _extract_sections(markdown_doc: str) -> list[tuple[int, str, list[str]]]:
    lines = markdown_doc.splitlines()
    sections: list[tuple[int, str, list[str]]] = []
    cur_level = 0
    cur_title = "root"
    cur_lines: list[str] = []
    for line in lines:
        m = re.match(r"^(#{1,6})\s+(.+?)\s*$", line)
        if m:
            sections.append((cur_level, cur_title, cur_lines))
            cur_level = len(m.group(1))
            cur_title = m.group(2).strip()
            cur_lines = []
            continue
        cur_lines.append(line)
    sections.append((cur_level, cur_title, cur_lines))
    return sections


def _section_lines(sections: list[tuple[int, str, list[str]]], keys: list[str]) -> list[str]:
    out: list[str] = []
    lowered_keys = [k.lower() for k in keys]
    for _, title, lines in sections:
        t = title.lower()
        if any(k in t for k in lowered_keys):
            out.extend(lines)
    return out


def _all_lines(sections: list[tuple[int, str, list[str]]]) -> list[str]:
    out: list[str] = []
    for _, _, lines in sections:
        out.extend(lines)
    return out


def _parse_table_rows(lines: list[str]) -> list[dict[str, str]]:
    table_lines = [line.strip() for line in lines if line.strip().startswith("|")]
    if len(table_lines) < 3:
        return []
    headers = [h.strip().lower() for h in table_lines[0].strip("|").split("|")]
    rows: list[dict[str, str]] = []
    for raw in table_lines[2:]:
        if set(raw.replace("|", "").strip()) <= {"-", " "}:
            continue
        cols = [c.strip() for c in raw.strip("|").split("|")]
        if not cols:
            continue
        row = {headers[i]: (cols[i] if i < len(cols) else "") for i in range(len(headers))}
        rows.append(row)
    return rows


def _parse_readiness_and_strategy(lines: list[str]) -> tuple[int, str, str]:
    score = 60
    risk_tier = "medium"
    strategy = "Phased modernization"
    for line in lines:
        text = _clean(line)
        if not text:
            continue
        ms = re.search(r"readiness[^0-9]{0,16}(\d{1,3})\s*(?:/\s*100)?", text, re.IGNORECASE)
        if ms:
            try:
                score = max(0, min(100, int(ms.group(1))))
            except ValueError:
                pass
        mr = re.search(r"risk(?:\s*tier)?[^a-zA-Z]{0,8}(low|medium|high)", text, re.IGNORECASE)
        if mr:
            risk_tier = mr.group(1).lower()
        mst = re.search(r"recommended strategy[^:]*:\s*(.+)$", text, re.IGNORECASE)
        if mst:
            strategy = _clean(mst.group(1))
    return score, risk_tier, strategy


def _parse_projects(project_lines: list[str]) -> list[dict[str, Any]]:
    projects: list[dict[str, Any]] = []
    for idx, line in enumerate(project_lines):
        text = _clean(line)
        if not text or "file=" not in text:
            continue
        parts = [p.strip() for p in text.split("|")]
        kv: dict[str, str] = {}
        for part in parts:
            if "=" in part:
                k, v = part.split("=", 1)
                kv[k.strip().lower()] = v.strip()
        file_name = _clean(kv.get("file"))
        if not file_name:
            continue
        name = f"Project{len(projects) + 1}"
        prev = _clean(project_lines[idx - 1]) if idx > 0 else ""
        if prev and "=" not in prev and "|" not in prev:
            name = prev.lstrip("-* ").strip() or name
        forms_count = 0
        try:
            forms_count = int(kv.get("forms", "0") or 0)
        except ValueError:
            forms_count = 0
        members_count = 0
        try:
            members_count = int(kv.get("members", "0") or 0)
        except ValueError:
            members_count = 0
        projects.append(
            {
                "project_name": name,
                "project_file": file_name,
                "project_type": _clean(kv.get("type")) or "Exe",
                "startup_object": _clean(kv.get("startup")),
                "forms_count": forms_count,
                "members_count": members_count,
                "member_files": [],
                "forms": [],
                "controls": [],
                "activex_dependencies": [],
                "business_objective_hypothesis": "",
                "key_business_capabilities": [],
            }
        )
    return projects


def _parse_forms(lines: list[str]) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    seen: set[str] = set()
    for line in lines:
        text = _clean(line)
        if not text:
            continue
        for name in FORM_NAME_RE.findall(text):
            key = name.lower()
            if key in seen:
                continue
            seen.add(key)
            found.append({"form_name": name.replace(".frm", ""), "file": name, "business_use": ""})
        m = re.search(r"\bForm[:\s]+([A-Za-z0-9_]+)\b", text, re.IGNORECASE)
        if m:
            nm = m.group(1)
            key = nm.lower()
            if key in seen:
                continue
            seen.add(key)
            found.append({"form_name": nm, "file": f"{nm}.frm", "business_use": ""})
    return found


def _extract_sql_lines(lines: list[str]) -> list[str]:
    sqls: list[str] = []
    seen: set[str] = set()
    sql_start_re = re.compile(r"\b(select|insert|update|delete|create|alter|drop|truncate)\b.*", re.IGNORECASE)
    for line in lines:
        text = _clean(line)
        if not text:
            continue
        text = re.sub(r"^\s*[-*]\s*", "", text)
        candidates: list[str] = []
        candidates.extend(re.findall(r"`([^`]+)`", text))
        candidates.append(text)
        if ":" in text:
            candidates.append(text.split(":", 1)[-1].strip())
        for candidate in candidates:
            cand = _clean(candidate).strip("`")
            if not cand:
                continue
            m = sql_start_re.search(cand)
            if not m:
                continue
            sql = _clean(m.group(0))
            if not sql:
                continue
            key = sql.lower()
            if key in seen:
                continue
            seen.add(key)
            sqls.append(sql)
    return sqls


def _extract_tables(sql_rows: list[str]) -> list[str]:
    tables: list[str] = []
    seen: set[str] = set()
    for sql in sql_rows:
        for name in TABLE_RE.findall(sql):
            key = name.lower()
            if key in seen:
                continue
            seen.add(key)
            tables.append(name)
    return tables[:24]


def _parse_event_map(lines: list[str]) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    current_form = ""
    handler_line_re = re.compile(
        r"^\s*[-*]\s*`?(?P<handler>[A-Za-z_][A-Za-z0-9_]*)`?\s*(?:\(|\s+line|\s*:\s*)(?P<rest>.*)$",
        re.IGNORECASE,
    )
    form_heading_re = re.compile(r"\b([A-Za-z0-9_ -]+\.frm)\b", re.IGNORECASE)
    i = 0
    while i < len(lines):
        line = _clean(lines[i])
        if not line:
            i += 1
            continue
        form_heading = form_heading_re.search(line)
        if form_heading:
            current_form = _clean(form_heading.group(1))
        file_line = re.search(r"^\s*[-*]?\s*\*\*File\*\*:\s*`?([^`]+\.frm)`?\s*$", line, re.IGNORECASE)
        if file_line:
            current_form = _clean(file_line.group(1))

        m = EVENT_ENTRY_RE.match(line)
        row: dict[str, Any] | None = None
        if m:
            head = _clean(m.group(1)).lstrip("-* ").strip()
            rhs = _clean(m.group(2))
            if "event=" in rhs.lower() or "control=" in rhs.lower() or "form=" in rhs.lower():
                kv: dict[str, str] = {}
                for segment in rhs.split("|"):
                    if "=" in segment:
                        k, v = segment.split("=", 1)
                        kv[k.strip().lower()] = v.strip()
                row = {
                    "event_handler": head,
                    "form": _clean(kv.get("form")) or current_form,
                    "control": _clean(kv.get("control")),
                    "event": _clean(kv.get("event")),
                    "procedure_calls": [],
                    "sql_touches": [],
                }
        if not row:
            hm = handler_line_re.match(line)
            if hm:
                handler = _clean(hm.group("handler"))
                rest = _clean(hm.group("rest"))
                control = ""
                event = ""
                parts = handler.split("_")
                if len(parts) >= 2:
                    control = _clean(parts[0])
                    event = _clean(parts[-1])
                calls: list[str] = []
                for token in re.findall(r"`([^`]+)`", rest):
                    t = _clean(token)
                    if t and not SQL_KEYWORD_RE.match(t):
                        calls.append(t)
                sqls = _extract_sql_lines([rest])
                row = {
                    "event_handler": handler,
                    "form": current_form,
                    "control": control,
                    "event": event,
                    "procedure_calls": calls[:10],
                    "sql_touches": sqls[:20],
                }

        if row:
            j = i + 1
            while j < len(lines):
                nxt = _clean(lines[j])
                if not nxt:
                    j += 1
                    continue
                if re.match(r"^(#{1,6})\s+", nxt):
                    break
                if EVENT_ENTRY_RE.match(nxt):
                    break
                if handler_line_re.match(nxt):
                    break
                low = nxt.lower()
                if "calls:" in low and ":" in nxt:
                    _, right = nxt.split(":", 1)
                    extra_calls = [_clean(x) for x in re.split(r"[;,]", right) if _clean(x)]
                    row["procedure_calls"] = [*row.get("procedure_calls", []), *extra_calls][:20]
                sql_extra = _extract_sql_lines([nxt])
                if sql_extra:
                    row["sql_touches"] = [*row.get("sql_touches", []), *sql_extra][:40]
                j += 1
            entries.append(row)
            i = j
            continue

        i += 1
    deduped: list[dict[str, Any]] = []
    seen_rows: set[str] = set()
    for row in entries:
        key = "|".join(
            [
                _clean(row.get("event_handler")).lower(),
                _clean(row.get("form")).lower(),
                _clean(row.get("control")).lower(),
                _clean(row.get("event")).lower(),
            ]
        )
        if key in seen_rows:
            continue
        seen_rows.add(key)
        deduped.append(row)
    return deduped[:400]


def _parse_dependencies(lines: list[str]) -> tuple[list[str], list[str], list[str], list[str], list[str]]:
    all_deps: list[str] = []
    seen: set[str] = set()
    for line in lines:
        for dep in DEP_RE.findall(line):
            key = dep.lower()
            if key in seen:
                continue
            seen.add(key)
            all_deps.append(dep)
    ocx = [d for d in all_deps if d.lower().endswith(".ocx")]
    dll = [d for d in all_deps if d.lower().endswith(".dll")]
    dcx = [d for d in all_deps if d.lower().endswith(".dcx")]
    dca = [d for d in all_deps if d.lower().endswith(".dca")]
    return all_deps, ocx, dll, dcx, dca


def _parse_business_rules(lines: list[str]) -> list[dict[str, Any]]:
    rules: list[dict[str, Any]] = []
    current_category = "other"
    for line in lines:
        text = _clean(line)
        if not text:
            continue
        if text.startswith("#"):
            continue
        if text.lower().startswith("category"):
            current_category = _clean(text.split(":", 1)[-1]).replace(" ", "_").lower() or current_category
            continue
        if not re.search(r"\bBR[-_A-Za-z0-9]+\b", text, re.IGNORECASE) and not re.search(r"\brule\b", text, re.IGNORECASE):
            continue
        m = RULE_RE.match(text)
        if not m:
            continue
        stmt = _clean(m.group("stmt"))
        if not stmt or stmt.lower() in {"none", "n/a"}:
            continue
        rid = _clean(m.group("rid")) or f"BR-{len(rules) + 1:03d}"
        cat = _clean(m.group("cat")).replace(" ", "_").lower() or current_category
        evd = _clean(m.group("evd"))
        rules.append(
            {
                "id": rid,
                "rule_type": cat if cat else "other",
                "statement": stmt,
                "evidence": evd,
                "confidence": 0.7,
                "tags": [cat] if cat else [],
            }
        )
    return rules


def _parse_detector_findings(lines: list[str]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    profile = {
        "on_error_resume_next": 0,
        "on_error_goto": 0,
        "late_bound_com_calls": 0,
        "default_instance_references": 0,
        "control_array_index_markers": 0,
    }
    for line in lines:
        text = _clean(line)
        if not text:
            continue
        m = DETECTOR_RE.match(text)
        if m:
            sev = _clean(m.group("sev")).lower() or "medium"
            if sev not in {"low", "medium", "high"}:
                sev = "medium"
            cnt = 0
            try:
                cnt = int(m.group("count") or 0)
            except ValueError:
                cnt = 0
            findings.append(
                {
                    "id": _clean(m.group("id")),
                    "severity": sev,
                    "count": cnt,
                    "evidence": _clean(m.group("rest")),
                    "requires": [],
                }
            )
        low = text.lower()
        for key, rx in (
            ("on_error_resume_next", r"on[\s_]?error[\s_]?resume[\s_]?next[^0-9]*(\d+)"),
            ("on_error_goto", r"on[\s_]?error[\s_]?goto[^0-9]*(\d+)"),
            ("late_bound_com_calls", r"late[\s_-]?bound(?:\s+com)?\s+calls?[^0-9]*(\d+)"),
            ("default_instance_references", r"default[\s_-]?instance(?:\s+refs?| references?)?[^0-9]*(\d+)"),
            ("control_array_index_markers", r"control[\s_-]?array(?:\s+markers?)?[^0-9]*(\d+)"),
        ):
            mm = re.search(rx, low, re.IGNORECASE)
            if mm:
                try:
                    profile[key] = int(mm.group(1))
                except ValueError:
                    pass
    return findings, profile


def _parse_open_questions(lines: list[str]) -> tuple[list[dict[str, Any]], list[str]]:
    out: list[dict[str, Any]] = []
    warnings: list[str] = []
    for line in lines:
        text = _clean(line)
        if not text:
            continue
        if "[object object]" in text.lower():
            warnings.append("open_questions_parse: encountered '[object Object]' in markdown")
            continue
        m = OPEN_QUESTION_RE.match(text)
        if not m:
            continue
        q = _clean(m.group("question"))
        if not q:
            continue
        sev = _clean(m.group("sev")).lower() or "medium"
        if sev not in {"blocker", "high", "medium", "low"}:
            sev = "medium"
        out.append(
            {
                "id": _clean(m.group("qid")) or f"Q-{len(out) + 1:03d}",
                "question": q,
                "owner": _clean(m.group("owner")) or "Unassigned",
                "severity": sev,
                "context": "",
            }
        )
    return out, warnings


def _parse_backlog(lines: list[str]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    fr: list[dict[str, Any]] = []
    nfr: list[dict[str, Any]] = []
    rows = _parse_table_rows(lines)
    for row in rows:
        req_id = _clean(row.get("id"))
        if not req_id:
            continue
        priority = _normalize_priority(row.get("pri") or row.get("priority") or "P1")
        req_type = _clean(row.get("type")).lower()
        title = _clean(row.get("outcome") or row.get("title") or req_id)
        acceptance = _clean(row.get("acceptance"))
        acceptance_items = [_clean(x) for x in acceptance.split("/") if _clean(x)] if acceptance else []
        item = {
            "id": req_id,
            "title": title,
            "description": title,
            "priority": priority,
            "acceptance_criteria": acceptance_items,
        }
        if req_id.upper().startswith("NFR-") or "non" in req_type:
            nfr.append(item)
        else:
            fr.append(item)
    return fr, nfr


def _file_type_coverage(lines: list[str]) -> dict[str, Any]:
    text = "\n".join(lines)
    counts = {}
    for ext in ("cls", "frm", "frx", "bas", "ctl", "ctx", "vbp", "vbg", "res", "ocx", "dcx", "dca"):
        counts[ext] = len(re.findall(rf"\.{ext}\b", text, re.IGNORECASE))
    return {"counts": counts}


def migrate_markdown_to_analyst_output(markdown_doc: str, base_output: dict[str, Any] | None = None) -> dict[str, Any]:
    """
    Translate legacy single-markdown analyst output into standardized raw artifacts
    plus analyst_report_v2 composition output.
    """

    base = dict(base_output) if isinstance(base_output, dict) else {}
    sections = _extract_sections(markdown_doc or "")
    all_lines = _all_lines(sections)

    brief_lines = _section_lines(sections, ["decision brief", "executive summary", "at a glance"])
    inventory_lines = _section_lines(sections, ["legacy code inventory", "vb6 projects", "forms and business use"])
    dep_lines = _section_lines(sections, ["activex", "com dependencies", "dependency"])
    event_lines = _section_lines(sections, ["ui event map", "event map"])
    sql_lines = _section_lines(sections, ["sql query catalog", "sql catalog"])
    rules_lines = _section_lines(sections, ["business rules catalog", "business rule"])
    detector_lines = _section_lines(sections, ["pitfall detectors", "error handling profile", "detector"])
    backlog_lines = _section_lines(sections, ["backlog"])
    question_lines = _section_lines(sections, ["open questions"])

    score, risk_tier, strategy_name = _parse_readiness_and_strategy([*brief_lines, *all_lines])
    projects = _parse_projects(inventory_lines)
    if not projects:
        projects = _parse_projects(all_lines)
    forms = _parse_forms([*inventory_lines, *event_lines])
    if not forms:
        forms = _parse_forms(all_lines)
    sql_rows = _extract_sql_lines(sql_lines)
    if not sql_rows:
        sql_rows = _extract_sql_lines(all_lines)
    event_rows = _parse_event_map(event_lines)
    if not event_rows:
        event_rows = _parse_event_map(all_lines)
    deps, ocx, dll, dcx, dca = _parse_dependencies([*dep_lines, *inventory_lines])
    if not deps:
        deps, ocx, dll, dcx, dca = _parse_dependencies(all_lines)
    rules = _parse_business_rules(rules_lines)
    if not rules:
        rules = _parse_business_rules(all_lines)
    findings, profile = _parse_detector_findings(detector_lines)
    if not findings and not any(profile.values()):
        findings, profile = _parse_detector_findings(all_lines)
    questions, question_warnings = _parse_open_questions(question_lines)
    if not questions and not question_warnings:
        questions, question_warnings = _parse_open_questions(all_lines)
    fr, nfr = _parse_backlog(backlog_lines)

    # Best-effort objective extraction.
    objective = ""
    for line in [*brief_lines, *all_lines]:
        text = _clean(line)
        if not text:
            continue
        if text.lower().startswith("objective:"):
            objective = _clean(text.split(":", 1)[-1])
            break
    if not objective:
        title_line = ""
        for _, title, _ in sections:
            if title.lower().startswith("modernization brief"):
                title_line = title
                break
        objective = title_line or "Legacy system modernization analysis"

    tables = _extract_tables(sql_rows)
    form_names = [f.get("form_name", "") for f in forms if isinstance(f, dict)]
    event_handlers = [e.get("event_handler", "") for e in event_rows if isinstance(e, dict)]
    project_member_files: list[str] = []
    for line in all_lines:
        project_member_files.extend(PROJECT_FILE_RE.findall(line))
        project_member_files.extend(FORM_NAME_RE.findall(line))
    project_member_files = sorted({f for f in project_member_files if f})

    legacy_inventory = {
        "vb6_projects": projects,
        "project_members": project_member_files,
        "forms": forms,
        "controls": [],
        "event_handlers": sorted({h for h in event_handlers if h}),
        "database_tables": tables,
        "sql_query_catalog": sql_rows,
        "ui_event_map": event_rows,
        "activex_controls": ocx,
        "dll_dependencies": dll,
        "ocx_dependencies": ocx,
        "dcx_dependencies": dcx,
        "dca_dependencies": dca,
        "dependencies": deps,
        "business_rules_catalog": rules,
        "pitfall_detectors": findings,
        "error_handling_profile": profile,
        "vb6_file_type_coverage": _file_type_coverage(all_lines),
        "binary_companion_files": sorted(
            {
                part
                for line in all_lines
                for part in re.findall(r"\b[A-Za-z0-9_.-]+\.(?:frx|ctx|res|ocx)\b", line, re.IGNORECASE)
            }
        ),
        "win32_declares": [],
        "modernization_readiness": {
            "score": score,
            "risk_tier": risk_tier,
            "recommended_strategy": {"name": strategy_name},
            "required_actions": [],
        },
    }

    seed = dict(base)
    seed["analysis_walkthrough"] = {"business_objective_summary": objective}
    seed["legacy_code_inventory"] = legacy_inventory
    if fr:
        seed["functional_requirements"] = fr
    if nfr:
        seed["non_functional_requirements"] = nfr
    if questions:
        seed["open_questions"] = questions

    quality_gates = _as_list(seed.get("quality_gates"))
    if question_warnings:
        quality_gates.append(
            {
                "id": "open_questions_parse",
                "status": "warn",
                "message": "Open questions could not be fully parsed from markdown. Use structured question objects.",
            }
        )
    quality_gates.append(
        {
            "id": "markdown_translation",
            "status": "pass",
            "message": "Markdown translated into standardized raw artifacts and analyst_report_v2.",
        }
    )
    seed["quality_gates"] = quality_gates[-20:]

    raw_artifacts = build_raw_artifact_set_v1(seed)
    seed["raw_artifacts"] = raw_artifacts
    report_v2 = build_analyst_report_v2(seed)

    return {
        "analysis_walkthrough": seed.get("analysis_walkthrough", {}),
        "legacy_code_inventory": legacy_inventory,
        "functional_requirements": _as_list(seed.get("functional_requirements")),
        "non_functional_requirements": _as_list(seed.get("non_functional_requirements")),
        "open_questions": _as_list(seed.get("open_questions")),
        "quality_gates": _as_list(seed.get("quality_gates")),
        "raw_artifacts": raw_artifacts,
        "analyst_report_v2": report_v2,
        "markdown_migration": {
            "ok": True,
            "migrated_at": _now(),
            "source_format": "markdown_v1",
            "warnings": question_warnings,
            "counts": {
                "projects": len(projects),
                "forms": len(form_names),
                "dependencies": len(deps),
                "event_map_entries": len(event_rows),
                "sql_statements": len(sql_rows),
                "business_rules": len(rules),
                "detector_findings": len(findings),
            },
        },
    }
